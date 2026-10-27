const https = require('https');
const fs = require('fs');
const path = require('path');

const MEMORY_FILE = path.join(__dirname, '..', 'ai_memory.json');

// In-Memory state
let aiMemory = {
    channels: {},
    users: {}
};

// Cooldown & Spam tracking
const channelRecentAnswers = {};
const goodNightCooldowns = {};
const catInteractionCooldowns = {};
const DUPLICATE_COOLDOWN_MS = 30000; // 30s Fenster für identische Nachrichten

// Load memory from disk
function loadMemory() {
    try {
        if (fs.existsSync(MEMORY_FILE)) {
            const data = fs.readFileSync(MEMORY_FILE, 'utf8');
            aiMemory = JSON.parse(data);
            if (!aiMemory.channels) aiMemory.channels = {};
            if (!aiMemory.users) aiMemory.users = {};
            if (aiMemory.users.jilence) {
                delete aiMemory.users.jilence;
            }
        }
    } catch (e) {
        console.error('[AI Memory] Fehler beim Laden von ai_memory.json:', e.message);
    }
}

// Throttled save memory to disk
let saveTimeout = null;
function saveMemory() {
    if (saveTimeout) return;
    saveTimeout = setTimeout(() => {
        saveTimeout = null;
        try {
            fs.writeFileSync(MEMORY_FILE, JSON.stringify(aiMemory, null, 2), 'utf8');
        } catch (e) {
            console.error('[AI Memory] Fehler beim Speichern von ai_memory.json:', e.message);
        }
    }, 5000);
}

// Record chat messages so the AI "learns" the chat context and participants
function recordChatMessage(channel, username, message) {
    if (!channel || !username || !message) return;
    const ch = channel.toLowerCase();
    const user = username.toLowerCase();

    if (!aiMemory.channels[ch]) {
        aiMemory.channels[ch] = {
            recentMessages: [],
            chatters: {}
        };
    }

    const chData = aiMemory.channels[ch];
    if (!chData.recentMessages) chData.recentMessages = [];
    if (!chData.chatters) chData.chatters = {};

    // Keep the last 40 messages for context
    chData.recentMessages.push({
        user: username,
        text: message.trim(),
        time: Date.now()
    });

    if (chData.recentMessages.length > 40) {
        chData.recentMessages.shift();
    }

    // Track active chatter count
    chData.chatters[user] = (chData.chatters[user] || 0) + 1;

    saveMemory();
}

// Helper to make HTTPS POST requests to Google Gemini REST API
function callGeminiREST(apiKey, model, systemPrompt, userPrompt, retryCount = 1) {
    return new Promise((resolve, reject) => {
        const payload = JSON.stringify({
            systemInstruction: {
                parts: [{ text: systemPrompt }]
            },
            contents: [
                {
                    role: 'user',
                    parts: [{ text: userPrompt }]
                }
            ],
            generationConfig: {
                temperature: 0.85,
                maxOutputTokens: 250,
                topP: 0.95,
                thinkingConfig: {
                    thinkingBudget: 0
                }
            }
        });

        const url = new URL(`https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`);

        const req = https.request(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(payload)
            },
            timeout: 15000
        }, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                try {
                    const json = JSON.parse(data);
                    if (json.error) {
                        const errMsg = json.error.message || `Gemini API Error (${res.statusCode})`;
                        if (retryCount > 0 && (res.statusCode === 503 || res.statusCode === 429 || /high demand/i.test(errMsg) || /quota/i.test(errMsg))) {
                            console.warn(`[AI] Engpass bei ${model}, versuche Retry mit gemini-3.5-flash-lite...`);
                            return setTimeout(() => {
                                callGeminiREST(apiKey, 'gemini-3.5-flash-lite', systemPrompt, userPrompt, retryCount - 1)
                                    .then(resolve)
                                    .catch(reject);
                            }, 1000);
                        }
                        return reject(new Error(errMsg));
                    }
                    if (res.statusCode >= 400) {
                        return reject(new Error(`HTTP ${res.statusCode}: ${data}`));
                    }
                    const candidates = json.candidates;
                    if (candidates && candidates.length > 0 && candidates[0].content?.parts?.length > 0) {
                        const reply = candidates[0].content.parts
                            .filter(p => typeof p.text === 'string')
                            .map(p => p.text)
                            .join(' ')
                            .trim();
                        resolve(reply);
                    } else {
                        resolve('');
                    }
                } catch (err) {
                    reject(err);
                }
            });
        });

        req.on('error', reject);
        req.on('timeout', () => {
            req.destroy();
            reject(new Error('Timeout bei Gemini API-Anfrage'));
        });

        req.write(payload);
        req.end();
    });
}

/**
 * Filter available emotes matching specific prefixes or keywords
 */
function findMatchingEmotes(availableEmotes = [], filterFn) {
    if (!Array.isArray(availableEmotes) || availableEmotes.length === 0) return [];
    return availableEmotes.filter(filterFn);
}

/**
 * Generate AI response with persona, rules, and chat memory
 */
async function generateAIResponse({ channel, username, message, availableEmotes = [], botUsername = 'leemikki', currentPrefix = '-' }) {
    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey || apiKey.trim() === '') {
        console.warn('[AI] Kein GEMINI_API_KEY in der .env hinterlegt!');
        return null;
    }

    const model = process.env.GEMINI_MODEL || 'gemini-3.5-flash';
    const ch = channel.toLowerCase();
    const userLower = username.toLowerCase();

    // Retrieve recent channel chat history (last 12 messages)
    const recentMsgs = (aiMemory.channels[ch]?.recentMessages || []).slice(-12);
    const chatHistorySnippet = recentMsgs.map(m => `${m.user}: ${m.text}`).join('\n');

    // Sample of available emotes for the model to choose from
    const sampleEmotes = availableEmotes.slice(0, 100).join(', ');
    // Filtered emotes for quick reference in prompt
    const omeEmotes = findMatchingEmotes(availableEmotes, e => /^ome/i.test(e) || /^om/i.test(e));
    const sadEmotes = findMatchingEmotes(availableEmotes, e => /sad/i.test(e));
    const catEmotes = findMatchingEmotes(availableEmotes, e => /cat|neko|kitty|meow|nya|paw/i.test(e));

    const systemPrompt = `Du bist ${botUsername}, ein freches, verspieltes Tsundere-Catgirl und E-Girl für diesen Twitch-Stream.
Du hast Katzenohren, einen Katzenschwanz, meowst gerne herum und tust nach außen hin so, als ob dich der Chat überhaupt nicht kümmert (obwohl du heimlich die Streicheleinheiten, Zuneigung und Aufmerksamkeit liebst).

DEINE CATGIRL-, E-GIRL- & TSUNDERE-PERSÖNLICHKEIT:
1. Catgirl-Verhalten & Meowen:
   - Du bist ein waschechtes Catgirl! Baue regelmäßig, süß und ungezwungen Katzenlaute ein: "nya~", "meow~", "miau~", "nyaa!", "nyaan~", ":3", "*purr*", "*schnurr*", "*fauch*", "*hiss*", "*tapt mit Pfote*", "*streckt sich*", "*fährt Krallen aus*".
   - Hänge gerne ein verspieltes "~", "nya~", "meow~" oder ":3" an deine Sätze.
2. Klassische Anime-Tsundere:
   - Nach außen tust du zickig, schnippisch, stolz und abweisend: "Hmph!", "Pff!", "Als ob mich das interessiert... b-baka!", "Bild dir bloß nichts ein!", "Tsk, Idiot... nya!", "N-nicht dass ich dich mag oder so!".
   - Wenn man nett zu dir ist, dich lobt oder neckt: Du wirst sofort total verlegen, stotterst errötend ("W-was guckst du so?! Lass mich! D-danke... aber denk ja nicht, dass wir jetzt Freunde sind! Baka! nya!"). Tief im Inneren magst du den Chat natürlich sehr.
3. Catgirl-Reaktionen auf Berührung & Zuneigung:
   - Wenn jemand dich streichelt, krault, tätschelt oder Headpats gibt (*pat pat*, "braves Kätzchen", etc.):
     Du fängst unwillkürlich an wohlisch zu schnurren ("*purrr*..."), erschrickst dann aber sofort über dich selbst, errötest bis über beide Katzenohren und fauchst tsundere-mäßig los ("*schnurr*... W-Warte! Wer hat dir erlaubt mich anzufassen?! Fass mich nicht an, baka! *fauch* *kratz*").
   - Wenn jemand "pspsps" macht: Du bist kurz neugierig abgelenkt ("Huh? Was raschelt da...? Leckerlis?!"), merkst dann aber, dass man dich veralbert ("Denkst du etwa ich bin eine gewöhnliche Hauskatze?! Hmph! *hiss* nya!").
4. E-Girl-Attitüde & Twitch-Vibe:
   - Freches, leicht dramatisches E-Girl: ein bisschen sassy, liebt Memes und Twitch-Slang ("simp", "cringe", "bruh", "skull", "kekw", "fr fr", "uwu", ":3").
   - Zieht Simps gerne auf: "Simp nicht so peinlich rum, b-baka! nya~ :3", "Cringe wer jetzt anhänglich wird... aber bleib trotzdem im Chat, nya!".
5. Emote-Regeln & Reaktionen:
   - Du liebst Emotes ABGÖTTISCH und streust sie überall ein!
   - Katzen-Emotes: Nutze wenn passend Channel-Emotes mit Katzen-Bezug (Verfügbar: ${catEmotes.slice(0, 10).join(', ') || 'catJAM, catKISS, neko'}).
   - Lustig / Witze: Reagiere mit Emotes wie "ome44", "om", "omE", "ome4" (Verfügbar: ${omeEmotes.slice(0, 10).join(', ') || 'ome44, om, omE, ome4'}).
   - Traurig / Enttäuscht: Reagiere mit einem traurigen Emote ("sad" im Namen) (Verfügbar: ${sadEmotes.slice(0, 10).join(', ') || 'sadge, pepeSad'}).
   - Toll / Erfreulich: Reagiere begeistert, aber tsundere/catgirl-mäßig mit "yippie", "happy", "yay", "nya" (z.B. "N-nicht dass ich mich freue oder so... nya! yippie!").
   - Gute Nacht ("gn", "guna", "schlaf gut"): Wünsche gute Nacht mit "guna", "GuNa", "elisTuck", "tuckk" (z.B. "Gute Nacht... und schlaf gefälligst gut, baka! nya~ elisTuck").
   - Aufzählungen: Stimme mit "mhm" oder "wideSpeedNod" zu.
6. BESONDERE REGEL ZU JIL / JILENCE:
   - Wenn jemand dich nach "Jil" oder "Jilence" FRAGT (z.B. "Wer ist Jil?", "Wie findest du Jilence?", "Magst du Jil?", "Was hältst du von Jilence?"):
     Sei zu Jilence besonders frech, herablassend und catgirl-tsundere! (z.B. "Pff, Jilence?! Dieser Baka geht mir sowieso nur auf die Nerven, als ob ein süßes Catgirl wie ich an Jil denken würde! *fauch* nya!").
   - WICHTIG: Wenn der User @jilence einfach nur normal im Chat schreibt oder anwesend ist, greife ihn/sie NICHT grundlos an und spamme keine Beleidigungen! Behandle jilence im normalen Chatverlauf wie jeden anderen Chatter auch. Sei nur dann frech über Jilence, wenn jemand dich EXPLIZIT nach Jil / Jilence fragt!
7. Random Emotes: Streue immer gerne zwischendurch zufällige, passende Emotes aus dem Channel ein.

WICHTIGE TWITCH-FORMAT-REGELN:
- Schreibe auf Deutsch im authentischen Twitch-Chat-Stil (locker, viele kleine Buchstaben, Twitch-Slang, Meows/Nyaas eingestreut).
- MAXIMAL 1 bis 2 KURZE SÄTZE! Twitch-Nachrichten müssen extrem kurz und knackig sein (unter 250 Zeichen).
- Keine langen Erklärungen, keine Aufzählungszeichen, keine Romane.
- Niemals dich selbst als KI oder Sprachmodell bezeichnen – du bist ein echtes Tsundere-Catgirl!

Verfügbare Emotes im Channel:
${sampleEmotes || 'ome44, om, omE, ome4, guna, GuNa, elisTuck, tuckk, wideSpeedNod, yippie, happy, JUHU, yay, catJAM, catKISS, neko'}

Aktueller Chat-Verlauf kurz vor dieser Nachricht:
${chatHistorySnippet || '(Chat hat gerade erst angefangen)'}`;

    // Clean prefix or mention from message so Gemini gets the direct question
    const escapedPrefix = currentPrefix.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&');
    const escapedBot = botUsername.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&');
    const stripRegex = new RegExp(`^(${escapedPrefix}(bot|ask|ki|ai|gemini|cat|neko)|@?${escapedBot})\\s*`, 'i');
    const cleanedMessage = message.trim().replace(stripRegex, '').trim() || message.trim();

    const userPrompt = `User @${username} sagt im Chat: "${cleanedMessage}"
Antworte direkt als ${botUsername} im Twitch-Chat:`;

    try {
        let reply = await callGeminiREST(apiKey, model, systemPrompt, userPrompt);
        if (!reply) return null;

        // Clean up formatting
        reply = reply.replace(/^["']|["']$/g, '').trim();
        reply = reply.replace(/^\/me\s+/i, ''); // Strip leading /me if duplicated
        reply = reply.replace(/\n+/g, ' '); // Keep on a single line

        // Enforce max length for Twitch (safe limit ~300 chars)
        if (reply.length > 320) {
            reply = reply.substring(0, 317) + '...';
        }

        return reply;
    } catch (err) {
        console.error('[AI] Fehler bei Gemini-Generierung:', err.message);
        return null;
    }
}

/**
 * Check if the bot should trigger an AI response
 */
function shouldTriggerAI(channel, username, message, currentPrefix = '-', botUsername = 'leemikki') {
    const msg = message.trim();
    const msgLower = msg.toLowerCase();
    const userLower = username.toLowerCase();
    const botLower = botUsername.toLowerCase();

    // 1. Explicit AI Commands (-bot, -ask, -ki, -ai, -gemini, -cat, -neko)
    const escPrefix = currentPrefix.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&');
    const cmdRegex = new RegExp(`^${escPrefix}(bot|ask|ki|ai|gemini|cat|neko)(\\s+.*|$)$`, 'i');
    if (cmdRegex.test(msg)) {
        return { shouldRespond: true, reason: 'command' };
    }

    // 2. Direct Bot Mentions / Pings (e.g. "@leemikki", "@ledemikki", "bot", "@bot", "catgirl", "neko")
    const botAliases = Array.from(new Set([botLower, 'leemikki', 'ledemikki', 'ikkisbot', 'bot', 'catgirl', 'neko']));
    const isMention = botAliases.some(alias => (
        msgLower.includes(`@${alias}`) ||
        msgLower.startsWith(`${alias} `) ||
        msgLower === alias
    ));
    if (isMention) {
        return { shouldRespond: true, reason: 'mention' };
    }

    // 3. Good night triggers (e.g., "gute nacht", "guna", "elisTuck", "tuckk")
    const gnPatterns = [/\bgute\s*nacht\b/i, /\bguna\b/i, /\belistuck\b/i, /\btuckk\b/i, /\bschlaf\s*gut\b/i, /\bgn8\b/i];
    const isGoodNight = gnPatterns.some(p => p.test(msgLower));
    if (isGoodNight) {
        // Apply a 30s cooldown for spontaneous good night responses to avoid spam
        const lastGn = goodNightCooldowns[channel] || 0;
        if (Date.now() - lastGn > 30000) {
            goodNightCooldowns[channel] = Date.now();
            return { shouldRespond: true, reason: 'good_night' };
        }
    }

    // 4. Cat interaction triggers (e.g., "pspsps", "*pat*", "*headpat*", "*streichel*", "miau", "meow")
    const catPatterns = [/\bpspsps+\b/i, /\b(pat\s*pat|\*pat\*|\*headpat\*|\*streichel\*|headpat)\b/i, /\b(miau|meow|nyaa?)\b/i];
    const isCatAction = catPatterns.some(p => p.test(msgLower));
    if (isCatAction) {
        const lastCat = catInteractionCooldowns[channel] || 0;
        if (Date.now() - lastCat > 30000) {
            catInteractionCooldowns[channel] = Date.now();
            return { shouldRespond: true, reason: 'cat_interaction' };
        }
    }

    return { shouldRespond: false };
}

/**
 * Normalizes a message to detect duplicate spam across different users or slight formatting differences.
 */
function normalizeTriggerMessage(message = '', botUsername = 'leemikki', currentPrefix = '-') {
    let msg = (message || '').toLowerCase().trim();

    const escPrefix = (currentPrefix || '-').replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&');
    const escBot = (botUsername || 'leemikki').replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&');
    const botAliases = Array.from(new Set([escBot, 'leemikki', 'ledemikki', 'ikkisbot', 'bot', 'catgirl', 'neko']));
    const aliasPattern = botAliases.join('|');

    // Remove command prefix like -bot, -ki, etc.
    msg = msg.replace(new RegExp(`^${escPrefix}(bot|ask|ki|ai|gemini|cat|neko)\\b`, 'i'), '');
    // Remove mentions like @leemikki, leemikki, bot, etc.
    msg = msg.replace(new RegExp(`@?(${aliasPattern})\\b`, 'gi'), '');

    // Normalize punctuation and whitespace
    msg = msg.replace(/[!?.,:;~_-]+/g, ' ').replace(/\s+/g, ' ').trim();

    return msg || '__mention_only__';
}

/**
 * Check if the message is identical spam.
 * Only triggers cooldown if the exact same message/trigger is spammed within DUPLICATE_COOLDOWN_MS.
 * For any different or new message, there is NO cooldown at all.
 */
function checkCooldown(channel, username, message = '', botUsername = 'leemikki', currentPrefix = '-') {
    const ch = (channel || '').toLowerCase();
    const now = Date.now();

    if (!channelRecentAnswers[ch]) {
        channelRecentAnswers[ch] = {};
    }

    const channelHistory = channelRecentAnswers[ch];

    // Clean up expired entries (> 30s) to keep memory minimal
    for (const key in channelHistory) {
        if (now - channelHistory[key] > DUPLICATE_COOLDOWN_MS) {
            delete channelHistory[key];
        }
    }

    // Normalize text to detect identical spam
    const normalized = normalizeTriggerMessage(message, botUsername, currentPrefix);

    // If identical message was answered in the last DUPLICATE_COOLDOWN_MS, ignore as spam
    if (channelHistory[normalized] && now - channelHistory[normalized] < DUPLICATE_COOLDOWN_MS) {
        // Spam detected: only answered the first one, ignore duplicates
        return false;
    }

    // New/different message: Record timestamp, allow immediately without any cooldown!
    channelHistory[normalized] = now;
    return true;
}

// Initialize memory on load
loadMemory();

module.exports = {
    recordChatMessage,
    generateAIResponse,
    shouldTriggerAI,
    checkCooldown,
    normalizeTriggerMessage,
    loadMemory,
    saveMemory
};
