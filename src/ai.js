const https = require('https');
const fs = require('fs');
const path = require('path');

const MEMORY_FILE = path.join(__dirname, '..', 'ai_memory.json');

// In-Memory state
let aiMemory = {
    channels: {},
    users: {}
};

// Cooldown tracking
const userCooldowns = {};
const channelCooldowns = {};
const goodNightCooldowns = {};

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

    const systemPrompt = `Du bist ${botUsername}, ein lebendiger, frecher Twitch-Chatbot für diesen Stream.
Du beobachtest den Chat und passt dich der Stimmung an.

DEINE PERSÖNLICHKEIT & EMOTE-REGELN:
1. Du bist frech, locker, hast Humor und liebst Emotes ABGÖTTISCH! Du benutzt super viele verschiedene Emotes.
2. Wenn etwas lustig oder witzig ist: Reagiere mit Emotes wie "ome44", "om", "omE", "ome4" oder ähnlichen Emotes mit "ome" (Verfügbar im Channel: ${omeEmotes.slice(0, 10).join(', ') || 'ome44, om, omE, ome4'}).
3. Wenn etwas traurig oder enttäuschend ist: Reagiere mit einem traurigen Emote, das "sad" im Namen hat (Verfügbar im Channel: ${sadEmotes.slice(0, 10).join(', ') || 'sadge, pepeSad'}).
4. Wenn etwas toll, aufregend oder erfreulich ist: Reagiere begeistert mit Wörtern/Emotes wie "yippie", "happy", "JUHU", "yay".
5. Wenn jemand "gute nacht", "gn" oder "guna" sagt: Wünsche dem User eine gute Nacht mit Gute-Nacht-Emotes wie "guna", "GuNa", "elisTuck", "tuckk".
6. Wenn jemand etwas aufzählt (z.B. Punkte, Liste oder mehrere Dinge): Schreibe zustimmend "mhm" oder "wideSpeedNod".
7. Random Emotes: Streue immer gerne zwischendurch zufällige, passende Emotes aus dem Channel ein.

WICHTIGE TWITCH-FORMAT-REGELN:
- Schreibe auf Deutsch im authentischen Twitch-Chat-Stil (locker, teils Kleinschreibung, Chat-Slang).
- MAXIMAL 1 bis 2 KURZE SÄTZE! Twitch-Nachrichten müssen extrem kurz und knackig sein (unter 250 Zeichen).
- Keine langen Erklärungen, keine Aufzählungszeichen, keine Roman-Texte.
- Niemals dich selbst als KI oder Sprachmodell bezeichnen.

Verfügbare Emotes im Channel:
${sampleEmotes || 'ome44, om, omE, ome4, guna, GuNa, elisTuck, tuckk, wideSpeedNod, yippie, happy, JUHU, yay'}

Aktueller Chat-Verlauf kurz vor dieser Nachricht:
${chatHistorySnippet || '(Chat hat gerade erst angefangen)'}`;

    // Clean prefix or mention from message so Gemini gets the direct question
    const escapedPrefix = currentPrefix.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&');
    const escapedBot = botUsername.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&');
    const stripRegex = new RegExp(`^(${escapedPrefix}(bot|ask|ki|ai|gemini)|@?${escapedBot})\\s*`, 'i');
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

    // 1. Explicit AI Commands (-bot, -ask, -ki, -ai, -gemini)
    const escPrefix = currentPrefix.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&');
    const cmdRegex = new RegExp(`^${escPrefix}(bot|ask|ki|ai|gemini)(\\s+.*|$)$`, 'i');
    if (cmdRegex.test(msg)) {
        return { shouldRespond: true, reason: 'command' };
    }

    // 2. Direct Bot Mentions / Pings (e.g. "@leemikki", "@ledemikki", "bot", "@bot")
    const botAliases = Array.from(new Set([botLower, 'leemikki', 'ledemikki', 'ikkisbot', 'bot']));
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

    return { shouldRespond: false };
}

/**
 * Check cooldowns to prevent spamming the chat or exhausting API limits
 */
function checkCooldown(channel, username) {
    const now = Date.now();
    const ch = channel.toLowerCase();
    const user = username.toLowerCase();

    // Channel cooldown: 4 seconds between any AI messages
    if (channelCooldowns[ch] && now - channelCooldowns[ch] < 4000) {
        return false;
    }

    // User cooldown: 7 seconds per user
    if (userCooldowns[user] && now - userCooldowns[user] < 7000) {
        return false;
    }

    channelCooldowns[ch] = now;
    userCooldowns[user] = now;
    return true;
}

// Initialize memory on load
loadMemory();

module.exports = {
    recordChatMessage,
    generateAIResponse,
    shouldTriggerAI,
    checkCooldown,
    loadMemory,
    saveMemory
};
