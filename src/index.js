require('dotenv').config();
const tmi = require('tmi.js');
const fs = require('fs');
const path = require('path');
const http = require('http'); // For Render Health Checks
const { getClientId, getTwitchUserId, get7TVEmotes, parseHint } = require('./7tv');
// mongoose is loaded conditionally below to prevent local crashes

// Configuration
const client = new tmi.Client({
    options: { debug: true, messagesLogLevel: 'info' },
    connection: {
        reconnect: true,
        secure: true
    },
    identity: {
        username: process.env.TWITCH_USERNAME,
        password: process.env.TWITCH_OAUTH_TOKEN
    },
    channels: process.env.TWITCH_CHANNEL.split(',').map(c => c.trim())
});

// --- Render Web Service Support ---
// Render needs a port to be open to check if the app is alive.
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('Twitch Bot is alive!');
});
// Render sets the PORT env variable automatically
const port = process.env.PORT || 3000;
server.listen(port, () => {
    console.log(`Webserver für Render läuft auf Port ${port}`);
});

// Global State
let cachedEmotes = [];
let activeTimers = [];
let currentPrefix = '-';
let isChangingPrefix = false;
let prefixChangeUser = null;
let pendingReminders = {};
let userStars = {};
let activeChatUsers = new Set();
let channelIds = {};
let useMongoDB = false;

let User;
let ChatStat;
// Connection to MongoDB
if (process.env.MONGODB_URI) {
    try {
        const mongoose = require('mongoose');
        const userSchema = new mongoose.Schema({
            username: { type: String, required: true, unique: true },
            balance: { type: Number, default: 0 },
            lastClaim: { type: Number, default: 0 },
            reminded: { type: Boolean, default: false },
            pendingLoan: { type: Boolean, default: false },
            level: { type: Number, default: 0 },
            investedStars: { type: Number, default: 0 },
            nextLevelCost: { type: Number, default: 670 },
            lastChannel: { type: String },
            loan: {
                active: { type: Boolean, default: false },
                amount: { type: Number, default: 0 },
                debt: { type: Number, default: 0 },
                startTime: { type: Number, default: 0 },
                hoursTracked: { type: Number, default: 0 }
            }
        });

        User = mongoose.model('User', userSchema);

        const chatStatSchema = new mongoose.Schema({
            date: { type: String, required: true }, // Format: MM/DD/YYYY from toLocaleString
            username: { type: String, required: true },
            count: { type: Number, default: 0 }
        });
        chatStatSchema.index({ date: 1, username: 1 }, { unique: true });
        ChatStat = mongoose.model('ChatStat', chatStatSchema);

        mongoose.connect(process.env.MONGODB_URI)
            .then(() => {
                console.log("Verbunden mit MongoDB! (Permanente Speicherung aktiv)");
                useMongoDB = true;
                loadStars();
            })
            .catch(err => console.error("MongoDB Verbindungsfehler:", err));
    } catch (e) {
        console.error("Mongoose konnte nicht geladen werden. MongoDB deaktiviert.");
        useMongoDB = false;
    }
}
let gambleCooldowns = {};
let afkUsers = {};
let activeBlackjackGames = {};



// Persistence
const STARS_FILE = path.join(__dirname, '..', 'stars.json');
const REMINDERS_FILE = path.join(__dirname, '..', 'reminders.json');

async function loadStars() {
    if (useMongoDB) {
        try {
            const users = await User.find({});
            userStars = {};
            users.forEach(u => {
                userStars[u.username] = {
                    balance: u.balance,
                    lastClaim: u.lastClaim,
                    reminded: u.reminded,
                    pendingLoan: u.pendingLoan,
                    level: u.level || 0,
                    investedStars: u.investedStars || 0,
                    nextLevelCost: u.nextLevelCost || 670,
                    lastChannel: u.lastChannel,
                    loan: u.loan
                };
            });
            console.log(`Stars aus MongoDB geladen: ${Object.keys(userStars).length} User.`);
        } catch (e) {
            console.error("Fehler beim Laden von MongoDB:", e);
        }
    } else {
        try {
            if (fs.existsSync(STARS_FILE)) {
                const data = fs.readFileSync(STARS_FILE, 'utf8');
                userStars = JSON.parse(data);
                console.log(`Stars geladen: ${Object.keys(userStars).length} User.`);
            }
        } catch (e) {
            console.error("Fehler beim Laden der Stars:", e);
        }
    }
}

async function saveStars(specificUser = null) {
    if (useMongoDB) {
        try {
            // If we want to save all (rarely used now)
            if (!specificUser) {
                for (const [username, data] of Object.entries(userStars)) {
                    await User.findOneAndUpdate(
                        { username },
                        { ...data, username },
                        { upsert: true, new: true }
                    );
                }
            } else {
                // Save just the one changed user for performance
                await User.findOneAndUpdate(
                    { username: specificUser },
                    { ...userStars[specificUser], username: specificUser },
                    { upsert: true, new: true }
                );
            }
        } catch (e) {
            console.error("Fehler beim Speichern in MongoDB:", e);
        }
    } else {
        try {
            fs.writeFileSync(STARS_FILE, JSON.stringify(userStars, null, 2));
        } catch (e) {
            console.error("Fehler beim Speichern der Stars:", e);
        }
    }
}

let scheduledReminders = [];

function loadReminders() {
    try {
        if (fs.existsSync(REMINDERS_FILE)) {
            const data = fs.readFileSync(REMINDERS_FILE, 'utf8');
            scheduledReminders = JSON.parse(data);
            console.log(`Reminders geladen: ${scheduledReminders.length} ausstehend.`);

            // Restore timers for future reminders
            const now = Date.now();
            let count = 0;
            // Clean up old processed ones first just in case
            scheduledReminders = scheduledReminders.filter(r => r.dueTime > now);

            scheduledReminders.forEach(r => {
                setReminderTimeout(r);
                count++;
            });
            console.log(`${count} aktive Timer wiederhergestellt.`);
            saveReminders(); // Save cleaned list
        }
    } catch (e) {
        console.error("Fehler beim Laden der Reminders:", e);
    }
}

function saveReminders() {
    try {
        fs.writeFileSync(REMINDERS_FILE, JSON.stringify(scheduledReminders, null, 2));
    } catch (e) {
        console.error("Fehler beim Speichern der Reminders:", e);
    }
}

function setReminderTimeout(reminder) {
    const now = Date.now();
    const delay = Math.max(0, reminder.dueTime - now);

    // Safety cap for setTimeout is 24.8 days, assume user requests are shorter or we handle logic elsewhere
    // For simplicity, we just set it.

    const id = setTimeout(() => {
        const channel = process.env.TWITCH_CHANNEL;
        if (client.readyState() === 'OPEN') {
            client.say(channel, `/me @${reminder.target} Erinnerung von @${reminder.from}: ${reminder.message}`);
        }
        // Remove from list
        scheduledReminders = scheduledReminders.filter(r => r.id !== reminder.id);
        saveReminders();
    }, delay);

    activeTimers.push(id);
}

function addScheduledReminder(target, dueTime, message, from) {
    const reminder = {
        id: Date.now() + Math.random().toString(36).substr(2, 9),
        target,
        dueTime,
        message,
        from
    };
    scheduledReminders.push(reminder);
    saveReminders();
    setReminderTimeout(reminder);
}

function parseTimeStr(input) {
    // Regex looking for 'in Xh Ym' etc.
    // Supports: h, std, m, min, s, sec
    const timeFullRegex = /in\s+((?:(?:\d+)(?:h|std|m|min|s|sec)\s*)+)/i;
    const match = input.match(timeFullRegex);

    if (!match) return null; // No time specification found

    const timeStr = match[1];
    let ms = 0;

    const tokenRegex = /(\d+)\s*(h|std|m|min|s|sec)/gi;
    let tMatch;

    while ((tMatch = tokenRegex.exec(timeStr)) !== null) {
        const val = parseInt(tMatch[1]);
        const unit = tMatch[2].toLowerCase();

        if (unit.startsWith('h') || unit === 'std') {
            ms += val * 3600000;
        } else if (unit.startsWith('m')) { // m or min
            ms += val * 60000;
        } else if (unit.startsWith('s')) { // s or sec
            ms += val * 1000;
        }
    }

    if (ms === 0) return null;

    return {
        duration: ms,
        cleanText: input.replace(match[0], '').trim()
    };
}

// Combo State
let currentComboEmote = null;
let currentComboUsers = new Set();
let hasJoinedCombo = false;

// Time Checker State
let lastSchnapszahl = "";

function clearAllTimers() {
    activeTimers.forEach(id => clearTimeout(id));
    activeTimers = [];
}

function checkSchnapszahl() {
    const now = new Date().toLocaleString("en-US", { timeZone: "Europe/Berlin" });
    const berlinDate = new Date(now);
    const hours = berlinDate.getHours();
    const minutes = berlinDate.getMinutes();

    // Check for "Schnapszahl" (11:11, 22:22, 00:00, 01:01, etc.)
    // Logic: Hours equals Minutes.
    if (hours === minutes) {
        const pad = n => n < 10 ? '0' + n : n;
        const timeString = `${pad(hours)}:${pad(minutes)}`;

        if (lastSchnapszahl !== timeString) {
            const channels = process.env.TWITCH_CHANNEL.split(',').map(c => c.trim());
            // Ensure client is connected before sending
            if (client.readyState() === 'OPEN') {
                channels.forEach(ch => {
                    const target = ch.startsWith('#') ? ch : '#' + ch;
                    client.say(target, `wowii ${timeString}`);
                });
                lastSchnapszahl = timeString;
            }
        }
    }
}



// Check time every 5 seconds
setInterval(checkSchnapszahl, 5000);
// Check loans every minute
setInterval(checkLoans, 60000);

function formatPoints(points) {
    try {
        if (typeof points !== 'number') {
            return String(points);
        }
        return new Intl.NumberFormat("de-DE").format(points);
    } catch (e) {
        return String(points);
    }
}

function getDeck() {
    const suits = ['♠', '♥', '♦', '♣'];
    const values = ['2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K', 'A'];
    let deck = [];
    for (let s of suits) {
        for (let v of values) {
            deck.push({ value: v, suit: s });
        }
    }
    return deck.sort(() => Math.random() - 0.5);
}

function getHandValue(hand) {
    let value = 0;
    let aces = 0;
    for (let card of hand) {
        if (['J', 'Q', 'K'].includes(card.value)) {
            value += 10;
        } else if (card.value === 'A') {
            aces += 1;
            value += 11;
        } else {
            value += parseInt(card.value);
        }
    }
    while (value > 21 && aces > 0) {
        value -= 10;
        aces -= 1;
    }
    return value;
}

function formatHand(hand) {
    return hand.map(c => `${c.value}${c.suit}`).join(' ');
}

function doBlackjackStand(user, username, channel) {
    const game = activeBlackjackGames[user];
    if (!game) return;

    let dVal = getHandValue(game.dHand);

    // Dealer draws to 17
    while (dVal < 17) {
        game.dHand.push(game.deck.pop());
        dVal = getHandValue(game.dHand);
    }

    const pVal = getHandValue(game.pHand);

    let msg = `/me @${username} Stand. Du: [ ${formatHand(game.pHand)} ] (${pVal}) | Dealer: [ ${formatHand(game.dHand)} ] (${dVal}). `;

    if (dVal > 21) {
        // Dealer Bust
        const win = game.bet * 2;
        userStars[user].balance += win;
        msg += `Dealer Bust! Du gewinnst ${formatPoints(win - game.bet)} Star. Balance: ${formatPoints(userStars[user].balance)} Star`;
    } else if (pVal > dVal) {
        // Win
        const win = game.bet * 2;
        userStars[user].balance += win;
        msg += `Gewonnen! +${formatPoints(win - game.bet)} Star. Balance: ${formatPoints(userStars[user].balance)} Star`;
    } else if (pVal === dVal) {
        // Push
        userStars[user].balance += game.bet;
        msg += `Unentschieden. Du behältst deinen Einsatz. Balance: ${formatPoints(userStars[user].balance)} Star`;
    } else {
        // Loss
        msg += `Verloren. -${formatPoints(game.bet)} Star. Balance: ${formatPoints(userStars[user].balance)} Star`;
    }

    saveStars();
    client.say(channel, msg);
    delete activeBlackjackGames[user];
}

function checkLoans() {

    const now = Date.now();
    let changed = false;

    for (const user in userStars) {
        const data = userStars[user];
        if (data.loan && data.loan.active) {
            const hoursSinceStart = (now - data.loan.startTime) / 3600000;
            const hoursTracked = data.loan.hoursTracked || 0;

            // If we have passed a new hour threshold
            if (hoursSinceStart >= hoursTracked + 1 && hoursTracked < 6) {
                // Apply interest
                let interestRate = 0.067; // 6.7%

                if (hoursTracked + 1 === 6) {
                    interestRate = 0.167; // 16.7%
                }

                const oldDebt = data.loan.debt;
                const newDebt = Math.floor(oldDebt * (1 + interestRate));

                data.loan.debt = newDebt;
                data.loan.hoursTracked = hoursTracked + 1;

                changed = true;

                console.log(`Zinsen für ${user}: ${oldDebt} -> ${newDebt} (Stunde ${data.loan.hoursTracked})`);
            }

            // Check for Default/Deadline (e.g. 5 minutes AFTER the 6th hour interest)
            // 6.0 hours = 6th interest applied.
            // 6.1 hours (~6m later) = Timeout if not paid.
            if (hoursSinceStart >= 6.1) {
                const channel = process.env.TWITCH_CHANNEL;
                const debt = data.loan.debt;

                // Timeout amount in seconds. Cap at 2 weeks (1209600s) just in case.
                const timeoutDuration = Math.min(debt, 1209600);

                if (client.readyState() === 'OPEN') {
                    client.timeout(channel, user, timeoutDuration, "Kredit nicht zurückgezahlt!")
                        .then(() => {
                            client.say(channel, `/timeout ${user} ${timeoutDuration}`);
                            client.say(channel, `/me @${user} hat seinen Kredit nicht bezahlt haher Timeout für ${timeoutDuration} Sekunden! Rest in Peace o7`);
                        })
                        .catch(err => {
                            console.error(`Konnte ${user} nicht timeouten:`, err);
                            client.say(channel, `/me @${user} hat Glück. Aber Kredit ist weg. Top`);
                        });
                }

                // Clear stats
                data.balance = 0; // Bankrupt
                data.loan = {
                    active: false,
                    amount: 0,
                    debt: 0,
                    startTime: 0,
                    hoursTracked: 0
                };
                changed = true;
            }
        }
    }

    if (changed) saveStars();
}

function scheduleStarReminder(username, delay, channel = null) {
    if (delay < 0) delay = 0;
    setTimeout(() => {
        // Use provided channel, or stored lastChannel, or fallback to first channel in config
        let targetChannel = channel;

        if (!targetChannel && userStars[username] && userStars[username].lastChannel) {
            targetChannel = userStars[username].lastChannel;
        }

        if (!targetChannel) {
            targetChannel = process.env.TWITCH_CHANNEL.split(',')[0].trim();
        }

        if (targetChannel && !targetChannel.startsWith('#')) {
            targetChannel = '#' + targetChannel;
        }

        if (client.readyState() === 'OPEN' && targetChannel) {
            client.say(targetChannel, `/me @${username} bingi hol deine Star ab mit ${currentPrefix}star`);
            if (userStars[username]) {
                userStars[username].reminded = true;
                saveStars(username);
            }
        }
    }, delay);
}

function restoreStarReminders() {
    const now = Date.now();
    const cooldown = 3600000;
    let restoredCount = 0;

    for (const user in userStars) {
        // Migration: If reminded is undefined, assume true (don't spam old users) or false?
        // Let's assume false if they are in cooldown?
        const data = userStars[user];
        if (data.reminded === true) continue;

        // Logic:
        // claimTime + cooldown = dueTime.
        // If dueTime > now: wait (dueTime - now).
        // If dueTime <= now: ping immediately!

        const dueTime = (data.lastClaim || 0) + cooldown;
        if (now < dueTime) {
            // Still in cooldown, schedule future reminder
            scheduleStarReminder(user, dueTime - now, data.lastChannel);
            restoredCount++;
        } else {
            // Cooldown expired while offline, or never claimed
            // If reminded is explicitly false or undefined (not true)
            if (data.reminded !== true) {
                scheduleStarReminder(user, 1000, data.lastChannel); // Remind shortly
                restoredCount++;
            }
        }
    }
    if (restoredCount > 0) console.log(`Wiederhergestellt: ${restoredCount} Star-Reminders.`);
}

async function refreshEmotes() {
    try {
        const channels = process.env.TWITCH_CHANNEL.split(',').map(c => c.trim());
        const token = process.env.TWITCH_OAUTH_TOKEN;

        let clientId;
        try {
            clientId = await getClientId(token);
        } catch (e) {
            console.error("Auth Error: Token ungültig? Bitte .env prüfen.");
            return;
        }

        let allEmotes = [];
        for (const channelName of channels) {
            console.log(`Lade 7TV Emotes für: ${channelName}...`);
            const userId = await getTwitchUserId(channelName, clientId, token);

            if (userId) {
                channelIds[channelName.toLowerCase()] = userId;
                const emotes = await get7TVEmotes(userId);
                allEmotes = allEmotes.concat(emotes.map(e => e.name));
            } else {
                console.error(`Konnte Twitch User ID für ${channelName} nicht finden.`);
            }
        }

        // Set of unique emotes
        cachedEmotes = Array.from(new Set(allEmotes));
        console.log(`Erfolg! ${cachedEmotes.length} Emotes insgesamt geladen.`);
    } catch (e) {
        console.error('Fehler beim Laden der Emotes:', e);
    }
}

client.connect().catch(console.error);

client.on('message', async (channel, tags, message, self) => {
    // Ignore echoed messages.
    if (self) return;

    const sender = tags.username.toLowerCase();
    activeChatUsers.add(tags.username);

    // --- Message Counting & Daily Reset ---
    const today = new Date().toLocaleString("en-US", { timeZone: "Europe/Berlin" }).split(',')[0];
    if (useMongoDB && ChatStat) {
        try {
            await ChatStat.findOneAndUpdate(
                { date: today, username: sender },
                { $inc: { count: 1 } },
                { upsert: true }
            );
        } catch (e) {
            console.error("Fehler beim Speichern der Chat-Statistik:", e);
        }
    } else {
        if (today !== lastResetDate) {
            messageCounts = {};
            lastResetDate = today;
            console.log("Daily message counts reset.");
        }
        messageCounts[sender] = (messageCounts[sender] || 0) + 1;
    }

    let emote = "";
    if (cachedEmotes.length > 0) {
        emote = cachedEmotes[Math.floor(Math.random() * cachedEmotes.length)];
    }



    // --- AFK Check ---

    if (cachedEmotes.length > 0) {
        emote = cachedEmotes[Math.floor(Math.random() * cachedEmotes.length)];
    }

    // --- AFK Check ---
    if (afkUsers[sender]) {
        if (!message.startsWith(currentPrefix + 'afk')) { // Don't trigger on the AFK command itself
            const startTime = afkUsers[sender];
            const durationMs = Date.now() - startTime;

            // Format duration
            const seconds = Math.floor((durationMs / 1000) % 60);
            const minutes = Math.floor((durationMs / (1000 * 60)) % 60);
            const hours = Math.floor((durationMs / (1000 * 60 * 60)));

            let timeString = "";
            if (hours > 0) timeString += `${hours}h `;
            if (minutes > 0) timeString += `${minutes}m `;
            timeString += `${seconds}s`;

            client.say(channel, `/me halo @${tags.username} ist nach ${timeString.trim()} wieder da ${emote}`);
            delete afkUsers[sender];
        }
    }

    // --- Check for Reminders ---
    if (pendingReminders[sender] && pendingReminders[sender].length > 0) {
        pendingReminders[sender].forEach(rem => {
            client.say(channel, `/me @${tags.username}, Erinnerung von @${rem.from}: ${rem.message}`);
        });
        delete pendingReminders[sender];
    }

    // --- Loan Confirmation Listener ---
    if (userStars[sender] && userStars[sender].pendingLoan) {
        const response = message.trim().toLowerCase();
        if (response === 'ja' || response === 'nein') {
            if (response === 'nein') {
                userStars[sender].pendingLoan = false;
                client.say(channel, `/me @${tags.username} Kredit abgelehnt. Smart`);
                saveStars();
            } else {
                // Generate Loan
                const amount = Math.floor(Math.random() * (676767 - 67 + 1)) + 67;
                userStars[sender].balance += amount;
                userStars[sender].loan = {
                    active: true,
                    amount: amount, // Original principal
                    debt: amount, // Current debt
                    startTime: Date.now(),
                    hoursTracked: 0
                };
                userStars[sender].pendingLoan = false;
                saveStars();
                userStars[sender].pendingLoan = false;
                saveStars();
                client.say(channel, `/me @${tags.username} Kredit genehmigt! Du hast ${formatPoints(amount)} Star erhalten. Viel Glück beim Zurückzahlen in 6h...`);
            }

            return; // Stop processing other commands
        }
    }

    // --- Emote Combo Logic ---
    const msgContent = message.trim();
    if (cachedEmotes.includes(msgContent)) {
        if (msgContent === currentComboEmote) {
            currentComboUsers.add(sender);
            if (currentComboUsers.size >= 3) {
                client.say(channel, msgContent);
                currentComboUsers.clear(); // Reset to count next 3
            }
        } else {
            currentComboEmote = msgContent;
            currentComboUsers = new Set([sender]);
        }
    }

    // --- Special Mode: Waiting for new Prefix ---
    if (isChangingPrefix) {
        // Optional: Only allow the user who started the change to finish it
        if (prefixChangeUser && tags.username !== prefixChangeUser) return;

        const newPrefix = message.trim().split(' ')[0]; // Take first word/character
        if (newPrefix) {
            currentPrefix = newPrefix;
            isChangingPrefix = false;
            prefixChangeUser = null;
            client.say(channel, `wideSpeedNod neuer prefix: ${currentPrefix}`);
        } else {
            client.say(channel, "wideSpeedNod mit dem prefix gehts nicht");
            isChangingPrefix = false;
            prefixChangeUser = null;
        }
        return; // Don't process this message as a command
    }

    // --- Fortnite Dialog Script ---
    if (message.toLowerCase().includes('ey joel') && !self) {
        const dialog = [
            "ja bruder was los?",
            "du spielst doch fortnite ne?",
            "ja und jetzt?",
            "ja.. du bist voll der opfer junge",
            "was für opfer digga, ich sag dir ganz ehrlich digga",
            "das game an sich fortnite ist so geil digga, aber einfach nur diese kleinen kinder",
            "haben dieses game so kaputt gemacht digga, neue map hier digga, neue map da digga",
            "sie wünschen sich alles digga und wenn ich mal 1v1 gegen die mache digga und die verkacken, die beleidigen, die beleidigen mich direkt als hs",
            "oder generell sie swipen durch ihre tiktok fy und schreiben unter jedes video hs digga daraus besteht fortnite digga",
            "sonst an sich fortnite ist so ein geiles prinzip digga",
            "früher du hast gezockt es war alles wild digga keiner konnte was",
            "früher du hast fun an fortnite gehabt digga und jetzt einfach bruder jetzt besteht dieses game aus irgendwelchen kindern digga die nur beleidigen weil sie verkacken digga das ist einfach fortnite digga ganz ehrlich an sich ich sag dir ganz ehrlich digga fortnite ist so ein wildes game bruder ich sag dir ganz ehrlich digga."
        ];

        // Send messages sequentially
        for (let i = 0; i < dialog.length; i++) {
            const id = setTimeout(() => {
                client.say(channel, dialog[i]);
            }, i * 2000);
            activeTimers.push(id);
        }
        return;
    }

    if (message.toLowerCase().includes('hast du anna') && !self) {
        const dialog = [
            "...schon mal an der möse geleckt fragt der motherfucker AufLock",
            "ne digga hab ich nicht nope",
            "dafür deiner schwester LOL",
        ];

        // Send messages sequentially
        for (let i = 0; i < dialog.length; i++) {
            const id = setTimeout(() => {
                client.say(channel, dialog[i]);
            }, i * 1500);
            activeTimers.push(id);
        }
        return;
    }

    // --- Baka Script ---
    if (message.trim().toLowerCase() === 'baka' && !self) {
        const dialog = [
            "/me oioioi baka",
            "/me oioioi oioioi baka",
            "/me oioioi oioioi oioioi baka",
            "/me oioioi oioioi oioioi oioioi baka",
            "/me oioioi oioioi oioioi oioioi oioioi baka",
            "/me oioioi oioioi oioioi oioioi oioioi oioioi baka",
            "/me oioioi oioioi oioioi oioioi oioioi oioioi oioioi baka",
            "/me oioioi oioioi oioioi oioioi oioioi oioioi oioioi oioioi baka",
            "/me oioioi oioioi oioioi oioioi oioioi oioioi oioioi oioioi oioioi baka",
            "/me oioioi oioioi oioioi oioioi oioioi oioioi oioioi oioioi oioioi oioioi baka",
            "/me oioioi oioioi oioioi oioioi oioioi oioioi oioioi oioioi oioioi oioioi oioioi baka",
            "/me oioioi oioioi oioioi oioioi oioioi oioioi oioioi oioioi oioioi oioioi oioioi oioioi baka",
        ];

        for (let i = 0; i < dialog.length; i++) {
            const id = setTimeout(() => {
                client.say(channel, dialog[i]);
            }, i * 100);
            activeTimers.push(id);
        }
        return;
    }

    if (message.startsWith(currentPrefix)) {
        const args = message.slice(currentPrefix.length).split(' ');
        const command = args.shift().toLowerCase(); // Remove prefix and get command

        if (command === 'hilfe') {
            client.say(channel, `Befehle: ${currentPrefix}suche <Hinweis>, ${currentPrefix}remind <user> <msg>, ${currentPrefix}randomemote, ${currentPrefix}ping, ${currentPrefix}prefix, ${currentPrefix}frage, ${currentPrefix}spam, ${currentPrefix}emotes, ${currentPrefix}hug, ${currentPrefix}star, ${currentPrefix}topchatter, ${currentPrefix}stop, ${currentPrefix}befehle`);
        }

        if (command === 'commands' || command === 'befehle') {
            const allCommands = [
                'hilfe', 'ping', 'prefix', 'randomemote', 'refresh', 'reload', 'frage',
                'topdebt', 'schulden', 'stop', 'afk', 'emotes', 'spam', 'remindme',
                'remind', 'star', 'hug', 'suche', 'guess', 'gamba', 'bj', 'blackjack',
                'hit', 'h', 'stand', 's', 'balance', 'stars', 'give', 'pay', 'lb',
                'leaderboard', 'allstars', 'listall', 'kredit', 'repay', 'payback',
                'tc', 'topchatter', 'commands', 'levelup', 'kok'
            ];

            let header = "Nerd commands: ";
            let currentMsg = header;

            for (let i = 0; i < allCommands.length; i++) {
                let emote = "";
                if (cachedEmotes.length > 0) {
                    emote = cachedEmotes[Math.floor(Math.random() * cachedEmotes.length)];
                }

                // If no emotes, we just use a space or a dash
                const prefix = emote ? emote + " " : "- ";
                const cmdEntry = `${prefix}${allCommands[i]} `;

                if (currentMsg.length + cmdEntry.length > 400) {
                    client.say(channel, currentMsg.trim());
                    currentMsg = cmdEntry;
                } else {
                    currentMsg += cmdEntry;
                }
            }

            if (currentMsg.trim() !== "") {
                client.say(channel, currentMsg.trim());
            }
        }

        if (command === 'ping') {
            client.say(channel, 'anwesend bin da');
        }

        if (command === 'prefix') {
            isChangingPrefix = true;
            prefixChangeUser = tags.username;
            client.say(channel, `Nerd was willst du als prefix? aktuell hast du: ${currentPrefix}`);
        }

        if (command === 'randomemote') {
            if (cachedEmotes.length === 0) await refreshEmotes();
            if (cachedEmotes.length > 0) {
                const randomEmote = cachedEmotes[Math.floor(Math.random() * cachedEmotes.length)];
                client.say(channel, `/me random emote: ${randomEmote}`);
            } else {
                client.say(channel, "/me irgendwie keine emotes gefunden lol");
            }
        }

        if (command === 'refresh' || command === 'reload') {
            const isMod = tags.mod || (tags.badges && tags.badges.broadcaster);
            if (!isMod) {
                client.say(channel, `@${tags.username} Nerd du bist kein Mod`);
                return;
            }
            await refreshEmotes();
            client.say(channel, `/me System reloaded! Emotes: ${cachedEmotes.length}`);
        }

        if (command === 'frage') {
            const answers = [
                "/me Genau ja", "/me nope nein", "/me eeh vielleicht", "/me Skip frag später nochmal",
                "/me Genau auf jeden fall", "/me nope niemals", "/me eeh wahrscheinlich schon",
                "/me manidk ich glaube nicht", "/me manik definitiv", "/me haher träum weiter"
            ];
            const randomAnswer = answers[Math.floor(Math.random() * answers.length)];
            client.say(channel, `${randomAnswer}`);
        }

        if (command === 'topdebt' || command === 'schulden') {
            const debtors = Object.entries(userStars)
                .filter(([name, data]) => data.loan && data.loan.active)
                .map(([name, data]) => ({ name, debt: data.loan.debt }))
                .sort((a, b) => b.debt - a.debt);

            if (debtors.length === 0) {
                client.say(channel, `/me Niemand hat schulden wowii`);
                return;
            }

            const top = debtors[0];
            client.say(channel, `/me DprePffttt @${top.name} hat die meisten schulden: ${formatPoints(top.debt)} Star`);

        }

        if (command === 'stop') {
            clearAllTimers();
            client.say(channel, "bob bin schon leise");
        }

        if (command === 'afk') {
            afkUsers[tags.username.toLowerCase()] = Date.now();
            client.say(channel, `/me ${emote} @${tags.username} ist jetzt AFK bye`);
        }


        if (command === 'emotes') {
            if (cachedEmotes.length === 0) await refreshEmotes();
            client.say(channel, `/me ich spamme jetzt ${cachedEmotes.length} emotes (dauert maybe ~${Math.round(cachedEmotes.length / 60)} min)...`);
            for (let i = 0; i < cachedEmotes.length; i++) {
                const id = setTimeout(() => {
                    client.say(channel, cachedEmotes[i]);
                }, i * 900);
                activeTimers.push(id);
            }
        }

        if (command === 'spam') {
            const count = parseInt(args[0]);
            const textToSpam = args.slice(1).join(' ');

            if (isNaN(count) || count < 1 || count > 50) {
                client.say(channel, '/me Nerd es geht nur von 1-50');
                return;
            }

            if (!textToSpam) {
                client.say(channel, '/me bob was soll ich spammen');
                return;
            }

            for (let i = 0; i < count; i++) {
                const id = setTimeout(() => {
                    client.say(channel, textToSpam);
                }, i * 40);
                activeTimers.push(id);
            }
        }

        if (command === 'remindme') {
            const text = args.join(' ');
            if (!text) {
                client.say(channel, `/me @${tags.username} Nerd Nutzung: ${currentPrefix}remindme in 2h 30m <Nachricht>`);
                return;
            }

            const timeData = parseTimeStr(text);
            if (!timeData) {
                client.say(channel, `/me @${tags.username} Nerd try mal sowas wie 'in 2h 10min wäsche'`);
                return;
            }

            const dueTime = Date.now() + timeData.duration;
            addScheduledReminder(tags.username, dueTime, timeData.cleanText || "Zeit ist um!", tags.username);

            // Format duration for confirmation
            const minutes = Math.round(timeData.duration / 60000);
            client.say(channel, `/me @${tags.username} ich reminde dich in ca. ${minutes} min Top`);
        }

        if (command === 'remind') {
            const target = args[0];
            const rest = args.slice(1).join(' ');

            if (!target || !rest) {
                client.say(channel, `/me Nerd benutzung: ${currentPrefix}remind <user> (in 1h) <nachricht>`);
                return;
            }

            const targetUser = target.toLowerCase().replace('@', '');

            const timeData = parseTimeStr(rest);
            if (timeData) {
                // Scheduled Reminder
                const dueTime = Date.now() + timeData.duration;
                addScheduledReminder(targetUser, dueTime, timeData.cleanText || "Ping!", tags.username);
                const minutes = Math.round(timeData.duration / 60000);
                client.say(channel, `/me @${tags.username} reminder für @${targetUser} in ${minutes}min gesetzt Top`);
            } else {
                // Passive Reminder
                if (!pendingReminders[targetUser]) {
                    pendingReminders[targetUser] = [];
                }
                pendingReminders[targetUser].push({
                    from: tags.username,
                    message: rest
                });
                client.say(channel, `/me Hm ich schreib @${targetUser} beim nächsten chatten`);
            }
        }

        if (command === 'star') {
            const user = tags.username.toLowerCase();
            const now = Date.now();
            const cooldown = 3600000; // 1 Hour

            if (!userStars[user]) {
                // First Time
                userStars[user] = {
                    balance: 0,
                    lastClaim: 0,
                    level: 0,
                    investedStars: 0,
                    nextLevelCost: 670,
                    lastChannel: channel
                };
            }

            const lastClaim = userStars[user].lastClaim;
            if (now - lastClaim < cooldown) {
                const minutesLeft = Math.ceil((cooldown - (now - lastClaim)) / 60000);
                client.say(channel, `/me @${tags.username}, Nerd warte noch ${minutesLeft} minuten (balance: ${formatPoints(userStars[user].balance)} Star )`);
                return;
            }

            // Calculate Reward
            let reward = Math.floor(Math.random() * (677 - 67 + 1)) + 67;
            let isFirst = false;

            if (userStars[user].lastClaim === 0) {
                isFirst = true;
                reward += 676; // Bonus
            }

            userStars[user].balance += reward;
            userStars[user].lastClaim = now;
            userStars[user].reminded = false; // Reset reminder flag
            userStars[user].lastChannel = channel; // Store last used channel
            saveStars(user);

            if (isFirst) {
                client.say(channel, `/me qq @${tags.username} da du das erste mal hier bist bekommst du ein bonus JUHU , (${formatPoints(reward - 676)} + 676 bonus) dein aktueller Star betrag ist ${formatPoints(userStars[user].balance)} Star`);
            } else {
                client.say(channel, `/me @${tags.username} du hast ${formatPoints(reward)} Star bekommen Top total: ${formatPoints(userStars[user].balance)} Star `);
            }

            // Set Reminder
            scheduleStarReminder(user, cooldown, channel);
        }

        // Helper to get all current viewers via NotedBot API
        async function getViewers() {
            try {
                const pureChannelName = channel.replace('#', '').toLowerCase();
                let targetId = channelIds[pureChannelName];

                if (!targetId) {
                    // Try to get ID if not set
                    const token = process.env.TWITCH_OAUTH_TOKEN;
                    const clientId = await getClientId(token);
                    targetId = await getTwitchUserId(pureChannelName, clientId, token);
                    if (targetId) channelIds[pureChannelName] = targetId;
                }

                if (!targetId) {
                    console.error("Keine Channel ID gefunden.");
                    return [];
                }

                const response = await fetch(`https://api.notedbot.de/chatters?channelId=${targetId}`);

                if (!response.ok) {
                    console.error('Failed to fetch viewers from NotedBot:', response.status);
                    return [];
                }

                const json = await response.json();

                // Check if data is null or error is true
                if (json.error || !json.data) {
                    console.warn('NotedBot API returned no data:', json.message);
                    return [];
                }

                const data = json.data;
                let allViewers = [];

                // Helper to extract logins from array of objects { login: "name" }
                const extract = (arr) => (arr || []).map(u => u.login);

                allViewers = allViewers.concat(extract(data.broadcasters));
                allViewers = allViewers.concat(extract(data.chatbots)); // Include bots? Sure, why not
                allViewers = allViewers.concat(extract(data.moderators));
                allViewers = allViewers.concat(extract(data.vips));
                allViewers = allViewers.concat(extract(data.staff));
                allViewers = allViewers.concat(extract(data.viewers));

                return allViewers;

            } catch (e) {
                console.error('Error fetching viewers:', e);
                return [];
            }
        }

        if (command === 'hug') {
            let users = await getViewers();

            // Fallback to active chatters if API fails or returns empty
            if (!users || users.length === 0) {
                users = Array.from(activeChatUsers);
            }

            const otherUsers = users.filter(u => u.toLowerCase() !== tags.username.toLowerCase());

            let targetUser = tags.username;
            if (otherUsers.length > 0) {
                targetUser = otherUsers[Math.floor(Math.random() * otherUsers.length)];
            }

            client.say(channel, `/me @${tags.username} umarmt @${targetUser} hugg`);
        }

        if (command === 'suche' || command === 'guess') {
            if (cachedEmotes.length === 0) {
                await refreshEmotes();
                if (cachedEmotes.length === 0) {
                    client.say(channel, "eeeh lwk gibts hier keine emotes, guck mal ob du 7tv hast");
                    return;
                }
            }

            const hintText = args.join(' ');
            if (!hintText.trim()) return;

            const filters = parseHint(hintText);

            if (filters.length === 0) {
                client.say(channel, "peepoConfused ich verstehe nichts, try so '14 2=c' oder 'hat 7 buchstaben'");
                return;
            }

            // Filter emotes
            const matches = cachedEmotes.filter(name => {
                return filters.every(f => f(name));
            });

            if (!matches.length) {
                client.say(channel, "lol hab nichts gefunden");
            } else if (matches.length > 20) {
                let chunk = [];
                for (const emote of matches) {
                    chunk.push(emote);
                    if (chunk.length >= 25) {
                        client.say(channel, `Nerd das kanns sein: ${chunk.join(' , ')}`);
                        chunk = [];
                    }
                }
                if (chunk.length > 0) {
                    client.say(channel, `Nerd das kanns sein: ${chunk.join(' , ')}`);
                }
            } else {
                client.say(channel, `Nerd das kanns sein: ${matches.join(" , ")}`);
            }
        }
        if (command === 'gamba') {
            const user = tags.username.toLowerCase();
            const amountStr = args[0];

            if (!userStars[user]) {
                userStars[user] = { balance: 0, lastClaim: 0, level: 0, investedStars: 0, nextLevelCost: 670, lastChannel: channel };
            }
            userStars[user].lastChannel = channel;

            // Cooldown Check (10s)
            const now = Date.now();
            if (gambleCooldowns[user] && now < gambleCooldowns[user]) {
                return; // Silent ignore or minimal message? User usually spams, so silent or short is good.
                // client.say(channel, `@${tags.username} chill mal kurz (Cooldown)`);
            }
            gambleCooldowns[user] = now + 5000;

            const balance = userStars[user].balance;

            if (!amountStr) {
                client.say(channel, `/me @${tags.username} Nutzung: ${currentPrefix}gamba <Menge> oder 'all'`);
                return;
            }

            let betAmount = 0;
            if (amountStr.toLowerCase() === 'all') {
                betAmount = balance;
            } else if (amountStr.toLowerCase() === 'half' || amountStr.toLowerCase() === 'hälfte') {
                betAmount = Math.floor(balance / 2);
            } else if (amountStr.endsWith('%')) {
                const percentage = parseInt(amountStr.slice(0, -1));
                if (!isNaN(percentage) && percentage > 0 && percentage <= 100) {
                    betAmount = Math.ceil(balance * (percentage / 100));
                }
            } else {
                betAmount = parseInt(amountStr);
            }

            if (isNaN(betAmount) || betAmount <= 0) {
                client.say(channel, `/me @${tags.username} Ungültiger Einsatz bob `);
                return;
            }

            if (betAmount > balance) {
                client.say(channel, `/me @${tags.username} idiot du hast nur ${formatPoints(balance)} Star`);
                return;
            }


            if (cachedEmotes.length < 3) {
                client.say(channel, `/me @${tags.username} Um keine emotes für gamba`);
                return;
            }

            // Game Logic
            // 1% Jackpot (Triple)
            // 67% Win (Double)
            // 16% Near Miss (Loss)
            // 16% Loss

            const roll = Math.random() * 100; // 0 - 100
            let resultSlots = [];
            let outcome = ""; // win, jackpot, loss

            // Pick a set of 3 distinct symbols for the reels to choose from
            let reelSymbols = [];
            while (reelSymbols.length < 3) {
                const r = cachedEmotes[Math.floor(Math.random() * cachedEmotes.length)];
                if (!reelSymbols.includes(r)) reelSymbols.push(r);
            }

            if (roll < 1) {
                // Jackpot 1%
                outcome = "jackpot";
                const s = reelSymbols[0];
                resultSlots = [s, s, s];
            } else if (roll < 68) {
                // Win 67% (1 to 68)
                outcome = "win";
                const s = reelSymbols[0];
                resultSlots = [s, s, s];
            } else if (roll < 84) {
                // 16% (68 to 84) -> 2 Same
                outcome = "loss"; // Near miss is a loss
                // [A, A, B] shuffled
                const s1 = reelSymbols[0];
                const s2 = reelSymbols[1];
                resultSlots = [s1, s1, s2];
                resultSlots.sort(() => Math.random() - 0.5);
            } else {
                // 16% (84 to 100) -> 3 Diff
                outcome = "loss";
                resultSlots = reelSymbols;
                resultSlots.sort(() => Math.random() - 0.5);
            }

            if (outcome === "jackpot") {
                const winAmount = betAmount * 3;
                userStars[user].balance = balance - betAmount + winAmount;
                saveStars();
                client.say(channel, `/me [ ${resultSlots.join(' | ')} ] - @${tags.username} HeCrazy JACKPOT HeCrazy  VERDREIFACHT HeCrazy balance: ${formatPoints(userStars[user].balance)} Star`);
            } else if (outcome === "win") {
                const winAmount = betAmount * 2;
                userStars[user].balance = balance - betAmount + winAmount;
                saveStars();
                client.say(channel, `/me [ ${resultSlots.join(' | ')} ] - @${tags.username} ALTA gewonnen, aktuelle balance: ${formatPoints(userStars[user].balance)} Star`);
            } else {
                userStars[user].balance = balance - betAmount;
                saveStars();
                client.say(channel, `/me [ ${resultSlots.join(' | ')} ] - @${tags.username} eww verloren, aktuelle balance: ${formatPoints(userStars[user].balance)} Star`);
            }

        }

        if (command === 'bj' || command === 'blackjack') {
            const user = tags.username.toLowerCase();

            if (activeBlackjackGames[user]) {
                client.say(channel, `/me @${tags.username} du hast schon ein spiel offen ADHD schreib '${currentPrefix}hit' oder '${currentPrefix}stand' wideSpeedNod `);
                return;
            }

            const amountStr = args[0];
            if (!userStars[user]) {
                userStars[user] = { balance: 0, lastClaim: 0, level: 0, investedStars: 0, nextLevelCost: 670, lastChannel: channel };
            }
            userStars[user].lastChannel = channel;
            const balance = userStars[user].balance;

            if (!amountStr) {
                client.say(channel, `/me @${tags.username} Nerd Nutzung: ${currentPrefix}bj <Menge> oder 'all'`);
                return;
            }

            let betAmount = 0;
            if (amountStr.toLowerCase() === 'all') {
                betAmount = balance;
            } else if (amountStr.toLowerCase() === 'half' || amountStr.toLowerCase() === 'hälfte') {
                betAmount = Math.floor(balance / 2);
            } else if (amountStr.endsWith('%')) {
                const percentage = parseInt(amountStr.slice(0, -1));
                if (!isNaN(percentage) && percentage > 0 && percentage <= 100) {
                    betAmount = Math.ceil(balance * (percentage / 100));
                }
            } else {
                betAmount = parseInt(amountStr);
            }

            if (isNaN(betAmount) || betAmount <= 0) {
                client.say(channel, `/me @${tags.username} Ungültiger Einsatz bob `);
                return;
            }

            if (betAmount > balance) {
                client.say(channel, `/me @${tags.username} idiot du hast nur ${formatPoints(balance)} Star`);
                return;
            }

            // Deduct bet
            userStars[user].balance -= betAmount;
            saveStars();

            const deck = getDeck();
            const pHand = [deck.pop(), deck.pop()];
            const dHand = [deck.pop(), deck.pop()];

            activeBlackjackGames[user] = {
                deck: deck,
                pHand: pHand,
                dHand: dHand,
                bet: betAmount,
                ts: Date.now()
            };

            const pVal = getHandValue(pHand);

            // Check Natural Blackjack
            if (pVal === 21) {
                const dVal = getHandValue(dHand);
                if (dVal === 21) {
                    // Push
                    userStars[user].balance += betAmount;
                    saveStars();
                    client.say(channel, `/me wideSpeedNod @${tags.username} blackjack push du: [${formatHand(pHand)}] dealer: [${formatHand(dHand)}], balance: ${formatPoints(userStars[user].balance)} Star`);
                } else {
                    // Win 1.5x (Net win 1.5x, so return 2.5x bet)
                    const win = Math.ceil(betAmount * 2.5);
                    userStars[user].balance += win;
                    saveStars();
                    client.say(channel, `/me wideSpeedNod @${tags.username} BLACKJACK du: [${formatHand(pHand)}] dealer: [${formatHand(dHand)}], Gewinn: ${formatPoints(win - betAmount)} balance: ${formatPoints(userStars[user].balance)} Star`);
                }
                delete activeBlackjackGames[user];
                return;
            }

            client.say(channel, `/me wideSpeedNod @${tags.username} blackjack gestartet, einsatz: ${formatPoints(betAmount)}, deine hand: [ ${formatHand(pHand)} ] (${pVal}) | dealer: [ ${dHand[0].value}${dHand[0].suit} ? ] , hit oder stand? Hmmm `);
        }

        if (command === 'hit' || command === 'h') {
            const user = tags.username.toLowerCase();
            const game = activeBlackjackGames[user];
            if (!game) return;

            const card = game.deck.pop();
            game.pHand.push(card);
            const val = getHandValue(game.pHand);

            if (val > 21) {
                // Bust
                client.say(channel, `/me @${tags.username} BUST ohno [ ${formatHand(game.pHand)} ] (${val}), du verlierst ${formatPoints(game.bet)} Star , balance: ${formatPoints(userStars[user].balance)} Star`);
                delete activeBlackjackGames[user];
            } else if (val === 21) {
                // Auto-stand
                doBlackjackStand(user, tags.username, channel);
            } else {
                client.say(channel, `/me @${tags.username} [ ${formatHand(game.pHand)} ] (${val}) | dealer: [ ${game.dHand[0].value}${game.dHand[0].suit} ? ] wideSpeedNod`);
            }
        }

        if (command === 'stand' || command === 's') {
            const user = tags.username.toLowerCase();
            if (!activeBlackjackGames[user]) return;
            doBlackjackStand(user, tags.username, channel);
        }

        if (command === 'balance' || command === 'stars') {
            const target = args[0] ? args[0].toLowerCase().replace('@', '') : tags.username.toLowerCase();

            if (!userStars[target]) {
                client.say(channel, `/me @${tags.username} der user ${target} hat keine Star Reacting`);
            } else {
                userStars[tags.username.toLowerCase()].lastChannel = channel; // Also update sender's channel
                const data = userStars[target];
                const totalStanding = (data.balance || 0) + (data.investedStars || 0);
                let msg = `/me @${tags.username} der user ${target} hat ${formatPoints(data.balance)} Star (lvl ${data.level || 0}, gesamt: ${formatPoints(totalStanding)})`;

                if (data.loan && data.loan.active) {
                    const debt = data.loan.debt;
                    const net = data.balance - debt;

                    if (data.balance === 0) {
                        msg += ` (ACHTUNG: Laufender Kredit! Schulden: ${formatPoints(debt)} Star !)`;
                    } else {
                        msg += ` (Laufender Kredit: -${formatPoints(debt)} Star | Netto: ${formatPoints(net)} Star)`;
                    }
                }


                client.say(channel, msg);
            }
        }

        if (command === 'kok') {
            const length = Math.floor(Math.random() * 167); // 0 to 30 cm
            client.say(channel, `/me @${tags.username} kok länge beträgt ${length} cm Reacting`);
        }

        if (command === 'pussy') {
            const length = Math.floor(Math.random() * 167); // 0 to 30 cm
            client.say(channel, `/me @${tags.username} Pussy länge beträgt -${length} cm Reacting`);
        }

        if (command === 'give' || command === 'pay') {
            const target = args[0];
            const amountStr = args[1];

            if (!target || !amountStr) {
                client.say(channel, `/me @${tags.username} Nutzung: ${currentPrefix}give <User> <Menge>`);
                return;
            }

            const sender = tags.username.toLowerCase();
            const receiver = target.toLowerCase().replace('@', '');

            if (sender === receiver) {
                client.say(channel, `/me @${tags.username} du kannst dir selbst nichts geben lol`);
                return;
            }

            if (!userStars[sender]) {
                userStars[sender] = { balance: 0, lastClaim: 0, level: 0, investedStars: 0, nextLevelCost: 670, lastChannel: channel };
            }
            userStars[sender].lastChannel = channel;

            let amount = parseInt(amountStr);
            if (amountStr.toLowerCase() === 'all') {
                amount = userStars[sender].balance;
            }

            if (isNaN(amount) || amount <= 0) {
                client.say(channel, `/me @${tags.username} joaa geht nicht`);
                return;
            }

            if (userStars[sender].balance < amount) {
                client.say(channel, `/me @${tags.username} du hast nicht genug Stars haher`);
                return;
            }

            // Transfer
            userStars[sender].balance -= amount;

            if (!userStars[receiver]) {
                userStars[receiver] = { balance: 0, lastClaim: 0, level: 0, investedStars: 0, nextLevelCost: 670 };
            }
            userStars[receiver].balance += amount;

            saveStars();
            saveStars();
            client.say(channel, `/me gib @${tags.username} hat @${receiver} ${formatPoints(amount)} Star gegeben`);
        }


        if (command === 'lb' || command === 'leaderboard') {
            // Convert to array and sort by balance + investedStars
            const sortedUsers = Object.entries(userStars)
                .map(([name, data]) => ({
                    name,
                    balance: data.balance,
                    invested: data.investedStars || 0,
                    level: data.level || 0,
                    total: (data.balance || 0) + (data.investedStars || 0)
                }))
                .sort((a, b) => b.total - a.total);

            const top10 = sortedUsers.slice(0, 10);
            let msg = "Top 10 Stars: ";

            for (let i = 0; i < 10; i++) {
                const rank = i + 1;
                if (i < top10.length) {
                    const u = top10[i];
                    // Random Emote
                    let emote = "";
                    if (cachedEmotes.length > 0) {
                        emote = cachedEmotes[Math.floor(Math.random() * cachedEmotes.length)];
                    }
                    msg += `${rank}. ${u.name} ( S: ${formatPoints(u.balance)}, L: ${u.level} ${emote} ) `;
                } else {
                    msg += `${rank}. (-) `;
                }
            }
            client.say(channel, msg);
        }

        if (command === 'allstars' || command === 'listall') {
            const isMod = tags.mod || (tags.badges && tags.badges.broadcaster === '1');
            if (!isMod) {
                client.say(channel, `/me @${tags.username} du hast nicht die nötige rolle`);
                return;
            }

            const sortedUsers = Object.entries(userStars)
                .map(([name, data]) => ({
                    name,
                    balance: data.balance,
                    invested: data.investedStars || 0,
                    level: data.level || 0,
                    total: (data.balance || 0) + (data.investedStars || 0)
                }))
                .sort((a, b) => b.total - a.total);

            if (sortedUsers.length === 0) {
                client.say(channel, `/me @${tags.username} Niemand hat Stars.`);
                return;
            }

            let currentMsg = "/me ";
            sortedUsers.forEach((u, i) => {
                let emote = "";
                if (cachedEmotes.length > 0) {
                    emote = cachedEmotes[Math.floor(Math.random() * cachedEmotes.length)];
                }
                const isLast = i === sortedUsers.length - 1;
                const entry = `${i + 1}. ${u.name} (Lvl ${u.level}): ${formatPoints(u.balance)} ${emote}${isLast ? "" : " | "}`;

                if (currentMsg.length + entry.length > 400) {
                    client.say(channel, currentMsg.trim());
                    currentMsg = "/me " + entry;
                } else {
                    currentMsg += entry;
                }
            });

            if (currentMsg.trim() !== "/me") {
                client.say(channel, currentMsg.trim());
            }
        }

        if (command === 'kredit') {
            const user = tags.username.toLowerCase();

            if (!userStars[user]) {
                userStars[user] = { balance: 0, lastClaim: 0, level: 0, investedStars: 0, nextLevelCost: 670 };
            }

            if (userStars[user].loan && userStars[user].loan.active) {
                client.say(channel, `/me wideSpeedNod @${tags.username} du hast schon einen laufenden Kredit. Schulden: ${formatPoints(userStars[user].loan.debt)} Star.`);
                return;
            }


            if (userStars[user].pendingLoan) {
                client.say(channel, `/me wideSpeedNod @${tags.username} willst du den Kredit nun? Schreib 'ja' oder 'nein'.`);
                return;
            }

            // Rules
            client.say(channel, `/me wideSpeedNod @${tags.username} REGELN: Zufälliger Betrag (67-676k). Laufzeit 6h. Jede Stunde +6.7% Zinsen. Letzte Stunde +16.7%. Willst du? (Schreib 'ja')`);
            userStars[user].pendingLoan = true;
            saveStars();
        }

        if (command === 'repay' || command === 'payback') {
            const user = tags.username.toLowerCase();
            if (!userStars[user] || !userStars[user].loan || !userStars[user].loan.active) {
                client.say(channel, `/me @${tags.username} du hast keine Schulden lol`);
                return;
            }

            const debt = userStars[user].loan.debt;
            const balance = userStars[user].balance;

            if (balance < debt) {
                client.say(channel, `/me @${tags.username} du bist zu broke. Du brauchst ${formatPoints(debt)} Star , hast aber nur ${formatPoints(balance)}.`);
                client.say(channel, `/timeout @${tags.username} ${formatPoints(debt)}`);
                return;
            }


            userStars[user].balance -= debt;
            userStars[user].loan = {
                active: false,
                amount: 0,
                debt: 0,
                startTime: 0,
                hoursTracked: 0
            };
            saveStars();
            client.say(channel, `/me @${tags.username} keine schulden mehr, bist frei FREIHEIT`);
        }

        if (command === 'levelup') {
            const user = tags.username.toLowerCase();
            if (!userStars[user]) {
                userStars[user] = { balance: 0, lastClaim: 0, level: 0, investedStars: 0, nextLevelCost: 670, lastChannel: channel };
            }
            userStars[user].lastChannel = channel;

            const data = userStars[user];
            const cost = data.nextLevelCost || 670;

            if (data.balance < cost) {
                client.say(channel, `/me @${tags.username} Nerd du hast nicht genug Star für Level ${data.level + 1}. Kosten: ${formatPoints(cost)} Star (du hast ${formatPoints(data.balance)})`);
                return;
            }

            // Pay and Level Up
            data.balance -= cost;
            data.level = (data.level || 0) + 1;
            data.investedStars = (data.investedStars || 0) + cost;

            // Calculate next cost: increase by 16.7% - 26.7%
            const increaseRaw = (Math.random() * (26.7 - 16.7) + 16.7) / 100;
            const nextCost = Math.ceil(cost * (1 + increaseRaw));
            data.nextLevelCost = nextCost;

            saveStars();
            client.say(channel, `/me HeCrazy @${tags.username} JUHU Du bist jetzt Level ${data.level}! Nächstes Level kostet ${formatPoints(nextCost)} Star`);
        }

        if (command === 'tc' || command === 'topchatter') {
            let stats = [];
            const today = new Date().toLocaleString("en-US", { timeZone: "Europe/Berlin" }).split(',')[0];

            if (useMongoDB && ChatStat) {
                try {
                    const results = await ChatStat.find({ date: today }).sort({ count: -1 }).limit(10);
                    stats = results.map(r => [r.username, r.count]);
                } catch (e) {
                    console.error("Fehler beim Laden der Top-Chatter:", e);
                }
            } else {
                stats = Object.entries(messageCounts)
                    .sort((a, b) => b[1] - a[1])
                    .slice(0, 10);
            }

            if (stats.length === 0) {
                client.say(channel, `/me Heute hat noch niemand geschrieben haher`);
                return;
            }

            let msg = "Die heutigen Top chatter/in: ";
            stats.forEach(([user, count], i) => {
                let emote = "";
                if (cachedEmotes.length > 0) {
                    emote = cachedEmotes[Math.floor(Math.random() * cachedEmotes.length)];
                }
                msg += `${i + 1}. ${user}: ${count} ${emote} | `;
            });

            if (msg.endsWith(' | ')) msg = msg.slice(0, -3);
            client.say(channel, msg);
        }
    }
});



// Initial load
loadStars();
loadReminders(); // Restore timed reminders
restoreStarReminders();
refreshEmotes();
