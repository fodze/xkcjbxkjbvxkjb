require('dotenv').config();
const tmi = require('tmi.js');
const fs = require('fs');
const path = require('path');
const http = require('http'); // For Render Health Checks
const { getClientId, getTwitchUserId, getTwitchUserById, get7TVEmotes, parseHint, helixTimeout } = require('./7tv');
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
    channels: [] // Channels are now managed dynamically via channels.json
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
let channelEmotes = {};
let activeTimers = [];
let currentPrefix = '-';
let isChangingPrefix = false;
let prefixChangeUser = null;
let userStars = {};
let activeChatUsers = new Set();
let channelIds = {};
let copyTargetUser = null;
let useMongoDB = false;
let botUserId = null;

let User;
let ChatStat;
let Channel;
let Reminder;
// Connection to MongoDB
let mongoConnectedPromise = Promise.resolve();

if (process.env.MONGODB_URI) {
    try {
        const mongoose = require('mongoose');
        const userSchema = new mongoose.Schema({
            username: { type: String, required: true, unique: true },
            balance: { type: Number, default: 0 },
            lastClaim: { type: Number, default: 0 },
            reminded: { type: Boolean, default: false },
            level: { type: Number, default: 0 },
            investedStars: { type: Number, default: 0 },
            nextLevelCost: { type: Number, default: 670 },
            lastChannel: { type: String },
            loanAmount: { type: Number, default: 0 },
            loanDueDate: { type: Number, default: 0 },
            lastInterestTime: { type: Number, default: 0 },
            repaymentFailures: { type: Number, default: 0 }
        });

        User = mongoose.model('User', userSchema);

        const chatStatSchema = new mongoose.Schema({
            date: { type: String, required: true }, // Format: MM/DD/YYYY from toLocaleString
            username: { type: String, required: true },
            count: { type: Number, default: 0 }
        });
        chatStatSchema.index({ date: 1, username: 1 }, { unique: true });
        ChatStat = mongoose.model('ChatStat', chatStatSchema);

        const channelSchema = new mongoose.Schema({
            username: { type: String, required: true, unique: true },
            id: { type: String, required: true },
            joinedAt: { type: Date, default: Date.now }
        });
        Channel = mongoose.model('Channel', channelSchema);

        const reminderSchema = new mongoose.Schema({
            targetUser: { type: String, required: true },
            sourceUser: { type: String, required: true },
            message: { type: String, required: true },
            dueAt: { type: Number, required: true },

            channel: { type: String, required: true },
            createdAt: { type: Number, default: Date.now }
        });
        Reminder = mongoose.model('Reminder', reminderSchema);

        mongoConnectedPromise = mongoose.connect(process.env.MONGODB_URI)
            .then(() => {
                console.log("Verbunden mit MongoDB! (Permanente Speicherung aktiv)");
                useMongoDB = true;
                // Init Stars is called later or here? loadStars is async. 
                // loadStars() writes to userStars. 
                return loadStars();
            })
            .catch(err => {
                console.error("MongoDB Verbindungsfehler:", err);
                useMongoDB = false;
            });
    } catch (e) {
        console.error("Mongoose konnte nicht geladen werden. MongoDB deaktiviert.");
        useMongoDB = false;
    }
}
let gambleCooldowns = {};
let afkUsers = {};
let lastAfkUsers = {};
let activeBlackjackGames = {};
let activeGuessGames = {};



// Persistence
const STARS_FILE = path.join(__dirname, '..', 'stars.json');
const CHANNELS_FILE = path.join(__dirname, '..', 'channels.json');
const REMINDERS_FILE = path.join(__dirname, '..', 'reminders.json');
let monitoredChannels = [];
let localReminders = []; // For non-mongo mode
// let lastReminderId = 0;

async function initLastReminderId() {
    try {
        let maxId = 0;
        if (useMongoDB) {
            // Fetch all reminders to find max ID
            // Since shortId is a string, we need to parse.
            // But doing this in DB requires aggregation or fetching all.
            // Given "Reminders" count is likely low (<1000), fetching fields is okay?
            // Optimization: Fetch only shortId field.
            const reminders = await Reminder.find({}, 'shortId');
            if (reminders.length > 0) {
                const ids = reminders.map(r => parseInt(r.shortId)).filter(n => !isNaN(n));
                if (ids.length > 0) maxId = Math.max(...ids);
            }
        } else {
            // Local mode
            if (localReminders.length > 0) {
                const ids = localReminders.map(r => parseInt(r.shortId)).filter(n => !isNaN(n));
                if (ids.length > 0) maxId = Math.max(...ids);
            }
        }
        lastReminderId = maxId;
        console.log(`Initialisierte Reminder-ID Startwert: ${lastReminderId}`);
    } catch (e) {
        console.error("Fehler beim Initialisieren der Reminder-ID:", e);
        lastReminderId = 0;
    }
}

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
                    level: u.level || 0,
                    investedStars: u.investedStars || 0,
                    nextLevelCost: u.nextLevelCost || 670,
                    lastChannel: u.lastChannel,
                    loanAmount: u.loanAmount || 0,
                    loanDueDate: u.loanDueDate || 0,
                    lastInterestTime: u.lastInterestTime || 0,
                    repaymentFailures: u.repaymentFailures || 0
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

async function loadReminders() {
    if (useMongoDB) {
        // No local cache needed, we query DB directly
    } else {
        try {
            if (fs.existsSync(REMINDERS_FILE)) {
                const data = fs.readFileSync(REMINDERS_FILE, 'utf8');
                localReminders = JSON.parse(data);
                console.log(`Reminders geladen: ${localReminders.length}`);
            }
        } catch (e) {
            console.error("Fehler beim Laden der Reminders:", e);
        }
    }
}

async function saveLocalReminders() {
    if (!useMongoDB) {
        try {
            fs.writeFileSync(REMINDERS_FILE, JSON.stringify(localReminders, null, 2));
        } catch (e) {
            console.error("Fehler beim Speichern der Reminders:", e);
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
    if (hours === minutes) {
        const pad = n => n < 10 ? '0' + n : n;
        const timeString = `${pad(hours)}:${pad(minutes)}`;

        if (lastSchnapszahl !== timeString) {
            // Use active channels from the client
            const channels = client.getChannels();
            console.log(`Zeit-Check: ${timeString}. Sende an ${channels.length} Kanäle: ${channels.join(', ')}`);

            channels.forEach(ch => {
                client.say(ch, `wowii ${timeString}`)
                    .catch(err => console.error(`Fehler beim Senden von Schnapszahl an ${ch}:`, err));
            });
            lastSchnapszahl = timeString;
        }
    }
}



// Check time every 5 seconds
setInterval(checkSchnapszahl, 5000);

// --- Reminder System ---

// Helper: Get Berlin Offset (UTC+1 or UTC+2)
function getBerlinOffset(date) {
    const year = date.getUTCFullYear();

    // DST Starts: Last Sunday in March at 01:00 UTC (02:00 CET -> 03:00 CEST)
    const march31 = new Date(Date.UTC(year, 2, 31));
    const dayMarch = march31.getUTCDay(); // 0=Sunday
    const lastSundayMarch = 31 - dayMarch;
    const startSummer = new Date(Date.UTC(year, 2, lastSundayMarch, 1));

    // DST Ends: Last Sunday in October at 01:00 UTC (03:00 CEST -> 02:00 CET)
    const oct31 = new Date(Date.UTC(year, 9, 31));
    const dayOct = oct31.getUTCDay();
    const lastSundayOct = 31 - dayOct;
    const endSummer = new Date(Date.UTC(year, 9, lastSundayOct, 1));

    if (date >= startSummer && date < endSummer) {
        return 2 * 60 * 60 * 1000;
    }
    return 1 * 60 * 60 * 1000;
}

// Helper: Parse Duration / Date
function parseTimeInput(args) {
    let durationMs = 0;
    let absoluteTime = null; // Will store UTC date representing Berlin Face Time
    let index = 0;

    // "Now" in Berlin Time (Face Value)
    const now = new Date();
    const offsetNow = getBerlinOffset(now);
    const nowBerlin = new Date(now.getTime() + offsetNow);

    // Regex Definitions
    const regexDurationCombined = /^(\d+)(y|m|w|d|h|min|s)$/i;
    const regexValue = /^(\d+)$/;
    const regexUnit = /^(y|m|w|d|h|min|s)$/i;
    const regexTime = /^(\d{1,2}):(\d{2})$/;
    const regexDate = /^(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?$/;

    // Iterate args to consume time parts
    for (; index < args.length; index++) {
        const token = args[index].toLowerCase();

        // 1. Combined Duration (e.g. 10m)
        const matchDur = token.match(regexDurationCombined);
        if (matchDur) {
            const val = parseInt(matchDur[1]);
            const unit = matchDur[2];
            durationMs += getUnitMs(val, unit);
            continue;
        }

        // 2. Split Duration (e.g. 10 m)
        if (regexValue.test(token) && args[index + 1] && regexUnit.test(args[index + 1])) {
            const val = parseInt(token);
            const unit = args[index + 1].toLowerCase();
            durationMs += getUnitMs(val, unit);
            index++; // Skip unit
            continue;
        }

        // 3. Time (e.g. 11:40)
        const matchTime = token.match(regexTime);
        if (matchTime) {
            const h = parseInt(matchTime[1]);
            const m = parseInt(matchTime[2]);
            if (!absoluteTime) absoluteTime = new Date(nowBerlin); // Copy Berlin Face Time
            absoluteTime.setUTCHours(h, m, 0, 0); // Set Berlin Face Time

            // Logic handled later
            continue;
        }

        // 4. Date (e.g. 14.02 or 14.02.2024)
        const matchDate = token.match(regexDate);
        if (matchDate) {
            const day = parseInt(matchDate[1]);
            const month = parseInt(matchDate[2]) - 1; // 0-indexed
            const yearStr = matchDate[3];
            let year = nowBerlin.getUTCFullYear();
            if (yearStr) {
                if (yearStr.length === 2) year = 2000 + parseInt(yearStr);
                else year = parseInt(yearStr);
            }

            if (!absoluteTime) absoluteTime = new Date(nowBerlin);
            absoluteTime.setUTCDate(day);
            absoluteTime.setUTCMonth(month);
            absoluteTime.setUTCFullYear(year);
            continue;
        }

        // 4.1 "uhr" Combined (e.g. 9uhr)
        const matchUhr = token.match(/^(\d{1,2})uhr$/i);
        if (matchUhr) {
            const h = parseInt(matchUhr[1]);
            if (!absoluteTime) absoluteTime = new Date(nowBerlin);
            absoluteTime.setUTCHours(h, 0, 0, 0);
            continue;
        }

        // 4.2 "uhr" Split (e.g. 9 uhr)
        if (/^\d{1,2}$/.test(token) && args[index + 1] && /^uhr$/i.test(args[index + 1])) {
            const h = parseInt(token);
            if (!absoluteTime) absoluteTime = new Date(nowBerlin);
            absoluteTime.setUTCHours(h, 0, 0, 0);
            index++;
            continue;
        }

        // 5. Fillers
        if (['am', 'um', 'uhr', 'in'].includes(token)) continue;

        // End of time part
        break;
    }

    // Logic Resolution
    let finalDueTime = 0;

    if (absoluteTime) {
        // absoluteTime holds the "Berlin Face Time" in UTC slots.
        // Compare with nowBerlin (which also holds Berlin Face Time in UTC slots via getBerlinOffset addition trick, wait no)
        // Let's align nowBerlin to be comparable:
        // nowBerlin = now + offset. So .getUTCHours() returns Berlin hours. 
        // Yes.

        if (absoluteTime.getTime() < nowBerlin.getTime()) {
            // Past time logic
            // Check if same day first (ignoring time components for date compare)
            const absDay = new Date(absoluteTime).setUTCHours(0, 0, 0, 0);
            const nowDay = new Date(nowBerlin).setUTCHours(0, 0, 0, 0);

            if (absDay === nowDay) {
                // Passed time today -> Tomorrow
                absoluteTime.setUTCDate(absoluteTime.getUTCDate() + 1);
            } else if (absoluteTime < nowBerlin) {
                // Past date -> Next Year (only if date wasn't explicit? assumed implicit)
                // But we don't know if it was explicit.
                // safe bet: next year if date is in past.
                absoluteTime.setUTCFullYear(absoluteTime.getUTCFullYear() + 1);
            }
        }

        // Convert Berlin Face Time back to Real UTC Timestamp
        // RealUTC = BerlinFaceTime - Offset
        // We need the offset for the TARGET date, not necessarily Now.
        const targetOffset = getBerlinOffset(absoluteTime);
        finalDueTime = absoluteTime.getTime() - targetOffset;

    } else if (durationMs > 0) {
        finalDueTime = now.getTime() + durationMs;
    } else {
        return null; // No valid time found
    }

    // Capture message
    const msg = args.slice(index).join(' ');
    // If msg empty, maybe reminder is just "Pong"?

    return { dueAt: finalDueTime, message: msg };
}

function getUnitMs(val, unit) {
    const s = 1000;
    const m = 60 * s;
    const h = 60 * m;
    const d = 24 * h;
    const w = 7 * d;

    switch (unit) {
        case 's': return val * s;
        case 'min': return val * m;
        case 'h': return val * h;
        case 'd': return val * d;
        case 'w': return val * w;
        case 'mo': return val * d * 30; // Month
        case 'm': return val * m; // Minute (User example: 10m wäsche)
        case 'y': return val * d * 365; // Year
        default: return 0;
    }
}

async function checkReminders() {
    const now = Date.now();
    let due = [];

    if (useMongoDB) {
        try {
            // Find and DELETE due reminders atomically-ish (find then delete)
            // Or find, trigger, then delete.
            const reminders = await Reminder.find({ dueAt: { $lte: now, $gt: 0 } });
            if (reminders.length > 0) {
                due = reminders;
                const ids = reminders.map(r => r._id);
                await Reminder.deleteMany({ _id: { $in: ids } });
            }
        } catch (e) {
            console.error("Fehler beim Prüfen der Reminders (Mongo):", e);
        }
    } else {
        // Local Filter
        due = localReminders.filter(r => r.dueAt > 0 && r.dueAt <= now);
        if (due.length > 0) {
            localReminders = localReminders.filter(r => r.dueAt === 0 || r.dueAt > now);
            saveLocalReminders();
        }
    }

    if (due.length > 0) {
        due.forEach(r => {
            // Send
            client.say(r.channel, `/me @${r.targetUser} bingi reminder von @${r.sourceUser} " ${r.message} "`);
        });
    }
}

// Check reminders every 10 seconds
setInterval(checkReminders, 10000);


function checkLoans() {
    const now = Date.now();
    for (const user in userStars) {
        const data = userStars[user];
        if (data.loanAmount > 0) {
            // 1. Hourly Interest Logic
            // If loan is active, check if 1 hour has passed since last interest application
            // We use a small buffer or just strict check. 
            // Initial lastInterestTime is set when loan is taken.
            if (data.lastInterestTime && (now - data.lastInterestTime) >= 3600000) { // 3600000 = 1 Hour
                const interest = Math.ceil(data.loanAmount * 0.10); // 10% Interest
                data.loanAmount += interest;
                data.lastInterestTime = now; // Reset timer for next hour

                // Optional: Notify user about interest increase?
                // client.say(process.env.TWITCH_CHANNEL, `/me @${user} Dein Kredit ist um 10% gestiegen! Neuer Betrag: ${formatPoints(data.loanAmount)} Star`);
                saveStars(user);
            }

            // 2. Deadline Logic (6 Hours)
            if (data.loanDueDate > 0 && now >= data.loanDueDate) {
                // Attempt Auto-Repay
                const canPay = Math.min(data.balance, data.loanAmount);
                if (canPay > 0) {
                    data.balance -= canPay;
                    data.loanAmount -= canPay;
                }

                if (data.loanAmount <= 0) {
                    // Fully Paid
                    data.loanAmount = 0;
                    data.loanDueDate = 0;
                    data.repaymentFailures = 0;
                    // client.say(process.env.TWITCH_CHANNEL, `/me @${user} Kredit automatisch zurückgezahlt.`);
                } else {
                    // Failed to pay after 6 hours
                    const timeoutDuration = Math.ceil(data.loanAmount); // Seconds equal to debt

                    let targetChannel = data.lastChannel;
                    if (!targetChannel) targetChannel = process.env.TWITCH_CHANNEL.split(',')[0].trim();
                    if (targetChannel && !targetChannel.startsWith('#')) targetChannel = '#' + targetChannel;

                    if (client.readyState() === 'OPEN') {
                        client.say(targetChannel, `/timeout @${user} ${timeoutDuration} opfer mit kredit`);
                        client.say(targetChannel, `/me @${user} die 6 Stunden sind um! Kredit nicht bezahlt -> ${timeoutDuration}s Timeout. Schulden beglichen.`);
                    }

                    // Reset Debt (Time Served)
                    data.loanAmount = 0;
                    data.loanDueDate = 0;
                    data.repaymentFailures = 0;
                }
                saveStars(user);
            }
        }
    }
}

// Check loans every 1 minute
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


/**
 * Central Timeout Helper using Helix if possible
 */


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
            // Check if running on Render (Production)
            if (process.env.RENDER) {
                client.say(targetChannel, `/me @${username} bingi hol deine Star ab mit ${currentPrefix}star`);
            } else {
                console.log(`[LOCAL] Would send reminder to ${targetChannel}: @${username} bingi hol deine Star ab...`);
            }

            if (userStars[username]) {
                userStars[username].reminded = true;
                saveStars(username);
            }
        }
    }, delay);
}

function restoreStarReminders() {
    // Only restore reminders if running on the server to avoid local spam at startup
    if (!process.env.RENDER) {
        console.log("Local Mode: Skipping automated star reminder restoration.");
        return;
    }

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
        // Use monitoredChannels instead of env
        let channels = monitoredChannels.length > 0 ? monitoredChannels : process.env.TWITCH_CHANNEL.split(',').map(c => c.trim());
        const token = process.env.TWITCH_OAUTH_TOKEN;

        let clientId;
        try {
            clientId = await getClientId(token);
        } catch (e) {
            console.error("Auth Error: Token ungültig? Bitte .env prüfen.");
            return;
        }

        channelEmotes = {}; // Reset

        for (const rawName of channels) {
            // Normalize: Ensure we have clean name for API, and #name for Map key
            const cleanName = rawName.replace(/^#/, '');
            const mapKey = `#${cleanName.toLowerCase()}`;

            console.log(`Lade 7TV Emotes für: ${cleanName}...`);
            const userId = await getTwitchUserId(cleanName, clientId, token);

            if (userId) {
                channelIds[cleanName.toLowerCase()] = userId;
                const emotes = await get7TVEmotes(userId);
                channelEmotes[mapKey] = emotes.map(e => e.name);
                console.log(`  > ${emotes.length} Emotes für ${cleanName} geladen.`);
            } else {
                console.error(`Konnte Twitch User ID für ${cleanName} nicht finden.`);
            }
        }

        // Get Bot's own User ID for Helix Moderation
        if (!botUserId) {
            botUserId = await getTwitchUserId(process.env.TWITCH_USERNAME, clientId, token);
        }

        console.log(`Erfolg! Emotes für ${Object.keys(channelEmotes).length} Kanäle geladen.`);
    } catch (e) {
        console.error('Fehler beim Laden der Emotes:', e);
    }
}





async function initializeChannels() {
    try {
        const token = process.env.TWITCH_OAUTH_TOKEN;
        const clientId = await getClientId(token);

        let channelsConfig = [];

        // 1. Load Channels (Support MongoDB Persistence)
        if (useMongoDB) {
            try {
                let dbChannels = await Channel.find({});

                // Sync: Check channels.json and add missing channels into MongoDB
                if (fs.existsSync(CHANNELS_FILE)) {
                    console.log("Synchronisiere channels.json mit MongoDB...");
                    const fileData = fs.readFileSync(CHANNELS_FILE, 'utf8');
                    const fileChannels = JSON.parse(fileData);
                    let addedCount = 0;

                    for (const fc of fileChannels) {
                        try {
                            const exists = dbChannels.find(d => d.username.toLowerCase() === fc.username.toLowerCase());
                            if (!exists) {
                                await Channel.create({
                                    username: fc.username,
                                    id: fc.id || "0" // Placeholder, will be fetched in validation loop
                                });
                                console.log(`Neu hinzugefügt: ${fc.username}`);
                                addedCount++;
                            }
                        } catch (err) {
                            console.error(`Fehler bei Sync von ${fc.username}:`, err);
                        }
                    }

                    if (addedCount > 0) {
                        // Reload DB
                        dbChannels = await Channel.find({});
                    }
                }

                channelsConfig = dbChannels.map(d => ({ username: d.username, id: d.id }));
            } catch (err) {
                console.error("Fehler beim Laden der Channels aus DB:", err);
            }
        }

        // Fallback or Local Mode: Load from JSON if MongoDB failed or not used, AND list is empty
        if ((!useMongoDB || channelsConfig.length === 0) && fs.existsSync(CHANNELS_FILE)) {
            try {
                const data = fs.readFileSync(CHANNELS_FILE, 'utf8');
                channelsConfig = JSON.parse(data);
            } catch (e) {
                console.error("Fehler beim Lesen von channels.json:", e);
            }
        }

        // 2. Validate and Update IDs/Usernames
        let configChanged = false;
        monitoredChannels = [];

        for (let i = 0; i < channelsConfig.length; i++) {
            let ch = channelsConfig[i];
            let userData = null;

            // If ID missing, fetch it
            if (!ch.id || ch.id === "0") {
                console.log(`Channel ${ch.username} hat keine ID. Suche...`);
                try {
                    const userId = await getTwitchUserId(ch.username, clientId, token);
                    if (userId) {
                        ch.id = userId;
                        configChanged = true;
                        channelIds[ch.username.toLowerCase()] = userId; // FORCE POPULATE
                        console.log(`ID für ${ch.username} gefunden: ${userId}`);

                        // Update in DB immediately if using Mongo
                        if (useMongoDB) {
                            await Channel.findOneAndUpdate({ username: ch.username }, { id: userId }, { upsert: true });
                        }
                    } else {
                        console.error(`Konnte ID für ${ch.username} nicht finden.`);
                        continue;
                    }
                } catch (e) {
                    console.error(`Fehler beim ID-Check für ${ch.username}:`, e);
                }
            }

            // Check for Rename
            if (ch.id) {
                // POPULATE CACHE
                channelIds[ch.username.toLowerCase()] = ch.id;
                try {
                    userData = await getTwitchUserById(ch.id, clientId, token);
                    if (userData) {
                        if (userData.login !== ch.username) {
                            console.log(`RENAME DETECTED: ${ch.username} -> ${userData.login}`);
                            // Remove old name from DB if needed, basically update the document match by ID
                            const oldName = ch.username;
                            ch.username = userData.login;
                            configChanged = true;

                            if (useMongoDB) {
                                // Update username where id matches
                                await Channel.findOneAndUpdate({ id: ch.id }, { username: ch.username });
                            }
                        }
                        monitoredChannels.push(ch.username);
                    } else {
                        console.warn(`User mit ID ${ch.id} nicht mehr gefunden.`);
                        // Optional: Remove from DB?
                    }
                } catch (e) {
                    console.error(`Fehler beim Rename-Check für ID ${ch.id}:`, e);
                    monitoredChannels.push(ch.username);
                }
            }
        }

        // 3. Save updates to JSON (As backup or primary if not Mongo)
        if (configChanged) {
            fs.writeFileSync(CHANNELS_FILE, JSON.stringify(channelsConfig, null, 2));
            console.log("channels.json wurde aktualisiert.");
        }

        // 4. Join Channels
        if (monitoredChannels.length > 0) {
            console.log(`Joine Channels: ${monitoredChannels.join(', ')}`);
            for (const ch of monitoredChannels) {
                await client.join(ch).catch(e => console.error(`Konnte ${ch} nicht joinen:`, e));
            }
        }

    } catch (e) {
        console.error("Fehler bei initializeChannels:", e);
    }
}

mongoConnectedPromise.then(() => {
    client.connect()
        .then(async () => {
            await initializeChannels();
            await refreshEmotes();
        })
        .catch(console.error);
});

client.on('message', async (channel, tags, message, self) => {
    // Ignore echoed messages.
    if (self) return;

    const sender = tags.username.toLowerCase();

    // --- Copy User Logic ---
    if (copyTargetUser && sender === copyTargetUser) {
        // Avoid infinite loops if the user sends the prefix
        if (!message.startsWith(currentPrefix)) {
            client.say(channel, message);
        }
    }

    activeChatUsers.add(tags.username);

    // --- Check for On-Message Reminders (Zero Time) ---
    if (useMongoDB) {
        try {
            const pendingReminders = await Reminder.find({ targetUser: { $regex: new RegExp('^' + sender + '$', 'i') }, dueAt: 0 });
            if (pendingReminders.length > 0) {
                pendingReminders.forEach(r => {
                    client.say(channel, `/me @${tags.username} bingi reminder von @${r.sourceUser}: ${r.message}`);
                });
                await Reminder.deleteMany({ targetUser: { $regex: new RegExp('^' + sender + '$', 'i') }, dueAt: 0 });
            }
        } catch (e) {
            console.error("Fehler beim Checken der On-Message Reminders:", e);
        }
    } else {
        const toSend = localReminders.filter(r => r.dueAt === 0 && r.targetUser.toLowerCase() === sender);
        if (toSend.length > 0) {
            toSend.forEach(r => {
                client.say(r.channel, `/me @${r.targetUser} bingi reminder von @${r.sourceUser}: ${r.message}`);
            });
            localReminders = localReminders.filter(r => !(r.dueAt === 0 && r.targetUser.toLowerCase() === sender));
            saveLocalReminders();
        }
    }

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
    // channelEmotes is keyed by lowercase #channelname usually? verify keys
    // refreshEmotes uses: "#" + cleanName.toLowerCase()

    const currentEmotes = channelEmotes[channel] || [];
    if (currentEmotes.length > 0) {
        emote = currentEmotes[Math.floor(Math.random() * currentEmotes.length)];
    }

    // --- Active Guess Game Logic (Check for answer without prefix) ---
    if (activeGuessGames[sender]) {
        const msgLower = message.trim().toLowerCase();
        if (['ungerade', 'odd', 'gerade', 'even'].includes(msgLower)) {
            const game = activeGuessGames[sender];
            const choice = msgLower;

            let betsOnOdd = false;
            if (['ungerade', 'odd'].includes(choice)) {
                betsOnOdd = true;
            }

            const isOdd = game.number % 2 !== 0; // 0 is even
            const win = (betsOnOdd && isOdd) || (!betsOnOdd && !isOdd);

            if (win) {
                const winAmount = game.bet * 2;
                userStars[sender].balance += winAmount;
                saveStars();
                client.say(channel, `/me ${emote} @${tags.username} Zahl war ${game.number} - Gewonnen JUHU +${formatPoints(game.bet)} Star Balance: ${formatPoints(userStars[sender].balance)} Star`);
            } else {
                saveStars();
                client.say(channel, `/me ${emote} @${tags.username} Zahl war ${game.number} - Verloren ohno -${formatPoints(game.bet)} Star Balance: ${formatPoints(userStars[sender].balance)} Star`);
            }

            delete activeGuessGames[sender];
            return; // Stop processing this message
        }
    }



    // --- AFK Check ---
    if (afkUsers[sender]) {
        if (!message.startsWith(currentPrefix + 'afk') && !message.startsWith(currentPrefix + 'rafk')) { // Don't trigger on AFK or RAFK
            const data = afkUsers[sender];
            const startTime = typeof data === 'object' ? data.startTime : data;
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
            lastAfkUsers[sender] = {
                startTime: startTime,
                reason: typeof data === 'object' ? data.reason : "",
                returnTime: Date.now()
            };
            delete afkUsers[sender];
        }
    }



    // --- Emote Combo Logic ---
    const msgContent = message.trim();
    if (currentEmotes.includes(msgContent)) {
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
            "was für opfer digga ich sag dir ganz ehrlich digga",
            "das game an sich fortnite ist so geil digga aber einfach nur diese kleinen kinder digga",
            "haben dieses game so kaputt gemacht digga neue map hier digga neue map da digga",
            "sie wünschen sich alles digga und wenn ich mal 1v1 gegen die mache digga und die verkacken, die beleidigen, die beleidigen mich direkt als hs digga",
            "oder generell sie swipen durch ihre tiktok digga fy und schreiben unter jedes video hs digga daraus besteht fortnite digga",
            "digga sonst an sich fortnite ist so ein geiles prinzip digga",
            "früher du hast gezockt es war alles wild digga keiner konnte was digga",
            "früher du hast fun an fortnite gehabt digga und jetzt einfach bruder jetzt besteht dieses game aus irgendwelchen kindern digga die nur beleidigen weil sie verkacken digga das ist einfach fortnite digga ganz ehrlich an sich ich sag dir ganz ehrlich digga fortnite ist so ein wildes game bruder ich sag dir ganz ehrlich digga"
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
    // --- Pyramids Script ---
    const msgLower = message.trim().toLowerCase();

    // Defined Triggers (Used for both Generic and oioioi variants)
    const pyramidTriggers = [
        'affe', 'cassy', 'jean', 'timo', 'jona', 'janne', 'julia',
        'knopers', 'ikki', 'kevin', 'sid', 'jasmin', 'sophia', 'noah',
        'wydios', 'kerze', 'NotedBot', 'ente', 'noel', 'antonia'
    ];

    // 1. Special "oioioi baka" Pyramid (Trigger: "baka")
    // Keeps legacy behavior where just "baka" triggers the oioioi pyramid
    if (msgLower === 'baka' && !self) {
        const dialog = [];
        const maxLevel = 10;

        // Build up
        for (let i = 1; i <= maxLevel; i++) {
            dialog.push(`/me ${Array(i).fill('oioioi').join(' ')} baka`);
        }
        // Build down
        for (let i = maxLevel - 1; i >= 1; i--) {
            dialog.push(`/me ${Array(i).fill('oioioi').join(' ')} baka`);
        }

        for (let i = 0; i < dialog.length; i++) {
            const id = setTimeout(() => {
                client.say(channel, dialog[i]);
            }, i * 200);
            activeTimers.push(id);
        }
        return;
    }

    // 2. "oioioi <text>" Pyramid (Trigger: "oioioi <anything>")
    if (msgLower.startsWith('oioioi ') && !self) {
        // Extract everything after "oioioi " from the original message to keep casing
        // We assume the prefix length is roughly 7 chars (oioioi + space)
        // Note: usage of substring based on index of first space might be safer if casing varies heavily for the prefix
        const content = message.slice(7).trim();

        if (content.length > 0) {
            const dialog = [];
            const maxLevel = 10;

            // Build up
            for (let i = 1; i <= maxLevel; i++) {
                dialog.push(`/me ${Array(i).fill('oioioi').join(' ')} ${content}`);
            }
            // Build down
            for (let i = maxLevel - 1; i >= 1; i--) {
                dialog.push(`/me ${Array(i).fill('oioioi').join(' ')} ${content}`);
            }

            for (let i = 0; i < dialog.length; i++) {
                const id = setTimeout(() => {
                    client.say(channel, dialog[i]);
                }, i * 200);
                activeTimers.push(id);
            }
            return;
        }
    }

    // 3. Generic Name/Emote Pyramid (Trigger: "<name>")
    const genericTrigger = pyramidTriggers.find(t => t.toLowerCase() === msgLower);

    if (genericTrigger && !self) {
        const dialog = [];
        const maxLevel = 10;

        // Build up
        for (let i = 1; i <= maxLevel; i++) {
            dialog.push(`/me ${Array(i).fill(triggerWord).join(' ')}`);
        }
        // Build down
        for (let i = maxLevel - 1; i >= 1; i--) {
            dialog.push(`/me ${Array(i).fill(triggerWord).join(' ')}`);
        }

        for (let i = 0; i < dialog.length; i++) {
            const id = setTimeout(() => {
                client.say(channel, dialog[i]);
            }, i * 200); // 200ms delay
            activeTimers.push(id);
        }
        return;
    }

    if (message.startsWith(currentPrefix)) {
        const args = message.slice(currentPrefix.length).split(' ');
        const command = args.shift().toLowerCase(); // Remove prefix and get command



        if (command === 'commands' || command === 'befehle') {
            const commandGroups = [
                ['ping'],
                ['prefix'],
                ['frage'],
                ['stop'],
                ['afk'],
                ['spam'],
                ['star'],
                ['hug'],
                ['suche', 'guess'],
                ['gamba'],
                ['bj', 'blackjack'],
                ['hit', 'h'],
                ['stand', 's'],
                ['balance', 'stars'],
                ['give', 'pay'],
                ['lb', 'leaderboard'],
                ['allstars', 'listall'],
                ['tc', 'topchatter'],
                ['levelup'],
                ['kok', 'pussy'],
                ['zahl'],
                ['levelup'],
                ['kok', 'pussy'],
                ['zahl'],
                ['kredit', 'repay', 'payback'],
                ['remindme', 'remind', 'reminders'],
                ['commands', 'befehle']
            ];

            let header = "Nerd commands: ";
            let currentMsg = header;

            for (let i = 0; i < commandGroups.length; i++) {
                let emote = "";
                const currentEmotes = channelEmotes[channel] || [];
                if (currentEmotes.length > 0) {
                    emote = currentEmotes[Math.floor(Math.random() * currentEmotes.length)];
                }

                const prefix = emote ? emote + " " : "- ";

                // Format the group: !cmd1, !cmd2, !cmd3
                const group = commandGroups[i];
                let groupStr = group.map(c => `${currentPrefix}${c}`).join(', ');

                const cmdEntry = `${prefix}${groupStr} `;

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

        if (command === 'v') {
            const senderId = tags['user-id'];
            const channelName = channel.replace('#', '').toLowerCase();
            const broadcasterId = channelIds[channelName];

            // Ensure we have necessary IDs
            if (broadcasterId && botUserId && senderId) {
                try {
                    const token = process.env.TWITCH_OAUTH_TOKEN;
                    const clientId = await getClientId(token);
                    // Timeout for 1 second
                    await helixTimeout(broadcasterId, botUserId, senderId, 1, "Timeout by -v command", clientId, token);
                    console.log(`User ${tags.username} timed out for 1s in ${channel}`);
                } catch (e) {
                    console.error(`Failed to timeout user ${tags.username}:`, e);
                }
            } else {
                console.warn("Cannot timeout: Missing IDs.", { broadcasterId, botUserId, senderId });
            }
        }




        if (command === 'pong') {
            client.say(channel, 'animeGirlPunchU bin da');
        }

        if (command === 'prefix') {
            isChangingPrefix = true;
            prefixChangeUser = tags.username;
            client.say(channel, `Nerd was willst du als prefix? aktuell hast du: ${currentPrefix}`);
        }


        if (command === 'frage') {
            const question = args.join(' ').toLowerCase();
            const restrictedKeywords = ['tod', 'sterben', 'umbringen', 'selbstmord', 'doid', 'jenseits', 'beenden', 'ppDone', 'erhängen', 'erhänhen', 'existieren', 'leben'];
            if (restrictedKeywords.some(w => question.includes(w))) {
                const restrictedAnswers = [
                    "/me stare", "/me nein stare", "/me nein", "/me nein sideeye", "/me sideeye", "/me stop", "/me nein stop"
                ];
                const response = restrictedAnswers[Math.floor(Math.random() * restrictedAnswers.length)];
                client.say(channel, response);
            } else {
                const answers = [
                    "/me Genau ja", "/me nope nein", "/me eeh vielleicht", "/me Skip frag später nochmal",
                    "/me Genau auf jeden fall", "/me nope niemals", "/me eeh wahrscheinlich schon",
                    "/me manidk ich glaube nicht", "/me manik definitiv", "/me haher träum weiter"
                ];
                const randomAnswer = answers[Math.floor(Math.random() * answers.length)];
                client.say(channel, `${randomAnswer}`);
            }
        }



        if (command === 'stop') {
            clearAllTimers();
            copyTargetUser = null;
            client.say(channel, "bob bin schon leise");
        }

        if (command === 'copy') {
            const isMod = tags.mod || (tags.badges && tags.badges.broadcaster);
            if (!isMod) return;

            if (!args[0]) {
                client.say(channel, "ome5");
                return;
            }
            const target = args[0].toLowerCase().replace('@', '');
            copyTargetUser = target;
            client.say(channel, `/me ome5`);
        }

        if (command === 'afk') {
            const reason = args.join(' ');
            afkUsers[sender] = { startTime: Date.now(), reason: reason };
            let msg = `/me ${emote} @${tags.username} ist jetzt AFK bye`;
            if (reason) msg += ` | ${reason}`;
            client.say(channel, msg);
        }

        if (command === 'rafk') {
            if (afkUsers[sender]) {
                client.say(channel, `/me @${tags.username} du bist doch schon AFK bob`);
                return;
            }
            if (lastAfkUsers[sender]) {
                const now = Date.now();
                const returnTime = lastAfkUsers[sender].returnTime || 0;

                // 5 Minute Window (5 * 60 * 1000 ms)
                if (now - returnTime > 300000) {
                    client.say(channel, `/me @${tags.username} Nerd die 5 Minuten sind um, du musst dich neu AFK stellen haher`);
                    delete lastAfkUsers[sender];
                    return;
                }

                afkUsers[sender] = {
                    startTime: lastAfkUsers[sender].startTime,
                    reason: lastAfkUsers[sender].reason
                };
                const reason = afkUsers[sender].reason;
                let msg = `/me ${emote} @${tags.username} ist wieder AFK bye`;
                if (reason) msg += ` | ${reason}`;
                client.say(channel, msg);
            } else {
                client.say(channel, `/me @${tags.username} Nerd du warst vorher nicht AFK`);
            }
        }

        if (command === 'join') {
            const isMod = tags.mod || (tags.badges && tags.badges.broadcaster);
            if (!isMod) return;

            // Allow specifying a channel, otherwise join the user's channel
            let target = args[0] ? args[0].toLowerCase() : tags.username.toLowerCase();
            if (target.startsWith('#')) target = target.slice(1);

            try {
                // 1. Fetch Key Info
                const token = process.env.TWITCH_OAUTH_TOKEN;
                const clientId = await getClientId(token);
                // We need ID for DB
                const userId = await getTwitchUserId(target, clientId, token);

                if (!userId) {
                    client.say(channel, `/me Konnte ID für ${target} nicht finden.`);
                    return;
                }

                // 2. Perform Join first to ensure it works
                await client.join(target);
                client.say(channel, `/me Joined ${target}`);

                // 3. PERSISTENCE
                // Add to monitoredChannels list
                if (!monitoredChannels.includes(target)) {
                    monitoredChannels.push(target);
                }

                // Save to MongoDB
                if (useMongoDB) {
                    await Channel.findOneAndUpdate(
                        { username: target },
                        { id: userId, joinedAt: new Date() },
                        { upsert: true, new: true }
                    );
                    console.log(`[DB] Channel ${target} gespeichert.`);
                }

                // Save to JSON (Backup)
                let storedChannels = [];
                try {
                    if (fs.existsSync(CHANNELS_FILE)) {
                        storedChannels = JSON.parse(fs.readFileSync(CHANNELS_FILE, 'utf8'));
                    }
                } catch (e) { }

                // Add if not exists
                if (!storedChannels.find(c => c.username === target)) {
                    storedChannels.push({ username: target, id: userId });
                    fs.writeFileSync(CHANNELS_FILE, JSON.stringify(storedChannels, null, 2));
                }

                // Refresh Emotes for new channel
                await refreshEmotes();

            } catch (e) {
                console.error("Join Error:", e);
                client.say(channel, `/me Fehler beim Joinen: ${e.message}`);
            }
        }

        if (command === 'part' || command === 'leave') {
            const isMod = tags.mod || (tags.badges && tags.badges.broadcaster);
            if (!isMod) return;

            let target = args[0] ? args[0].toLowerCase() : channel.replace('#', '').toLowerCase();
            if (target.startsWith('#')) target = target.slice(1);

            try {
                // 1. Leave
                await client.part(target);
                if (channel.replace('#', '').toLowerCase() !== target) {
                    client.say(channel, `/me Left ${target}`);
                } else {
                    console.log(`Left channel ${target}`);
                }

                // 2. PERSISTENCE REMOVAL
                // Remove from memory
                monitoredChannels = monitoredChannels.filter(c => c !== target);

                // Remove from MongoDB
                if (useMongoDB) {
                    await Channel.deleteOne({ username: target });
                    console.log(`[DB] Channel ${target} entfernt.`);
                }

                // Remove from JSON
                if (fs.existsSync(CHANNELS_FILE)) {
                    try {
                        let storedChannels = JSON.parse(fs.readFileSync(CHANNELS_FILE, 'utf8'));
                        const initLen = storedChannels.length;
                        storedChannels = storedChannels.filter(c => c.username !== target);
                        if (storedChannels.length !== initLen) {
                            fs.writeFileSync(CHANNELS_FILE, JSON.stringify(storedChannels, null, 2));
                        }
                    } catch (e) {
                        console.error("Fehler beim Update von channels.json:", e);
                    }
                }

            } catch (e) {
                console.error("Part Error:", e);
                client.say(channel, `/me Fehler beim Leaven: ${e.message}`);
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
                }, i * 10);
                activeTimers.push(id);
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

        if (command === 'kredit' || command === 'loan') {
            const user = tags.username.toLowerCase();
            // Random amount between 67 and 676,767,676,767
            const minCredit = 67;
            const maxCredit = 676767676767;
            const amount = Math.floor(Math.random() * (maxCredit - minCredit + 1)) + minCredit;

            if (!userStars[user]) {
                userStars[user] = { balance: 0, lastClaim: 0, loanAmount: 0, loanDueDate: 0, repaymentFailures: 0 };
            }

            if (userStars[user].loanAmount > 0) {
                client.say(channel, `/me @${tags.username} Du hast noch einen offenen Kredit von ${formatPoints(userStars[user].loanAmount)} Star`);
                return;
            }

            // Grant Loan
            const duration = 6 * 60 * 60 * 1000; // 6 Hours

            userStars[user].balance += amount;
            userStars[user].loanAmount = amount; // Start with principal. Interest added hourly.
            userStars[user].loanDueDate = Date.now() + duration;
            userStars[user].lastInterestTime = Date.now();
            userStars[user].repaymentFailures = 0;
            userStars[user].lastChannel = channel;

            saveStars(user);
            client.say(channel, `/me @${tags.username} Kredit von ${formatPoints(amount)} Star gewährt! Rückzahlung innerhalb von 6 Stunden. 10% Zinsen pro Stunde.`);
        }

        if (command === 'repay' || command === 'payback') {
            const user = tags.username.toLowerCase();

            if (!userStars[user] || !userStars[user].loanAmount || userStars[user].loanAmount <= 0) {
                client.say(channel, `/me @${tags.username} Du hast keine offenen Schulden.`);
                return;
            }

            const debt = userStars[user].loanAmount;
            const balance = userStars[user].balance;

            // Allow partial repayment? No, typically full repayment or whatever they can pay.
            // Let's just try to pay all.

            // Trap Logic: Timeout for 30 minutes and deny payment
            const timeoutDuration = 1800; // 30 Minutes

            if (client.readyState() === 'OPEN') {
                client.say(channel, `/timeout @${user} ${timeoutDuration} zu wenig geld opfer`);
                client.say(channel, `/me @${user} hat nicht genug Geld für die Rückzahlung und wurde für 30 Minuten timeoutet! Kredit läuft weiter.`);
            }
            // Do NOT repay anything.
            /* 
            if (balance >= debt) {
                userStars[user].balance -= debt;
                userStars[user].loanAmount = 0;
                userStars[user].loanDueDate = 0;
                userStars[user].repaymentFailures = 0;
                saveStars(user);
                client.say(channel, `/me @${tags.username} Kredit vollständig zurückgezahlt! Danke.`);
            } else {
                // Pay what they can
                const pay = balance; // Pay all they have
                if (pay > 0) {
                    userStars[user].balance = 0;
                    userStars[user].loanAmount -= pay;
                    saveStars(user);
                    client.say(channel, `/me @${tags.username} ${formatPoints(pay)} Star zurückgezahlt. Restschuld: ${formatPoints(userStars[user].loanAmount)} Star.`);
                } else {
                    client.say(channel, `/me @${tags.username} Du hast keine Stars zum Zurückzahlen!`);
                }
            }
            */
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
            const currentEmotes = channelEmotes[channel] || [];

            if (currentEmotes.length === 0) {
                // Try refresh if globally empty or just locally?
                // Let's refresh if empty
                await refreshEmotes();
                if ((channelEmotes[channel] || []).length === 0) {
                    client.say(channel, "eeeh lwk gibts hier keine emotes, guck mal ob du 7tv hast");
                    return;
                }
            }

            const activeEmotes = channelEmotes[channel] || [];
            if (activeEmotes.length === 0) {
                client.say(channel, "keine emotes gefunden für diesen channel");
                return;
            }

            const hintText = args.join(' ');
            if (!hintText.trim()) return;

            const filters = parseHint(hintText);

            if (filters.length === 0) {
                client.say(channel, "peepoConfused ich verstehe nichts, try so '14 2=c' oder 'hat 7 buchstaben'");
                return;
            }

            // Filter emotes
            const matches = activeEmotes.filter(name => {
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


            const currentEmotes = channelEmotes[channel] || [];
            if (currentEmotes.length < 3) {
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
                const r = currentEmotes[Math.floor(Math.random() * currentEmotes.length)];
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

        if (command === 'zahl') {
            const user = tags.username.toLowerCase();
            const input = args[0]; // Can be amount (start) or choice (finish)

            if (!userStars[user]) {
                userStars[user] = { balance: 0, lastClaim: 0, level: 0, investedStars: 0, nextLevelCost: 670, lastChannel: channel };
            }
            userStars[user].lastChannel = channel;

            // Scenario 1: User has an active game and is guessing
            if (activeGuessGames[user]) {
                if (!input) {
                    client.say(channel, `/me stop @${tags.username} du hast ein spiel offen! Sag "ungerade" oder "gerade" wideSpeedNod`);
                    return;
                }

                const choice = input.toLowerCase();
                let betsOnOdd = false;
                if (['ungerade', 'odd'].includes(choice)) {
                    betsOnOdd = true;
                } else if (['gerade', 'even'].includes(choice)) {
                    betsOnOdd = false;
                } else {
                    client.say(channel, `/me @${tags.username} bitte "ungerade" oder "gerade" wählen ${emote}`);
                    return;
                }

                const game = activeGuessGames[user];
                const isOdd = game.number % 2 !== 0;
                const win = (betsOnOdd && isOdd) || (!betsOnOdd && !isOdd);

                if (win) {
                    const winAmount = game.bet * 2;
                    userStars[user].balance += winAmount; // Refund bet + win
                    // Note: We already deducted the bet when starting, so adding winAmount results in +bet profit.
                    saveStars();
                    client.say(channel, `/me ${emote} @${tags.username} Zahl war ${game.number} - JUHU gewonnen! +${formatPoints(game.bet)} Star Balance: ${formatPoints(userStars[user].balance)} Star`);
                } else {
                    // Bet is already gone
                    saveStars();
                    client.say(channel, `/me ${emote} @${tags.username} Zahl war ${game.number} - ohno verloren. -${formatPoints(game.bet)} Star Balance: ${formatPoints(userStars[user].balance)} Star`);
                }

                delete activeGuessGames[user];
                return;
            }

            // Scenario 2: Start new game
            if (!input) {
                client.say(channel, `/me Nerd @${tags.username} Nutzung: ${currentPrefix}zahl <Menge>`);
                return;
            }

            const balance = userStars[user].balance;
            let betAmount = 0;

            if (input.toLowerCase() === 'all') {
                betAmount = balance;
            } else if (input.toLowerCase() === 'half' || input.toLowerCase() === 'hälfte') {
                betAmount = Math.floor(balance / 2);
            } else if (input.endsWith('%')) {
                const percentage = parseInt(input.slice(0, -1));
                if (!isNaN(percentage) && percentage > 0 && percentage <= 100) {
                    betAmount = Math.ceil(balance * (percentage / 100));
                }
            } else {
                betAmount = parseInt(input);
            }

            if (isNaN(betAmount) || betAmount <= 0) {
                client.say(channel, `/me @${tags.username} Ungültiger Einsatz bob`);
                return;
            }

            if (betAmount > balance) {
                client.say(channel, `/me @${tags.username} idiot du hast nur ${formatPoints(balance)} Star`);
                return;
            }

            // Deduct bet immediately
            userStars[user].balance -= betAmount;
            saveStars();

            // Store State
            activeGuessGames[user] = {
                number: Math.floor(Math.random() * 68),
                bet: betAmount,
                timestamp: Date.now()
            };

            client.say(channel, `/me @${tags.username} Spiel gestartet wideSpeedNod Einsatz: ${formatPoints(betAmount)} Star Ist die Zahl "gerade" oder "ungerade"? Hmm`);
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

        if (command === 'remindme') {
            let parsed = parseTimeInput(args);
            let dueAt = 0;
            let reminderMsg = "";
            // let shortId = "";

            if (!parsed) {
                // Treat as text-only/on-message
                let potentialMsg = args.join(' ');
                if (potentialMsg.trim().length === 0) {
                    const currentEmotes = channelEmotes[channel] || [];
                    if (currentEmotes.length > 0) {
                        potentialMsg = currentEmotes[Math.floor(Math.random() * currentEmotes.length)];
                    } else {
                        potentialMsg = "Erinnerung!";
                    }
                }
                dueAt = 0;
                reminderMsg = potentialMsg;
            } else {
                dueAt = parsed.dueAt;
                reminderMsg = parsed.message;

                if (!reminderMsg || reminderMsg.trim() === "") {
                    const currentEmotes = channelEmotes[channel] || [];
                    if (currentEmotes.length > 0) {
                        reminderMsg = currentEmotes[Math.floor(Math.random() * currentEmotes.length)];
                    } else {
                        reminderMsg = "Erinnerung!";
                    }
                }
            }

            if (dueAt > 0) {
                // lastReminderId++;
                // shortId removed
            }

            const sender = tags.username.toLowerCase();
            const reminderData = {
                targetUser: tags.username, // Display name
                sourceUser: tags.username,
                message: reminderMsg,
                dueAt: dueAt,
                channel: channel,
                // shortId: shortId,
                createdAt: Date.now()
            };

            if (useMongoDB) {
                await Reminder.create(reminderData);
            } else {
                localReminders.push(reminderData);
                saveLocalReminders();
            }

            if (dueAt > 0) {
                // Confirm
                const date = new Date(dueAt);
                // Manual Format to Berlin Time (Robust against Missing ICU)
                const offset = getBerlinOffset(date);
                const berlinDate = new Date(date.getTime() + offset);

                const pad = n => n < 10 ? '0' + n : n;
                const timeStr = `${pad(berlinDate.getUTCHours())}:${pad(berlinDate.getUTCMinutes())}`;

                const now = new Date();
                const nowOffset = getBerlinOffset(now);
                const nowBerlin = new Date(now.getTime() + nowOffset);

                const isToday = berlinDate.getUTCDate() === nowBerlin.getUTCDate() &&
                    berlinDate.getUTCMonth() === nowBerlin.getUTCMonth() &&
                    berlinDate.getUTCFullYear() === nowBerlin.getUTCFullYear();

                let timeDisplay = `um ${timeStr}`;
                if (!isToday) {
                    const dateStr = `${pad(berlinDate.getUTCDate())}.${pad(berlinDate.getUTCMonth() + 1)}`;
                    timeDisplay = `am ${dateStr} um ${timeStr}`;
                }

                client.say(channel, `/me @${tags.username} Top ich erinnere dich ${timeDisplay} " ${reminderMsg} "`);
            } else {
                client.say(channel, `/me @${tags.username} Top ich erinnere dich beim nächsten schreiben " ${reminderMsg} "`);
            }
        }

        if (command === 'remind') {
            const target = args[0];
            if (!target) {
                client.say(channel, `/me @${tags.username} ermm wen erinnern? Nerd Nutzung: ${currentPrefix}remind User [Zeit] Text`);
                return;
            }

            const timeArgs = args.slice(1);
            let parsed = parseTimeInput(timeArgs);
            let dueAt = 0;
            let reminderMsg = "";

            if (!parsed) {
                // No time found, treat everything after target as message
                let potentialMsg = timeArgs.join(' ');

                if (potentialMsg.trim().length === 0) {
                    // Empty message -> Random Emote
                    const currentEmotes = channelEmotes[channel] || [];
                    if (currentEmotes.length > 0) {
                        potentialMsg = currentEmotes[Math.floor(Math.random() * currentEmotes.length)];
                    } else {
                        potentialMsg = "lass uns eine skybase bauen wideSpeedNod ";
                    }
                }

                dueAt = 0; // On Message
                reminderMsg = potentialMsg;
            } else {
                dueAt = parsed.dueAt;
                reminderMsg = parsed.message;

                if (!reminderMsg || reminderMsg.trim() === "") {
                    const currentEmotes = channelEmotes[channel] || [];
                    if (currentEmotes.length > 0) {
                        reminderMsg = currentEmotes[Math.floor(Math.random() * currentEmotes.length)];
                    } else {
                        reminderMsg = " wideSpeedNod ich öffne die augen und beginne den tag";
                    }
                }
            }

            const cleanTarget = target.replace('@', '');

            // let shortId = "";
            if (dueAt > 0) {
                // lastReminderId++;
                // shortId removed
            }

            const reminderData = {
                targetUser: cleanTarget,
                sourceUser: tags.username,
                message: reminderMsg,
                dueAt: dueAt,
                channel: channel,
                // shortId: shortId,
                createdAt: Date.now()
            };

            if (useMongoDB) {
                await Reminder.create(reminderData);
            } else {
                localReminders.push(reminderData);
                saveLocalReminders();
            }

            if (dueAt > 0) {
                const date = new Date(dueAt);
                const timeOptions = { hour: '2-digit', minute: '2-digit', timeZone: 'Europe/Berlin' };
                const timeStr = date.toLocaleTimeString('de-DE', timeOptions);

                const now = new Date();
                const isToday = date.getDate() === now.getDate() && date.getMonth() === now.getMonth() && date.getFullYear() === now.getFullYear();

                let timeDisplay = `um ${timeStr}`;
                if (!isToday) {
                    const dateStr = date.toLocaleDateString('de-DE', { day: '2-digit', month: '2-digit', timeZone: 'Europe/Berlin' });
                    timeDisplay = `am ${dateStr} um ${timeStr}`;
                }

                client.say(channel, `/me @${tags.username} Noted ich erinnere ${cleanTarget} ${timeDisplay} " ${reminderMsg} "`);
            } else {
                client.say(channel, `/me @${tags.username} Noted ich erinnere ${cleanTarget} beim nächsten schreiben " ${reminderMsg} "`);
            }
        }



        if (command === 'reminders' || command === 'listreminders' || command === 'myreminders') {
            let myReminders = [];
            const userRegex = new RegExp('^' + tags.username + '$', 'i');

            if (useMongoDB) {
                myReminders = await Reminder.find({ targetUser: { $regex: userRegex } }).sort({ dueAt: 1 });
            } else {
                myReminders = localReminders.filter(r => r.targetUser.toLowerCase() === tags.username.toLowerCase())
                    .sort((a, b) => a.dueAt - b.dueAt);
            }

            if (myReminders.length === 0) {
                client.say(channel, `/me @${tags.username} Du hast keine aktiven Reminders.`);
                return;
            }

            let msgList = [];
            myReminders.forEach(r => {
                let timeInfo = "";
                if (r.dueAt === 0) {
                    timeInfo = "Beim nächsten Schreiben";
                } else {
                    const d = new Date(r.dueAt);
                    const tStr = d.toLocaleTimeString('de-DE', { hour: '2-digit', minute: '2-digit', timeZone: 'Europe/Berlin' });
                    timeInfo = `${tStr}`; // Showing just time to save space, assuming today/tmrw context usually
                    // Add Day if not today?
                    const now = new Date();
                    if (d.getDate() !== now.getDate()) {
                        timeInfo = `${d.getDate()}.${d.getMonth() + 1}. ${tStr}`;
                    }
                }

                msgList.push(`${timeInfo}: ${r.message}`);
            });

            const baseMsg = `/me @${tags.username} Deine Reminders: `;
            let currentMsg = baseMsg;

            for (let i = 0; i < msgList.length; i++) {
                const entry = (i === 0 ? "" : " | ") + msgList[i];
                if (currentMsg.length + entry.length > 450) {
                    client.say(channel, currentMsg);
                    currentMsg = `/me ... ${entry}`;
                } else {
                    currentMsg += entry;
                }
            }
            if (currentMsg !== baseMsg) {
                client.say(channel, currentMsg);
            }
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
                    const currentEmotes = channelEmotes[channel] || [];
                    if (currentEmotes.length > 0) {
                        emote = currentEmotes[Math.floor(Math.random() * currentEmotes.length)];
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
                client.say(channel, `/me @${tags.username} du hast nicht die nötige rolle Nerd`);
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
                const currentEmotes = channelEmotes[channel] || [];
                if (currentEmotes.length > 0) {
                    emote = currentEmotes[Math.floor(Math.random() * currentEmotes.length)];
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
                const currentEmotes = channelEmotes[channel] || [];
                if (currentEmotes.length > 0) {
                    emote = currentEmotes[Math.floor(Math.random() * currentEmotes.length)];
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
loadReminders();
restoreStarReminders();
refreshEmotes();
