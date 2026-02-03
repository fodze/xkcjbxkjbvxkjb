const https = require('https');

/**
 * Validates the OAuth token and returns the Client ID.
 */
function getClientId(token) {
    return new Promise((resolve, reject) => {
        const cleanToken = token.startsWith('oauth:') ? token.substring(6) : token;
        const options = {
            hostname: 'id.twitch.tv',
            path: '/oauth2/validate',
            method: 'GET',
            headers: { 'Authorization': `OAuth ${cleanToken}` }
        };

        const req = https.request(options, (res) => {
            let data = '';
            res.on('data', (chunk) => data += chunk);
            res.on('end', () => {
                if (res.statusCode === 200) {
                    resolve(JSON.parse(data).client_id);
                } else {
                    reject(new Error(`Token validation failed: ${res.statusCode}`));
                }
            });
        });
        req.on('error', reject);
        req.end();
    });
}

/**
 * Gets the Twitch User ID for a username using Helix API.
 */
function getTwitchUserId(username, clientId, token) {
    return new Promise((resolve, reject) => {
        const cleanToken = token.replace('oauth:', '');
        const options = {
            hostname: 'api.twitch.tv',
            path: `/helix/users?login=${username}`,
            method: 'GET',
            headers: {
                'Client-ID': clientId,
                'Authorization': `Bearer ${cleanToken}`
            }
        };

        const req = https.request(options, (res) => {
            let data = '';
            res.on('data', (chunk) => data += chunk);
            res.on('end', () => {
                if (res.statusCode === 200) {
                    const json = JSON.parse(data);
                    if (json.data && json.data.length > 0) {
                        resolve(json.data[0].id);
                    } else {
                        resolve(null);
                    }
                } else {
                    reject(new Error(`Twitch API error: ${res.statusCode} ${data}`));
                }
            });
        });
        req.on('error', reject);
        req.end();
    });
}

/**
 * Gets the Twitch User details (username) for a given ID using Helix API.
 */


/**
 * Gets the Twitch User details (username) for a given ID using Helix API.
 */
function getTwitchUserById(userId, clientId, token) {
    return new Promise((resolve, reject) => {
        const cleanToken = token.replace('oauth:', '');
        const options = {
            hostname: 'api.twitch.tv',
            path: `/helix/users?id=${userId}`,
            method: 'GET',
            headers: {
                'Client-ID': clientId,
                'Authorization': `Bearer ${cleanToken}`
            }
        };

        const req = https.request(options, (res) => {
            let data = '';
            res.on('data', (chunk) => data += chunk);
            res.on('end', () => {
                if (res.statusCode === 200) {
                    const json = JSON.parse(data);
                    if (json.data && json.data.length > 0) {
                        resolve(json.data[0]); // Returns whole user object { id, login, display_name, ... }
                    } else {
                        resolve(null);
                    }
                } else {
                    reject(new Error(`Twitch API error: ${res.statusCode} ${data}`));
                }
            });
        });
        req.on('error', reject);
        req.end();
    });
}

/**
 * Fetches the emote set for a given Twitch User ID from 7TV.
 */
function get7TVEmotes(twitchUserId) {
    return new Promise((resolve, reject) => {
        const options = {
            hostname: '7tv.io',
            path: `/v3/users/twitch/${twitchUserId}`,
            method: 'GET',
            headers: { 'Content-Type': 'application/json' }
        };

        const req = https.request(options, (res) => {
            let data = '';
            res.on('data', (chunk) => data += chunk);
            res.on('end', () => {
                if (res.statusCode === 404) {
                    resolve([]); // User has no 7TV
                    return;
                }
                try {
                    const json = JSON.parse(data);
                    // 7TV v3 structure: user -> emote_set -> emotes
                    // Or sometimes directly emote_set if different endpoint? 
                    // Based on my manual check: root object has "emote_set" (or "user" -> "emote_set"?)
                    // The output from 7tv.io/v3/users/twitch/{id} IS the user object.
                    // It has an "emote_set" field.
                    if (json.emote_set && json.emote_set.emotes) {
                        resolve(json.emote_set.emotes);
                    } else if (json.user && json.user.emote_set && json.user.emote_set.emotes) {
                        resolve(json.user.emote_set.emotes);
                    } else {
                        resolve([]);
                    }
                } catch (e) {
                    reject(e);
                }
            });
        });

        req.on('error', reject);
        req.end();
    });
}

function parseHint(hintText) {
    const filters = [];
    const text = hintText;

    // --- Format 1: Manual Short (e.g. "14 2=c 5=p" or "14 2 c") ---
    // Check if the input looks like a set of short codes
    // Matches standalone numbers (length) or pos=char / pos char patterns
    const tokens = text.trim().split(/\s+/);

    // Simple heuristic: if it contains "hat" or "Buchstabe:", process as text sentence (Format 2).
    // Otherwise try to process as short codes.
    const isSentence = /hat|Buchstabe:|Tips:/i.test(text);

    if (!isSentence) {
        for (let i = 0; i < tokens.length; i++) {
            const token = tokens[i];

            // "14" -> Length
            if (/^\d+$/.test(token)) {
                // If the NEXT token is a single char, it might be "2 c". 
                // But we need to distinguish "Length 14" vs "Pos 2".
                // Usually Length is the first number or large number.
                // Let's assume if it's the *first* token and large (>0), it's length?
                // OR: pos=char syntax is safer.

                // Let's support "14" = length IF it's the first numeric arg or > 10? 
                // Ambiguity: "5" -> Length 5 or Pos 5?
                // Let's assume standalone number without following char is Length.

                const nextToken = tokens[i + 1];
                if (nextToken && /^[a-zA-Z0-9]$/.test(nextToken)) {
                    // "2 c" pattern -> Position
                    const pos = parseInt(token) - 1;
                    const char = nextToken.toLowerCase();
                    filters.push(name => name.toLowerCase()[pos] === char);
                    i++; // Skip next token
                } else {
                    // Just a number -> Length
                    const len = parseInt(token);
                    filters.push(name => name.length === len);
                }
                continue;
            }

            // "2=c" or "2:c"
            const posMatch = token.match(/^(\d+)[:=]([a-zA-Z0-9])$/);
            if (posMatch) {
                const pos = parseInt(posMatch[1]) - 1;
                const char = posMatch[2].toLowerCase();
                filters.push(name => name.toLowerCase()[pos] === char);
            }
        }
        return filters;
    }

    // --- Format 2: Natural Language Sentence ---

    // 1. Length check: "Das Emote hat 14 Buchstaben"
    const lengthMatch = text.match(/hat\s+(\d+)\s+Buchstaben/i);
    if (lengthMatch) {
        const len = parseInt(lengthMatch[1]);
        filters.push(name => name.length === len);
    }

    // 2. Parse the "Tips:" section
    const ordinals = {
        'erster': 0, 'erste': 0, '1.': 0,
        'zweiter': 1, 'zweite': 1, '2.': 1,
        'dritter': 2, 'dritte': 2, '3.': 2,
        'vierter': 3, 'vierte': 3, '4.': 3,
        'fünfter': 4, 'fünfte': 4, '5.': 4,
        'sechster': 5, 'sechste': 5, '6.': 5,
        'siebter': 6, 'siebte': 6, '7.': 6,
        'achter': 7, 'achte': 7, '8.': 7,
        'neunter': 8, 'neunte': 8, '9.': 8,
        'zehnter': 9, 'zehnte': 9, '10.': 9
    };

    const charMatches = text.matchAll(/([a-zA-Zäöüß0-9.]+)\s+Buchstabe:\s+([a-zA-Z0-9])/gi);

    for (const match of charMatches) {
        const positionWord = match[1].toLowerCase();
        const char = match[2].toLowerCase();

        if (ordinals.hasOwnProperty(positionWord)) {
            const index = ordinals[positionWord];
            filters.push(name => name.toLowerCase()[index] === char);
        } else if (positionWord.includes('vorletzter') || positionWord.includes('vorletzte')) {
            filters.push(name => {
                const n = name.toLowerCase();
                return n.length >= 2 && n[n.length - 2] === char;
            });
        } else if (positionWord.includes('letzter') || positionWord.includes('letzte')) {
            filters.push(name => {
                const n = name.toLowerCase();
                return n.length >= 1 && n[n.length - 1] === char;
            });
        }
    }

    return filters;
}

/**
 * Modern Helix Timeout API
 */
function helixTimeout(broadcasterId, moderatorId, userId, duration, reason, clientId, token) {
    return new Promise((resolve, reject) => {
        const cleanToken = token.startsWith('oauth:') ? token.substring(6) : token;
        const body = JSON.stringify({
            data: {
                user_id: userId,
                duration: duration,
                reason: reason || "No reason provided"
            }
        });

        const options = {
            hostname: 'api.twitch.tv',
            path: `/helix/moderation/bans?broadcaster_id=${broadcasterId}&moderator_id=${moderatorId}`,
            method: 'POST',
            headers: {
                'Client-ID': clientId,
                'Authorization': `Bearer ${cleanToken}`,
                'Content-Type': 'application/json'
            }
        };

        const req = https.request(options, (res) => {
            let data = '';
            res.on('data', (chunk) => data += chunk);
            res.on('end', () => {
                if (res.statusCode === 200 || res.statusCode === 204) {
                    resolve(true);
                } else {
                    reject(new Error(`Helix Timeout Error: ${res.statusCode} - ${data}`));
                }
            });
        });

        req.on('error', reject);
        req.write(body);
        req.end();
    });
}

module.exports = {
    getClientId,
    getTwitchUserId,
    getTwitchUserById,
    get7TVEmotes,
    parseHint,
    helixTimeout
};
