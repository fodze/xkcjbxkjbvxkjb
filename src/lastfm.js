const https = require('https');

/**
 * Fetches the currently playing track for a Last.fm user.
 */
function getRecentTrack(username, apiKey) {
    return new Promise((resolve, reject) => {
        const url = `https://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=${encodeURIComponent(username)}&api_key=${apiKey}&format=json&limit=1`;

        https.get(url, (res) => {
            let data = '';
            res.on('data', (chunk) => data += chunk);
            res.on('end', () => {
                try {
                    const json = JSON.parse(data);
                    if (json.error) {
                        reject(new Error(json.message));
                        return;
                    }
                    const tracks = json.recenttracks.track;
                    if (!tracks || tracks.length === 0) {
                        resolve(null);
                        return;
                    }
                    // The first track is the most recent.
                    // If it's currently playing, it will have @attr: { nowplaying: 'true' }
                    const track = Array.isArray(tracks) ? tracks[0] : tracks;
                    resolve(track);
                } catch (e) {
                    reject(e);
                }
            });
        }).on('error', reject);
    });
}

/**
 * Fetches track info, specifically the user's playcount for that track.
 */
function getTrackPlaycount(username, artist, trackName, apiKey) {
    return new Promise((resolve, reject) => {
        const url = `https://ws.audioscrobbler.com/2.0/?method=track.getInfo&api_key=${apiKey}&artist=${encodeURIComponent(artist)}&track=${encodeURIComponent(trackName)}&username=${encodeURIComponent(username)}&format=json`;

        https.get(url, (res) => {
            let data = '';
            res.on('data', (chunk) => data += chunk);
            res.on('end', () => {
                try {
                    const json = JSON.parse(data);
                    if (json.error) {
                        // If track not found, maybe playcount is 0 or error
                        resolve(0);
                        return;
                    }
                    const userplaycount = json.track && json.track.userplaycount ? parseInt(json.track.userplaycount) : 0;
                    resolve(userplaycount);
                } catch (e) {
                    reject(e);
                }
            });
        }).on('error', reject);
    });
}

/**
 * Helper to fetch both at once (Current track info + playcount)
 */
async function getNowPlayingWithPlaycount(username, apiKey) {
    const track = await getRecentTrack(username, apiKey);
    if (!track) return null;

    const isNowPlaying = track['@attr'] && track['@attr'].nowplaying === 'true';
    const artist = track.artist['#text'];
    const trackName = track.name;

    const playcount = await getTrackPlaycount(username, artist, trackName, apiKey);

    return {
        artist,
        track: trackName,
        playcount,
        isNowPlaying,
        url: track.url
    };
}

module.exports = {
    getNowPlayingWithPlaycount
};
