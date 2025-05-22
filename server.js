const express = require('express');
const { WebcastPushConnection } = require('tiktok-live-connector');
const fs = require('fs').promises;
const path = require('path');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const port = process.env.PORT || 3000;

// Create HTTP server and WebSocket server
const server = http.createServer(app);
const io = new Server(server);

// Store active connections
const activeConnections = new Map();
const monitoringData = new Map();
const giftLogs = new Map(); // Store gift logs
const sessionData = new Map(); // Temporary storage for live session data

// Paths for JSON and CSV files
const sessionJsonPath = path.join(__dirname, 'public', 'session_data.json');
const csv1Path = path.join(__dirname, 'public', 'session_summary.csv');
const csv2Path = path.join(__dirname, 'public', 'top_spenders.csv');

// Serve static files
app.use(express.static('public'));
app.use(express.json());

// Serve the Socket.IO client library using a CDN
app.get('/socket.io.js', (req, res) => {
    res.redirect('https://cdn.jsdelivr.net/npm/socket.io-client/dist/socket.io.js');
});

// Load usernames from accounts.txt
async function loadUsernames() {
    try {
        const data = await fs.readFile('accounts.txt', 'utf8');
        return data
            .split('\n')
            .map(line => line.trim())
            .filter(line => line && !line.startsWith('#'));
    } catch (error) {
        console.error('Error loading usernames:', error);
        return [];
    }
}

// Enhanced error logging in monitorAccount
async function monitorAccount(username) {
    try {
        // If already monitoring, stop first
        if (activeConnections.has(username)) {
            console.warn(`Already monitoring @${username}. Stopping existing connection.`);
            await stopMonitoring(username);
        }

        console.log(`Starting monitor for @${username}`);
        const tiktokConnection = new WebcastPushConnection(username);

        // Store monitoring data
        monitoringData.set(username, {
            username,
            exists: 'UNKNOWN',
            liveStatus: 'CHECKING',
            connectionStatus: 'CONNECTING',
            lastChecked: new Date().toISOString(),
            viewerCount: 0,
            totalGifts: 0,
            giftValue: 0
        });

        // Initialize session data
        sessionData.set(username, {
            username,
            connectionTime: new Date().toISOString(),
            maxViewerCount: 0,
            totalDiamonds: 0,
            topGifter: null
        });

        // Connect to TikTok
        await tiktokConnection.connect().catch(error => {
            console.error(`Failed to connect to @${username}:`, error);
            throw new Error(`Connection failed for @${username}`);
        });

        // Update status on successful connection
        updateAccountStatus(username, {
            exists: 'FOUND',
            liveStatus: 'LIVE',
            connectionStatus: 'CONNECTED'
        });

        // Event handlers
        tiktokConnection.on('streamEnd', () => {
            updateAccountStatus(username, {
                liveStatus: 'NOT LIVE',
                connectionStatus: 'DISCONNECTED'
            });
        });

        tiktokConnection.on('roomUser', (data) => {
            const currentData = monitoringData.get(username);

            // Update current viewer count
            const updatedViewerCount = data.viewerCount;
            const maxViewerCount = Math.max(currentData.highestViewerCount || 0, updatedViewerCount);

            updateAccountStatus(username, {
                viewerCount: updatedViewerCount,
                highestViewerCount: maxViewerCount
            });

            const session = sessionData.get(username);
            if (session) {
                session.maxViewerCount = Math.max(session.maxViewerCount, data.viewerCount);
            }
        });

        tiktokConnection.on('gift', (data) => {
            const currentData = monitoringData.get(username);

            // Update total gifts and diamond value
            updateAccountStatus(username, {
                totalGifts: (currentData.totalGifts || 0) + 1,
                giftValue: (currentData.giftValue || 0) + (data.diamondCount || 0)
            });

            // Update top spenders
            if (!currentData.topGifters) {
                currentData.topGifters = [];
            }
            const existingGifter = currentData.topGifters.find(g => g.username === data.uniqueId);
            if (existingGifter) {
                existingGifter.diamonds += data.diamondCount;
            } else {
                currentData.topGifters.push({ username: data.uniqueId, diamonds: data.diamondCount });
            }
            currentData.topGifters.sort((a, b) => b.diamonds - a.diamonds);
            currentData.topGifters = currentData.topGifters.slice(0, 10);

            // Emit updated top gifters
            emitTopGifters(username);

            // Emit updated data to frontend
            emitGiftData(username, data);

            const session = sessionData.get(username);
            if (session) {
                session.totalDiamonds += data.diamondCount;
                if (!session.topGifter || data.diamondCount > session.topGifter.diamonds) {
                    session.topGifter = { username: data.uniqueId, diamonds: data.diamondCount };
                }
            }
        });

        // Handle TikTok-Live-Connector errors gracefully
        tiktokConnection.on('connectError', (error) => {
            console.error(`Connection error for @${username}:`, error);
            updateAccountStatus(username, {
                exists: 'NOT FOUND',
                liveStatus: 'NOT LIVE',
                connectionStatus: 'DISCONNECTED'
            });
        });

        // Store the connection
        activeConnections.set(username, tiktokConnection);

    } catch (error) {
        console.error(`Error monitoring @${username}:`, error);
        if (error.message.includes('UserOfflineError')) {
            console.warn(`@${username} is not live. Skipping monitoring.`);
        } else if (error.message.includes('timeout')) {
            console.warn(`Connection to @${username} timed out. Retrying later.`);
        } else {
            console.error(`Unexpected error for @${username}:`, error);
        }
        updateAccountStatus(username, {
            exists: 'FOUND',
            liveStatus: 'NOT LIVE',
            connectionStatus: 'DISCONNECTED'
        });
    }
}

// Update account status
function updateAccountStatus(username, updates) {
    const currentData = monitoringData.get(username) || {};
    monitoringData.set(username, {
        ...currentData,
        ...updates,
        lastChecked: new Date().toISOString()
    });

    if (updates.liveStatus === 'NOT LIVE') {
        const session = sessionData.get(username);
        if (session) {
            session.disconnectionTime = new Date().toISOString();
        }
        saveSessionToJson();
        saveSessionToCsv();
    }
}

// Emit gift data to frontend
function emitGiftData(username, gift) {
    if (!giftLogs.has(username)) {
        giftLogs.set(username, []);
    }
    const log = {
        username: gift.uniqueId,
        diamondCount: gift.diamondCount,
        timestamp: new Date().toISOString()
    };
    const logs = giftLogs.get(username);
    logs.push(log);
    if (logs.length > 50) {
        logs.shift(); // Keep only the last 50 entries
    }
    giftLogs.set(username, logs);
}

// Emit updated top gifters to frontend
function emitTopGifters(username) {
    const currentData = monitoringData.get(username);
    if (currentData) {
        const topGifters = currentData.topGifters || [];
        io.emit('updateTopGifters', { username, topGifters });
    }
}

// Stop monitoring a single account
async function stopMonitoring(username) {
    const connection = activeConnections.get(username);
    if (connection) {
        try {
            await connection.disconnect();
            activeConnections.delete(username);
            updateAccountStatus(username, {
                liveStatus: 'NOT LIVE',
                connectionStatus: 'DISCONNECTED'
            });
        } catch (error) {
            console.error(`Error stopping monitor for @${username}:`, error);
        }
    }
}

// Fix saveSessionToJson to use fs.writeFile
async function saveSessionToJson() {
    try {
        const data = Array.from(sessionData.values());
        await fs.writeFile(sessionJsonPath, JSON.stringify(data, null, 2));
        console.log(`Session data successfully saved to ${sessionJsonPath}`);
    } catch (error) {
        console.error(`Error saving session data to JSON at ${sessionJsonPath}:`, error);
    }
}

// Fix saveSessionToCsv to use fs.writeFile
async function saveSessionToCsv() {
    try {
        const csv1 = ['Connection Time,Account Name,Max Viewers,Total Diamonds,Disconnection Time'];
        const csv2 = ['Connection Time,Account Name,Top Spender 1,Top Spender 2,Top Spender 3'];

        sessionData.forEach((session) => {
            const disconnectionTime = session.disconnectionTime || '';
            csv1.push(`${session.connectionTime},${session.username},${session.maxViewerCount},${session.totalDiamonds},${disconnectionTime}`);

            const topSpenders = session.topGifters || [];
            const spenderRow = [
                session.connectionTime,
                session.username,
                ...topSpenders.slice(0, 3).map(g => `${g.username} (${g.diamonds})`)
            ];
            csv2.push(spenderRow.join(','));
        });

        await fs.writeFile(csv1Path, csv1.join('\n'));
        console.log(`Session summary successfully saved to ${csv1Path}`);

        await fs.writeFile(csv2Path, csv2.join('\n'));
        console.log(`Top spenders successfully saved to ${csv2Path}`);
    } catch (error) {
        console.error(`Error saving session data to CSV at ${csv1Path} or ${csv2Path}:`, error);
    }
}

// Periodically save data every 15 minutes
setInterval(() => {
    saveSessionToJson();
    saveSessionToCsv();
}, 15 * 60 * 1000);

// Prevent immediate exit on SIGINT
process.on('SIGINT', () => {
    console.log('SIGINT received. Saving session data...');
    saveSessionToJson();
    saveSessionToCsv();
    console.log('Session data saved. You can now safely terminate the program.');
});

// API Routes
app.get('/api/status', (req, res) => {
    const status = {
        monitoring: activeConnections.size,
        accounts: Array.from(monitoringData.values())
    };
    res.json(status);
});

// Enhanced error handling in /api/start
app.post('/api/start', async (req, res) => {
    try {
        const usernames = await loadUsernames();
        if (usernames.length === 0) {
            console.warn('No usernames found in accounts.txt');
            return res.status(400).json({ message: 'No usernames to monitor' });
        }

        for (const username of usernames) {
            try {
                await monitorAccount(username);
            } catch (error) {
                console.error(`Failed to start monitoring for @${username}:`, error);
            }
        }

        res.json({ message: 'Started monitoring' });
    } catch (error) {
        console.error('Error in /api/start:', error);
        res.status(500).json({ message: 'Failed to start monitoring' });
    }
});

app.post('/api/stop', async (req, res) => {
    for (const username of activeConnections.keys()) {
        await stopMonitoring(username);
    }
    saveSessionToJson();
    saveSessionToCsv();
    res.json({ message: 'Stopped monitoring' });
});

app.get('/api/gifts/:username', (req, res) => {
    const username = req.params.username;
    res.json(giftLogs.get(username) || []);
});

app.get('/api/session/:username', (req, res) => {
    const username = req.params.username;
    res.json(sessionData.get(username) || {});
});

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Start server
server.listen(port, () => {
    console.log(`Server running on port ${port}`);
});
