const express = require('express');
const { WebcastPushConnection } = require('tiktok-live-connector');
const fs = require('fs').promises;
const path = require('path');
const http = require('http');
const { Server } = require('socket.io');
const { createObjectCsvWriter } = require('csv-writer');

const app = express();
const port = process.env.PORT || 3000;

// Create HTTP server and WebSocket server
const server = http.createServer(app);
const io = new Server(server);

// Constants
const STORAGE_DIR = path.join(__dirname, 'public');
const SESSION_FILE = path.join(STORAGE_DIR, 'session_data.json');
const DATA_LIVE_CSV = path.join(STORAGE_DIR, 'data_live.csv');
const TOP_SPENDER_CSV = path.join(STORAGE_DIR, 'top_spender.csv');
const LIVE_END_TIMEOUT = 180000; // 3 minutes without events = end of live
const SESSION_CONTINUITY_THRESHOLD = 300000; // 5 minutes threshold for same session
const RECONNECT_DELAYS = [10000, 30000]; // Backoff delays for live accounts: 10s, 30s only
const OFFLINE_RETRY_INTERVAL = 15 * 60 * 1000; // 15 minutes for offline accounts

// State management
const activeConnections = new Map();
const activeSessions = new Map();
const offlineAccountRetryTimes = new Map();
let offlineCheckerInterval = null;

// CSV Writers
const liveDataWriter = createObjectCsvWriter({
    path: DATA_LIVE_CSV,
    header: [
        { id: 'kode_sesi', title: 'kode_sesi' },
        { id: 'tgl_jam_mulai', title: 'tgl_jam_mulai' },
        { id: 'akun', title: 'akun' },
        { id: 'durasi', title: 'durasi' },
        { id: 'peak_viewer', title: 'peak_viewer' },
        { id: 'total_diamond', title: 'total_diamond' }
    ]
});

const topSpenderWriter = createObjectCsvWriter({
    path: TOP_SPENDER_CSV,
    header: [
        { id: 'kode_sesi', title: 'kode_sesi' },
        { id: 'tgl_jam_mulai', title: 'tgl_jam_mulai' },
        { id: 'akun', title: 'akun' },
        { id: 'top1', title: 'top1' },
        { id: 'top2', title: 'top2' },
        { id: 'top3', title: 'top3' },
        { id: 'top4', title: 'top4' },
        { id: 'top5', title: 'top5' },
        { id: 'top6', title: 'top6' },
        { id: 'top7', title: 'top7' },
        { id: 'top8', title: 'top8' },
        { id: 'top9', title: 'top9' },
        { id: 'top10', title: 'top10' }
    ]
});

// Helper: Load session data
async function loadSessionData(session_id = null) {
    try {
        const data = await fs.readFile(SESSION_FILE, 'utf8');
        const sessions = JSON.parse(data);
        if (session_id) {
            return sessions.find(s => s.session_id === session_id);
        }
        return sessions;
    } catch (error) {
        return session_id ? null : [];
    }
}

// Helper: Save session data
async function saveSessionData(sessions) {
    try {
        await fs.writeFile(SESSION_FILE, JSON.stringify(sessions, null, 2));
    } catch (error) {
        console.error('Error saving session data:', error);
    }
}

// Create new session
async function createNewSession(username, roomId) {
    const startTime = new Date().toISOString();
    const session_id = `${username}_${startTime}`;
    
    const newSession = {
        username,
        session_id,
        room_id: roomId,
        start_time: startTime,
        end_time: null,
        status: 'active',
        viewer_peak: 0,
        gift_data: {},
        top_10: [],
        last_event_time: startTime
    };

    const sessions = await loadSessionData();
    // End any existing active sessions for this user
    const existingSessions = sessions.filter(s => s.username === username && s.status === 'active');
    for (const session of existingSessions) {
        session.status = 'ended';
        session.end_time = startTime;
    }
    
    sessions.push(newSession);
    await saveSessionData(sessions);
    console.log(`Created new session: ${session_id} for ${username}`);
    return newSession;
}

// Check if account is live without starting full monitoring
async function checkLiveStatus(username) {
    console.log(`[${new Date().toISOString()}] Checking live status for ${username}`);
    
    try {
        const tiktokConnection = new WebcastPushConnection(username);
        
        // Wrap connection in a promise with timeout
        const status = await Promise.race([
            new Promise(async (resolve) => {
                try {
                    await tiktokConnection.connect();
                    resolve({ 
                        isLive: true, 
                        roomId: tiktokConnection.roomId,
                        status: 'live'
                    });
                } catch (err) {
                    resolve({ 
                        isLive: false, 
                        error: err.message,
                        status: 'offline'
                    });
                } finally {
                    tiktokConnection.disconnect();
                }
            }),
            new Promise((resolve) => 
                setTimeout(() => resolve({ 
                    isLive: false, 
                    error: 'Connection timeout',
                    status: 'error'
                }), 10000)
            )
        ]);

        return status;
    } catch (error) {
        return { 
            isLive: false, 
            error: error.message,
            status: 'error'
        };
    }
}

// Monitor account with strict session management
async function monitorAccount(username, reconnectAttempt = 0) {
    if (activeConnections.has(username)) {
        console.log(`Already monitoring ${username}`);
        return;
    }

    // For live accounts, only allow 2 retry attempts
    const MAX_LIVE_RETRIES = 2;
    
    if (reconnectAttempt > MAX_LIVE_RETRIES) {
        console.log(`[${username}] Max retry attempts (${MAX_LIVE_RETRIES}) reached, marking as offline`);
        offlineAccountRetryTimes.set(username, Date.now() + OFFLINE_RETRY_INTERVAL);
        return;
    }

    const tiktokConnection = new WebcastPushConnection(username);
    let currentSession = null;
    let liveEndTimer = null;

    try {
        console.log(`Connecting to ${username}'s live stream...`);
        const state = await tiktokConnection.connect();
        
        currentSession = await createNewSession(username, state.roomId);
        activeConnections.set(username, tiktokConnection);
        
        console.log(`Connected to ${username}'s live stream. Session: ${currentSession.session_id}`);
        
        // Event handlers
        tiktokConnection.on('gift', async (data) => {
            if (!data.giftType === 1) return; // Only count paid gifts
            
            const giftData = {
                sender: data.nickname,
                diamonds: data.diamondCount * (data.repeatCount || 1)
            };
            
            const session = await updateSessionGiftData(currentSession.session_id, giftData);
            if (session) {
                io.emit('sessionUpdate', formatSessionUpdate(session));
            }
        });

        tiktokConnection.on('social', async (data) => {
            currentSession.last_event_time = new Date().toISOString();
        });

        tiktokConnection.on('like', async (data) => {
            currentSession.last_event_time = new Date().toISOString();
        });

        tiktokConnection.on('chat', async (data) => {
            currentSession.last_event_time = new Date().toISOString();
        });

        tiktokConnection.on('member', async (data) => {
            currentSession.last_event_time = new Date().toISOString();
        });

        tiktokConnection.on('streamEnd', async () => {
            console.log(`Stream ended for ${username}`);
            if (currentSession) {
                await endSession(currentSession.session_id);
            }
            activeConnections.delete(username);
            // Move to offline check queue
            offlineAccountRetryTimes.set(username, Date.now() + OFFLINE_RETRY_INTERVAL);
        });

    } catch (error) {
        console.error(`Error monitoring ${username}:`, error);
        activeConnections.delete(username);
        
        if (currentSession) {
            await endSession(currentSession.session_id);
        }

        if (reconnectAttempt < MAX_LIVE_RETRIES) {
            const delay = RECONNECT_DELAYS[reconnectAttempt];
            console.log(`[${username}] Retry ${reconnectAttempt + 1}/${MAX_LIVE_RETRIES} in ${delay/1000}s`);
            setTimeout(() => monitorAccount(username, reconnectAttempt + 1), delay);
        } else {
            console.log(`[${username}] Max retries reached, moving to offline queue`);
            offlineAccountRetryTimes.set(username, Date.now() + OFFLINE_RETRY_INTERVAL);
        }
    }
}

// Format session update for frontend
function formatSessionUpdate(session) {
    return {
        username: session.username,
        sessionId: session.session_id,
        isLive: session.status === 'active',
        startTime: session.start_time,
        endTime: session.end_time,
        viewerPeak: session.viewer_peak,
        recentGifts: Object.entries(session.gift_data).map(([sender, diamonds]) => ({
            sender,
            diamonds
        })),
        topSpenders: session.top_10,
        totalDiamonds: Object.values(session.gift_data).reduce((a, b) => a + b, 0)
    };
}

// Helper functions for CSV operations
async function readCsvFile(filePath) {
    try {
        const content = await fs.readFile(filePath, 'utf8');
        if (!content.trim()) return [];
        
        const lines = content.split('\n');
        const headers = lines[0].split(',');
        
        return lines
            .slice(1)
            .filter(line => line.trim())
            .map(line => {
                const values = line.split(',');
                const record = {};
                headers.forEach((header, index) => {
                    record[header.trim()] = values[index] ? values[index].trim() : '';
                });
                return record;
            });
    } catch (error) {
        console.error(`Error reading CSV file ${filePath}:`, error);
        return [];
    }
}

async function writeCsvData(writer, records) {
    try {
        const validRecords = records.filter(record => 
            record && typeof record === 'object' && Object.keys(record).length > 0
        );
        
        if (validRecords.length > 0) {
            await writer.writeRecords(validRecords);
            console.log(`Successfully wrote ${validRecords.length} records to CSV`);
        }
    } catch (error) {
        console.error('Error writing CSV data:', error);
    }
}

// Initialize CSV files
async function initializeCsvFiles() {
    try {
        // Create empty CSV files with headers
        await writeCsvData(liveDataWriter, []);
        await writeCsvData(topSpenderWriter, []);
        console.log('CSV files initialized');
    } catch (error) {
        console.error('Error initializing CSV files:', error);
    }
}

// API Endpoints

// Get account list
app.get('/api/accounts', async (req, res) => {
    try {
        const usernames = await loadUsernames();
        res.json({ accounts: usernames });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Check account status
app.post('/api/check-status', async (req, res) => {
    try {
        const usernames = await loadUsernames();
        const statuses = await Promise.all(
            usernames.map(async username => {
                const status = await checkLiveStatus(username);
                return { username, ...status };
            })
        );
        res.json({ statuses });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Start monitoring
app.post('/api/monitor/start', async (req, res) => {
    try {
        const usernames = await loadUsernames();
        
        // Check all accounts first
        const statuses = await Promise.all(
            usernames.map(async username => {
                const status = await checkLiveStatus(username);
                return { username, ...status };
            })
        );
        
        // Start monitoring live accounts
        const liveAccounts = statuses.filter(s => s.isLive).map(s => s.username);
        const startPromises = liveAccounts.map(username => monitorAccount(username));
        await Promise.all(startPromises);
        
        // Schedule offline accounts
        const offlineAccounts = statuses.filter(s => !s.isLive).map(s => s.username);
        offlineAccounts.forEach(username => {
            offlineAccountRetryTimes.set(username, Date.now() + OFFLINE_RETRY_INTERVAL);
        });
        
        // Start offline checker if needed
        if (offlineAccounts.length > 0 && !offlineCheckerInterval) {
            startOfflineRetryChecker();
        }
        
        res.json({ 
            message: 'Monitoring started',
            liveAccounts,
            offlineAccounts
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Stop monitoring
app.post('/api/monitor/stop', async (req, res) => {
    try {
        // Stop offline checker
        if (offlineCheckerInterval) {
            clearInterval(offlineCheckerInterval);
            offlineCheckerInterval = null;
        }
        
        // Clear offline retry times
        offlineAccountRetryTimes.clear();
        
        // Disconnect all active connections
        for (const [username, connection] of activeConnections) {
            connection.disconnect();
        }
        activeConnections.clear();
        
        // End all active sessions
        const sessions = await loadSessionData();
        const activeSessions = sessions.filter(s => s.status === 'active');
        
        for (const session of activeSessions) {
            session.status = 'ended';
            session.end_time = new Date().toISOString();
            await exportSessionToCSV(session);
        }
        
        await saveSessionData(sessions);
        res.json({ message: 'Monitoring stopped' });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// CSV operations
app.post('/api/export/csv', async (req, res) => {
    try {
        const sessions = await loadSessionData();
        const activeSessions = sessions.filter(s => s.status === 'active');
        
        for (const session of activeSessions) {
            await exportSessionToCSV(session);
        }
        
        res.json({ message: 'CSV data synchronized successfully' });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/export/download', async (req, res) => {
    try {
        const csvFile = req.query.type === 'top-spender' ? TOP_SPENDER_CSV : DATA_LIVE_CSV;
        res.download(csvFile);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Socket.IO event handlers
io.on('connection', async (socket) => {
    console.log('Client connected');
    
    try {
        const sessions = await loadSessionData();
        const updates = sessions.map(formatSessionUpdate);
        socket.emit('initialData', updates);
    } catch (error) {
        console.error('Error sending initial data:', error);
    }
    
    socket.on('disconnect', () => {
        console.log('Client disconnected');
    });
});

// Initialize server
server.listen(port, async () => {
    console.log(`Server running on port ${port}`);
    await initializeCsvFiles();
});
