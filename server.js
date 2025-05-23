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
const RECONNECT_DELAYS = [10000, 30000, 60000]; // Backoff delays: 10s, 30s, 1m
const AUTO_SAVE_INTERVAL = 300000; // Auto-save interval (5 minutes)

// State management
const activeConnections = new Map();
const activeSessions = new Map();

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

// Check and get active session
async function getActiveSession(username, roomId) {
    const sessions = await loadSessionData();
    const now = new Date();
    
    // Find the most recent active session for this user
    const activeSession = sessions
        .filter(s => s.username === username && s.status === 'active')
        .sort((a, b) => new Date(b.last_event_time) - new Date(a.last_event_time))[0];
    
    if (activeSession) {
        const lastEventTime = new Date(activeSession.last_event_time);
        const timeDiff = now - lastEventTime;
        
        // If session is recent (within threshold) and same room, continue it
        if (timeDiff < SESSION_CONTINUITY_THRESHOLD && activeSession.room_id === roomId) {
            console.log(`Continuing existing session: ${activeSession.session_id}`);
            return activeSession;
        } else {
            // End old session if it's too old or different room
            activeSession.status = 'ended';
            activeSession.end_time = now.toISOString();
            await saveSessionData(sessions);
        }
    }
    
    // Create new session if no valid active session found
    return await createNewSession(username, roomId);
}

// Update session with new gift data
async function updateSessionGiftData(session_id, giftData) {
    const sessions = await loadSessionData();
    const session = sessions.find(s => s.session_id === session_id);
    if (!session) return;

    const { sender, diamonds } = giftData;
    
    // Update gift data
    session.gift_data[sender] = (session.gift_data[sender] || 0) + diamonds;
    
    // Recalculate top 10
    session.top_10 = Object.entries(session.gift_data)
        .map(([username, diamond]) => ({ username, diamond }))
        .sort((a, b) => b.diamond - a.diamond)
        .slice(0, 10);
    
    session.last_event_time = new Date().toISOString();
    
    // Log gift details
    console.log(`Gift in session ${session_id}:`, {
        sender,
        diamonds,
        totalDiamonds: session.gift_data[sender]
    });
    
    await saveSessionData(sessions);
    return session;
}

// End session
async function endSession(session_id) {
    const sessions = await loadSessionData();
    const session = sessions.find(s => s.session_id === session_id);
    if (!session) return;
    
    session.status = 'ended';
    session.end_time = new Date().toISOString();
    
    console.log(`Ending session ${session_id}. Final leaderboard:`, 
        session.top_10.map(t => `${t.username}: ${t.diamond}`).join(', '));
    
    // Export session data to CSV files
    await exportSessionToCSV(session);
    
    await saveSessionData(sessions);
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

    const resetLiveEndTimer = () => {
        if (liveEndTimer) clearTimeout(liveEndTimer);
        liveEndTimer = setTimeout(async () => {
            console.log(`No events for 3 minutes, ending session for ${username}`);
            if (currentSession) {
                await endSession(currentSession.session_id);
                currentSession = null;
            }
            tiktokConnection.disconnect();
        }, LIVE_END_TIMEOUT);
    };

    try {
        console.log(`Connecting to ${username}'s live stream...`);
        const state = await tiktokConnection.connect();
        
        currentSession = await getActiveSession(username, state.roomId);
        activeConnections.set(username, tiktokConnection);
        
        console.log(`Connected to ${username}'s live stream. Session: ${currentSession.session_id}`);
        reconnectAttempt = 0;
        
        // Event handlers
        tiktokConnection.on('gift', async (data) => {
            resetLiveEndTimer();
            await updateSessionGiftData(currentSession.session_id, {
                sender: data.nickname || data.uniqueId,
                diamonds: data.diamondCount
            });
        });

        tiktokConnection.on('chat', resetLiveEndTimer);
        
        tiktokConnection.on('member', resetLiveEndTimer);
        
        tiktokConnection.on('disconnect', async () => {
            if (liveEndTimer) clearTimeout(liveEndTimer);
            activeConnections.delete(username);
            
            if (currentSession) {
                await endSession(currentSession.session_id);
                currentSession = null;
            }
            
            const delay = RECONNECT_DELAYS[Math.min(reconnectAttempt, RECONNECT_DELAYS.length - 1)];
            console.log(`Disconnected from ${username}, reconnecting in ${delay/1000}s...`);
            
            setTimeout(() => {
                monitorAccount(username, reconnectAttempt + 1);
            }, delay);
        });

        resetLiveEndTimer();

    } catch (error) {
        console.error(`Error monitoring ${username}:`, error);
        activeConnections.delete(username);
        
        if (currentSession) {
            await endSession(currentSession.session_id);
            currentSession = null;
        }
        
        const delay = RECONNECT_DELAYS[Math.min(reconnectAttempt, RECONNECT_DELAYS.length - 1)];
        console.log(`Failed to connect to ${username}, retrying in ${delay/1000}s...`);
        
        setTimeout(() => {
            monitorAccount(username, reconnectAttempt + 1);
        }, delay);
    }
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

// Map to store next retry times for offline accounts
const offlineAccountRetryTimes = new Map();
const OFFLINE_RETRY_INTERVAL = 15 * 60 * 1000; // 15 minutes

// Start monitoring with status check
async function startMonitoringWithCheck() {
    try {
        const usernames = await loadUsernames();
        console.log('Checking live status for accounts:', usernames);
        
        // Check status for all accounts
        for (const username of usernames) {
            const status = await checkLiveStatus(username);
            
            if (status.isLive) {
                // Start monitoring immediately if live
                console.log(`[${username}] Account is live, starting monitoring`);
                await monitorAccount(username);
            } else {
                // Schedule retry for offline accounts
                console.log(`[${username}] Account is offline, will retry in 15 minutes`);
                offlineAccountRetryTimes.set(username, Date.now() + OFFLINE_RETRY_INTERVAL);
            }
        }

        // Start retry checker for offline accounts
        startOfflineRetryChecker();
    } catch (error) {
        console.error('Error in startMonitoringWithCheck:', error);
    }
}

// Periodic checker for offline accounts
function startOfflineRetryChecker() {
    setInterval(async () => {
        const now = Date.now();
        
        for (const [username, retryTime] of offlineAccountRetryTimes) {
            if (now >= retryTime) {
                console.log(`[${username}] Retrying status check`);
                const status = await checkLiveStatus(username);
                
                if (status.isLive) {
                    console.log(`[${username}] Now live, starting monitoring`);
                    offlineAccountRetryTimes.delete(username);
                    await monitorAccount(username);
                } else {
                    console.log(`[${username}] Still offline, scheduling next retry`);
                    offlineAccountRetryTimes.set(username, now + OFFLINE_RETRY_INTERVAL);
                }
            }
        }
    }, 60000); // Check every minute
}

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

// Start monitoring
async function startMonitoring() {
    try {
        const usernames = await loadUsernames();
        console.log('Starting monitoring for accounts:', usernames);
        
        for (const username of usernames) {
            await monitorAccount(username);
        }
    } catch (error) {
        console.error('Error starting monitoring:', error);
    }
}

// Helper: Check if CSV files exist and create if not
async function initializeCsvFiles() {
    try {
        // Initialize live data CSV with headers
        if (!fs.existsSync(DATA_LIVE_CSV)) {
            await writeCsvData(liveDataWriter, []);
            console.log('Created data_live.csv with headers');
        }
        
        // Initialize top spender CSV with headers
        if (!fs.existsSync(TOP_SPENDER_CSV)) {
            await writeCsvData(topSpenderWriter, []);
            console.log('Created top_spender.csv with headers');
        }
    } catch (error) {
        console.error('Error initializing CSV files:', error);
    }
}

// Helper: Calculate session duration in minutes
function calculateDuration(startTime, endTime) {
    return Math.round((new Date(endTime) - new Date(startTime)) / (1000 * 60));
}

// Helper: Calculate total diamonds from gift data
function calculateTotalDiamonds(giftData) {
    return Object.values(giftData).reduce((sum, diamonds) => sum + diamonds, 0);
}

// Helper: Read existing CSV data
async function readCsvFile(filePath) {
    try {
        const content = await fs.readFile(filePath, 'utf8');
        if (!content.trim()) {
            return [];
        }
        
        const lines = content.split('\n');
        const headers = lines[0].split(',');
        
        return lines
            .slice(1) // Skip header row
            .filter(line => line.trim()) // Skip empty lines
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

// Helper: Write CSV data using csv-writer
async function writeCsvData(writer, records) {
    try {
        // Filter out any undefined or invalid records
        const validRecords = records.filter(record => 
            record && typeof record === 'object' && Object.keys(record).length > 0
        );
        
        if (validRecords.length > 0) {
            await writer.writeRecords(validRecords);
        }
    } catch (error) {
        console.error('Error writing CSV data:', error);
    }
}

// Export session data to CSV files with update support
async function exportSessionToCSV(session) {
    const kode_sesi = `${session.username}__${session.start_time}`;
    const now = new Date();
    
    // Prepare live data record
    const liveDataRecord = {
        kode_sesi,
        tgl_jam_mulai: session.start_time,
        akun: session.username,
        durasi: calculateDuration(session.start_time, session.end_time || now.toISOString()),
        peak_viewer: session.viewer_peak || 0,
        total_diamond: calculateTotalDiamonds(session.gift_data)
    };
    
    // Read existing live data
    let liveData = await readCsvFile(DATA_LIVE_CSV);
    
    // Update or add live data
    const existingIndex = liveData.findIndex(row => row.kode_sesi === kode_sesi);
    if (existingIndex >= 0) {
        liveData[existingIndex] = liveDataRecord;
    } else {
        liveData.push(liveDataRecord);
    }
    
    // Write updated live data
    await writeCsvData(liveDataWriter, liveData);
    
    // Prepare top spender record
    const topSpenderRecord = {
        kode_sesi,
        tgl_jam_mulai: session.start_time,
        akun: session.username
    };
    
    // Format top 10 spenders
    const top10 = session.top_10 || [];
    for (let i = 0; i < 10; i++) {
        const spender = top10[i];
        topSpenderRecord[`top${i + 1}`] = spender ? 
            `${spender.username}:${spender.diamond}` : 
            '';
    }
    
    // Read existing top spender data
    let topSpenderData = await readCsvFile(TOP_SPENDER_CSV);
    
    // Update or add top spender data
    const existingTopIndex = topSpenderData.findIndex(row => row.kode_sesi === kode_sesi);
    if (existingTopIndex >= 0) {
        topSpenderData[existingTopIndex] = topSpenderRecord;
    } else {
        topSpenderData.push(topSpenderRecord);
    }
    
    // Write updated top spender data
    await writeCsvData(topSpenderWriter, topSpenderData);
    
    console.log(`Updated CSV data for session ${kode_sesi}`);
}

// Auto-save active sessions periodically
async function autoSaveActiveSessions() {
    const sessions = await loadSessionData();
    const activeSessions = sessions.filter(s => s.status === 'active');
    
    if (activeSessions.length > 0) {
        for (const session of activeSessions) {
            await exportSessionToCSV(session);
        }
        console.log(`Auto-saved ${activeSessions.length} active sessions`);
    }
}

// Handle program termination
async function handleTermination() {
    console.log('\nReceived termination signal. Saving all active sessions...');
    
    const sessions = await loadSessionData();
    const activeSessions = sessions.filter(s => s.status === 'active');
    
    for (const session of activeSessions) {
        session.end_time = new Date().toISOString();
        session.status = 'ended';
        await exportSessionToCSV(session);
        console.log(`Saved and ended session: ${session.session_id}`);
    }
    
    await saveSessionData(sessions);
    console.log('All sessions saved. Shutting down...');
    process.exit(0);
}

// Setup auto-save and termination handlers
process.on('SIGINT', handleTermination);
process.on('SIGTERM', handleTermination);

// Initialize CSV files when server starts
server.listen(port, async () => {
    console.log(`Server running on port ${port}`);
    await initializeCsvFiles();
    
    // Start auto-save interval
    setInterval(autoSaveActiveSessions, AUTO_SAVE_INTERVAL);
});

// API endpoints for frontend

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
                return {
                    username,
                    ...status
                };
            })
        );
        res.json({ statuses });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/monitor/start', async (req, res) => {
    try {
        const usernames = await loadUsernames();
        
        // Check all accounts first
        const statuses = await Promise.all(
            usernames.map(async username => {
                const status = await checkLiveStatus(username);
                return {
                    username,
                    ...status
                };
            })
        );
        
        // Start monitoring only for live accounts
        const liveAccounts = statuses.filter(s => s.isLive).map(s => s.username);
        for (const username of liveAccounts) {
            await monitorAccount(username);
        }
        
        // Schedule checks for offline accounts
        const offlineAccounts = statuses.filter(s => !s.isLive).map(s => s.username);
        for (const username of offlineAccounts) {
            offlineAccountRetryTimes.set(username, Date.now() + OFFLINE_RETRY_INTERVAL);
        }
        
        // Start the offline checker if we have any offline accounts
        if (offlineAccounts.length > 0) {
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

app.post('/api/monitor/stop', async (req, res) => {
    try {
        // Clear retry timers
        offlineAccountRetryTimes.clear();
        
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

// API endpoints for CSV operations
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
