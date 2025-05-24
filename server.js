const express = require('express');
const { WebcastPushConnection } = require('tiktok-live-connector');
const fs = require('fs').promises;
const { existsSync } = require('fs');
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
const LIVE_END_TIMEOUT = 180000; // 3 minutes without events
const SESSION_CONTINUITY_THRESHOLD = 120000; // 2 minutes threshold for same session
const RECONNECT_DELAYS = [10000, 30000, 60000, 120000, 300000]; // 10s, 30s, 1m, 2m, 5m
const AUTO_SAVE_INTERVAL = 300000; // Auto-save interval (5 minutes)
const NOT_READY_CHECK_INTERVAL = 900000; // Check not-ready accounts every 15 minutes
const READY_RETRY_DELAY = 30000; // Retry ready accounts after 30 seconds
const MAX_READY_RETRIES = 3; // Maximum retries for ready accounts before moving to not-ready

// State management
const activeConnections = new Map();
const activeSessions = new Map();
const accountStatus = new Map();  // Tracks ready/not-ready status
const readyAccounts = new Set();  // Accounts that are currently live
const notReadyAccounts = new Set(); // Accounts that are not live
const retryAttempts = new Map(); // Track retry attempts per account
let isMonitoring = false;  // Global monitoring state

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
        gift_data: [], // Changed to array format
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

// Express setup
app.use(express.static('public'));
app.use(express.json());

// Serve main page
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Serve accounts.txt content
app.get('/accounts.txt', async (req, res) => {
    try {
        const content = await fs.readFile('accounts.txt', 'utf8');
        res.send(content);
    } catch (error) {
        res.status(500).send('Error reading accounts file');
    }
});

// Update accounts.txt content
app.post('/updateAccounts', async (req, res) => {
    try {
        await fs.writeFile('accounts.txt', req.body.content);
        res.send({ success: true });
    } catch (error) {
        res.status(500).send({ success: false, error: 'Error updating accounts file' });
    }
});

// Download CSV files
app.get('/download/csv', async (req, res) => {
    try {
        const archiver = require('archiver');
        const zip = archiver('zip');
        
        res.attachment('tiktok_data.zip');
        zip.pipe(res);
        
        zip.file(DATA_LIVE_CSV, { name: 'data_live.csv' });
        zip.file(TOP_SPENDER_CSV, { name: 'top_spender.csv' });
        
        await zip.finalize();
    } catch (error) {
        res.status(500).send('Error creating zip file');
    }
});

// Periodic check for not-ready accounts
let periodicCheckTimer = null;

async function startPeriodicCheck() {
    if (periodicCheckTimer) {
        clearInterval(periodicCheckTimer);
    }

    periodicCheckTimer = setInterval(async () => {
        console.log('Running periodic check for not-ready accounts...');
        const notReadyArray = Array.from(notReadyAccounts);
        
        for (const username of notReadyArray) {
            try {
                const isLive = await checkAccountStatus(username);
                if (isLive) {
                    console.log(`${username} is now LIVE!`);
                    if (isMonitoring) {
                        await monitorAccount(username);
                    }
                }
            } catch (error) {
                console.error(`Error checking status for ${username}:`, error);
            }
        }
        
        // Emit updated status to all clients
        io.emit('statusUpdate', Object.fromEntries([...accountStatus]));
    }, NOT_READY_CHECK_INTERVAL);
}

// Socket.IO event handlers
io.on('connection', (socket) => {
    console.log('Client connected');

    // Send initial status on connection
    socket.emit('statusUpdate', Object.fromEntries([...accountStatus]));

    // Check account status
    socket.on('checkStatus', async () => {
        console.log('Checking all accounts status...');
        readyAccounts.clear();
        notReadyAccounts.clear();
        accountStatus.clear();
        retryAttempts.clear();
        
        const usernames = await loadUsernames();
        console.log('Loaded usernames:', usernames);
        
        // Check all accounts in parallel with a slight delay between each
        for (const username of usernames) {
            try {
                await checkAccountStatus(username);
                // Small delay between checks to avoid rate limiting
                await new Promise(resolve => setTimeout(resolve, 500));
            } catch (error) {
                console.error(`Error checking status for ${username}:`, error);
            }
        }
        
        // Start periodic check for not-ready accounts
        startPeriodicCheck();
        
        // Send status update to all clients
        io.emit('statusUpdate', Object.fromEntries([...accountStatus]));
    });

    // Start monitoring
    socket.on('startMonitoring', async () => {
        if (isMonitoring) return;
        isMonitoring = true;
        console.log('Starting monitoring for ready accounts:', Array.from(readyAccounts));
        
        // Only monitor ready accounts
        for (const username of readyAccounts) {
            try {
                await monitorAccount(username);
                // Small delay between starts to avoid rate limiting
                await new Promise(resolve => setTimeout(resolve, 500));
            } catch (error) {
                console.error(`Error starting monitoring for ${username}:`, error);
            }
        }
    });

    // Stop monitoring
    socket.on('stopMonitoring', async () => {
        if (!isMonitoring) return;
        console.log('Stopping all monitoring...');
        isMonitoring = false;
        
        // Stop periodic check
        if (periodicCheckTimer) {
            clearInterval(periodicCheckTimer);
            periodicCheckTimer = null;
        }
        
        // Disconnect all active connections and save data
        for (const [username, connection] of activeConnections) {
            console.log(`Disconnecting ${username}...`);
            try {
                connection.disconnect();
            } catch (error) {
                console.error(`Error disconnecting ${username}:`, error);
            }
        }
        
        // Save all active sessions
        const sessions = await loadSessionData();
        const activeSessions = sessions.filter(s => s.status === 'active');
        
        for (const session of activeSessions) {
            try {
                session.end_time = new Date().toISOString();
                session.status = 'ended';
                await exportSessionToCSV(session);
                console.log(`Saved session for ${session.username}`);
            } catch (error) {
                console.error(`Error saving session for ${session.username}:`, error);
            }
        }
        
        await saveSessionData(sessions);
        
        // Clear maps
        activeConnections.clear();
        retryAttempts.clear();
        
        // Notify all clients
        io.emit('monitoringStopped');
    });

    socket.on('disconnect', () => {
        console.log('Client disconnected');
    });
});

// Rate limit tracking
const rateLimitTracker = {
    lastCheck: new Map(),
    dailyAttempts: new Map(),
    resetTime: null
};

// Rate limit checking
async function checkRateLimits(connection) {
    try {
        const limits = await connection.webClient.webSigner.webcast.getRateLimits();
        console.log('Current Rate Limits:', {
            remaining: limits.remaining,
            reset: new Date(limits.reset * 1000).toISOString(),
            limit: limits.limit
        });
        return limits;
    } catch (error) {
        console.error('Failed to fetch rate limits:', error);
        return null;
    }
}

// Enhanced check account status with rate limit checking
async function checkAccountStatus(username) {
    try {
        // Check daily limit
        const now = Date.now();
        if (!rateLimitTracker.resetTime || now > rateLimitTracker.resetTime) {
            // Reset daily tracking after 24 hours
            rateLimitTracker.dailyAttempts.clear();
            rateLimitTracker.resetTime = now + (24 * 60 * 60 * 1000);
        }

        // Check if we need to wait between requests
        const lastCheckTime = rateLimitTracker.lastCheck.get(username) || 0;
        const timeSinceLastCheck = now - lastCheckTime;
        if (timeSinceLastCheck < 5000) { // Minimum 5 seconds between checks
            await new Promise(resolve => setTimeout(resolve, 5000 - timeSinceLastCheck));
        }

        console.log(`Checking status for ${username}...`);
        const connection = new WebcastPushConnection(username, {
            requestPollingIntervalMs: 1000,
            requestOptions: { 
                timeout: 10000,
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
                }
            }
        });

        // Check rate limits before connecting
        const limits = await checkRateLimits(connection);
        if (limits && limits.remaining <= 0) {
            const waitTime = (limits.reset * 1000) - Date.now();
            console.log(`Rate limit exceeded. Waiting ${Math.ceil(waitTime/1000)} seconds until reset`);
            await new Promise(resolve => setTimeout(resolve, waitTime + 1000));
        }
        
        const state = await connection.connect();
        console.log(`${username} is LIVE with roomId: ${state.roomId}`);
        
        // Check rate limits after connecting to see remaining quota
        await checkRateLimits(connection);
        
        connection.disconnect();
        
        // Update tracking
        rateLimitTracker.lastCheck.set(username, now);
        readyAccounts.add(username);
        notReadyAccounts.delete(username);
        retryAttempts.delete(username);
        
        accountStatus.set(username, { 
            status: 'ready',
            roomId: state.roomId,
            lastChecked: new Date().toISOString(),
            rateLimits: limits
        });
        
        return true;
    } catch (error) {
        console.log(`${username} is NOT LIVE:`, error.message);
        
        // Handle rate limit errors
        if (error.message.includes('rate limit') || error.retryAfter) {
            const retryAfter = error.retryAfter || 60000; // Default to 60 seconds
            console.log(`Rate limited for ${username}, retry after ${retryAfter}ms`);
            rateLimitTracker.lastCheck.set(username, Date.now() + retryAfter);
        }
        
        notReadyAccounts.add(username);
        readyAccounts.delete(username);
        
        accountStatus.set(username, { 
            status: 'not-ready',
            lastChecked: new Date().toISOString(),
            error: error.message,
            nextCheckAllowed: new Date(rateLimitTracker.lastCheck.get(username)).toISOString()
        });
        
        return false;
    }
}

// Update gift data with gift name
async function updateSessionGiftData(session_id, giftData) {
    const sessions = await loadSessionData();
    const session = sessions.find(s => s.session_id === session_id);
    if (!session) return;

    const { sender, diamonds, giftId, giftName } = giftData;
    
    // Update gift data directly with complete details
    if (!session.gift_data) session.gift_data = [];
    session.gift_data.unshift({
        sender,
        diamonds,
        giftId,
        giftName,
        timestamp: new Date().toISOString(),
        count: 1
    });
    
    // Combine identical gifts that were sent close together
    for (let i = 1; i < session.gift_data.length; i++) {
        const prevGift = session.gift_data[i];
        if (prevGift.sender === sender && 
            prevGift.giftId === giftId &&
            (new Date(prevGift.timestamp) - new Date(session.gift_data[0].timestamp)) < 5000) {
            session.gift_data[0].count += prevGift.count;
            session.gift_data.splice(i, 1);
            i--;
        }
    }
    
    session.gift_data = session.gift_data.slice(0, 10); // Keep last 10 gifts
    
    // Calculate top 10 from gift data
    const giftTotals = session.gift_data.reduce((acc, gift) => {
        acc[gift.sender] = (acc[gift.sender] || 0) + gift.diamonds;
        return acc;
    }, {});
    
    session.top_10 = Object.entries(giftTotals)
        .map(([username, diamond]) => ({ username, diamond }))
        .sort((a, b) => b.diamond - a.diamond)
        .slice(0, 10);
    
    session.last_event_time = new Date().toISOString();
    
    // Emit session update to all connected clients
    io.emit('sessionUpdate', {
        [session.username]: {
            viewerCount: session.viewer_count || 0,
            peakViewer: session.viewer_peak || 0,
            recentGifts: session.gift_details,
            topSpenders: session.top_10
        }
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

// Monitor account with improved retry logic and session management
async function monitorAccount(username, reconnectAttempt = 0) {
    if (activeConnections.has(username)) {
        console.log(`Already monitoring ${username}`);
        return;
    }

    // Only monitor ready accounts
    if (!readyAccounts.has(username)) {
        console.log(`Skipping ${username} - not in ready state`);
        return;
    }

    const attempts = retryAttempts.get(username) || 0;
    retryAttempts.set(username, attempts + 1);

    const tiktokConnection = new WebcastPushConnection(username, {
        requestPollingIntervalMs: 2000,
        requestOptions: {
            timeout: 5000,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
            }
        },
        processInitialData: false
    });
    
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
        retryAttempts.set(username, 0); // Reset retry counter on successful connection
        
        console.log(`Connected to ${username}'s live stream. Session: ${currentSession.session_id}`);
        
        // Event handlers
        tiktokConnection.on('gift', async (data) => {
            resetLiveEndTimer();
            await updateSessionGiftData(currentSession.session_id, {
                sender: data.nickname || data.uniqueId,
                diamonds: data.diamondCount,
                giftId: data.giftId,
                giftName: data.giftName || 'Unknown Gift'
            });
        });

        tiktokConnection.on('chat', resetLiveEndTimer);
        tiktokConnection.on('member', resetLiveEndTimer);
        
        tiktokConnection.on('roomUser', async (data) => {
            resetLiveEndTimer();
            if (currentSession) {
                const viewerCount = data.viewerCount || 0;
                
                // Update in-memory session
                currentSession.viewer_count = viewerCount;
                currentSession.viewer_peak = Math.max(currentSession.viewer_peak || 0, viewerCount);
                
                // Update stored session data
                const sessions = await loadSessionData();
                const storedSession = sessions.find(s => s.session_id === currentSession.session_id);
                if (storedSession) {
                    storedSession.viewer_count = viewerCount;
                    storedSession.viewer_peak = Math.max(storedSession.viewer_peak || 0, viewerCount);
                    storedSession.last_event_time = new Date().toISOString();
                    await saveSessionData(sessions);
                }
                
                // Send update to frontend
                io.emit('sessionUpdate', {
                    [username]: {
                        viewerCount: viewerCount,
                        peakViewer: currentSession.viewer_peak,
                        recentGifts: currentSession.gift_details || [],
                        topSpenders: currentSession.top_10 || []
                    }
                });
            }
        });
        
        tiktokConnection.on('disconnect', async () => {
            if (liveEndTimer) clearTimeout(liveEndTimer);
            activeConnections.delete(username);
            
            if (currentSession) {
                await endSession(currentSession.session_id);
                currentSession = null;
            }
            
            const attempts = retryAttempts.get(username) || 0;
            
            if (attempts < MAX_READY_RETRIES) {
                console.log(`Disconnected from ${username}, retry ${attempts + 1}/${MAX_READY_RETRIES} in ${READY_RETRY_DELAY/1000}s...`);
                setTimeout(() => {
                    monitorAccount(username);
                }, READY_RETRY_DELAY);
            } else {
                console.log(`${username} failed ${MAX_READY_RETRIES} times, moving to not-ready state`);
                readyAccounts.delete(username);
                notReadyAccounts.add(username);
                accountStatus.set(username, { 
                    status: 'not-ready',
                    lastChecked: new Date().toISOString(),
                    error: 'Maximum retry attempts reached'
                });
                io.emit('statusUpdate', Object.fromEntries([...accountStatus]));
            }
        });

        resetLiveEndTimer();

    } catch (error) {
        console.error(`Error monitoring ${username}:`, error);
        activeConnections.delete(username);
        
        if (currentSession) {
            await endSession(currentSession.session_id);
            currentSession = null;
        }
        
        const attempts = retryAttempts.get(username) || 0;
        
        if (attempts < MAX_READY_RETRIES) {
            console.log(`Failed to connect to ${username}, retry ${attempts + 1}/${MAX_READY_RETRIES} in ${READY_RETRY_DELAY/1000}s...`);
            setTimeout(() => {
                monitorAccount(username);
            }, READY_RETRY_DELAY);
        } else {
            console.log(`${username} failed ${MAX_READY_RETRIES} times, moving to not-ready state`);
            readyAccounts.delete(username);
            notReadyAccounts.add(username);
            accountStatus.set(username, {
                status: 'not-ready',
                lastChecked: new Date().toISOString(),
                error: 'Maximum retry attempts reached'
            });
            io.emit('statusUpdate', Object.fromEntries([...accountStatus]));
        }
    }
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
        if (!existsSync(DATA_LIVE_CSV)) {
            await writeCsvData(liveDataWriter, []);
            console.log('Created data_live.csv with headers');
        }
        
        // Initialize top spender CSV with headers
        if (!existsSync(TOP_SPENDER_CSV)) {
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
    
    startMonitoring();
});
