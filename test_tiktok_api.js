const { WebcastPushConnection } = require('tiktok-live-connector');

// Replace with a TikTok username to test
const username = 'astrovibesssssss';

async function testTikTokAPI() {
    const tiktokConnection = new WebcastPushConnection(username);

    try {
        console.log(`Connecting to TikTok live stream for @${username}...`);

        // Connect to the live stream
        await tiktokConnection.connect();

        console.log('Connected successfully! Listening for raw data...');

        // Log raw data to inspect available fields
        tiktokConnection.on('rawData', (data) => {
            console.log('Raw data received:', JSON.stringify(data, null, 2));
        });

        // Handle disconnection
        tiktokConnection.on('streamEnd', () => {
            console.log('Stream ended.');
        });

    } catch (error) {
        console.error('Error connecting to TikTok live stream:', error);
    }
}

testTikTokAPI();
