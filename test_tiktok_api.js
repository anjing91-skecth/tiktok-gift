import { TikTokLiveConnection, WebcastEvent } from 'tiktok-live-connector';

// Replace with a TikTok username to test
const username = 'astrovibesssssss';

async function testTikTokAPI() {
    const tiktokConnection = new TikTokLiveConnection(username);

    try {
        console.log(`Connecting to TikTok live stream for @${username}...`);

        // Connect to the live stream
        const state = await tiktokConnection.connect();
        console.log(`Connected successfully! Room ID: ${state.roomId}`);

        // Log chat messages
        tiktokConnection.on(WebcastEvent.CHAT, (data) => {
            console.log('Chat message received:', data);
        });

        // Log gifts
        tiktokConnection.on(WebcastEvent.GIFT, (data) => {
            console.log('Gift received:', data);
        });

        // Log likes
        tiktokConnection.on(WebcastEvent.LIKE, (data) => {
            console.log('Like received:', data);
        });

        // Handle stream end
        tiktokConnection.on(WebcastEvent.STREAM_END, () => {
            console.log('Stream ended.');
        });

        // Handle errors
        tiktokConnection.on(WebcastEvent.ERROR, (error) => {
            console.error('Error occurred:', error);
        });

    } catch (error) {
        console.error('Error connecting to TikTok live stream:', error);
    }
}

testTikTokAPI();
