# BIG COMPANY BOT - Music Folder

## Required MP3 Files

This folder should contain the following Notorious B.I.G. tracks in MP3 format:

1. **big_poppa.mp3** - Plays first when opening the shop (required)
2. **notorious_thugs.mp3**
3. **juice.mp3**
4. **party_and_bullshit.mp3**
5. **hypnotize.mp3**

## How to Add Music Files

### Option 1: Download and Add Manually
1. Obtain legal MP3 files of the tracks listed above
2. Rename them exactly as shown above (lowercase, underscores for spaces)
3. Place them in this `webapp_fresh/music/` folder
4. Push to GitHub and redeploy to Render

### Option 2: Use YouTube Audio (Alternative)
If you prefer to stream from YouTube or another source:
1. Edit `webapp_fresh/app.html`
2. Find the `stations` array (around line 5553)
3. Replace the `src` URLs with your streaming URLs

Example:
```javascript
{ name: "♫ BIG POPPA", src: "https://your-url-here.com/big_poppa.mp3" }
```

## Music Player Features

- ⏮ **Previous Button**: Go to previous track
- ⏭ **Next Button**: Skip to next track
- 🎵 **Auto-play**: Plays "Big Poppa" automatically when shop opens
- 🔄 **Auto-advance**: Automatically plays next song when current song ends
- 📻 **Visualizer**: Animated bars show music is playing

## Legal Notice

Make sure you have the proper rights/licenses to use these music files in your application. Consider using:
- Royalty-free music
- Licensed streaming services
- Music with appropriate Creative Commons licenses

## File Specifications

- **Format**: MP3
- **Bitrate**: 128-320 kbps (recommended: 192 kbps)
- **Sample Rate**: 44.1 kHz
- **Channels**: Stereo
