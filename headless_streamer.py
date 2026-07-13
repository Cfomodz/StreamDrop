#!/usr/bin/env python3
"""
Optimized Headless Streamer - NO X11 REQUIRED
True headless streaming using direct frame capture:
- HTML: Chrome DevTools Protocol screencast (via capture_html.py)
- Pygame: SDL dummy-driver surface capture (via capture_pygame.py)

Both capture helpers emit JPEG frames on stdout, which are piped straight
into FFmpeg for encoding and RTMP delivery.
"""

import os
import sys
import time
import shutil
import signal
import logging
import subprocess

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def find_browser():
    """Locate an installed Chromium/Chrome binary."""
    override = os.environ.get('STREAMDROP_BROWSER')
    if override and (os.path.exists(override) or shutil.which(override)):
        return override
    for candidate in ('chromium-browser', 'chromium', 'google-chrome',
                      'google-chrome-stable', 'chrome'):
        path = shutil.which(candidate)
        if path:
            return path
    return None


class PipelineStreamer:
    """Base class: a frame-capture subprocess piped into FFmpeg."""

    def __init__(self, stream_key, rtmp_base='rtmp://a.rtmp.youtube.com/live2/'):
        self.stream_key = stream_key
        self.rtmp_url = f'{rtmp_base}{stream_key}'
        self.capture_process = None
        self.ffmpeg_process = None
        self.streaming = False

    def _start_ffmpeg(self, framerate=30, bitrate='2500k'):
        """Start FFmpeg reading JPEG frames from the capture process."""
        buf_kbits = int(bitrate.rstrip('k')) * 2
        ffmpeg_cmd = [
            'ffmpeg',
            '-f', 'image2pipe',
            '-framerate', str(framerate),
            '-i', '-',
            '-f', 'lavfi',
            '-i', 'anullsrc=channel_layout=stereo:sample_rate=44100',
            '-c:v', 'libx264',
            '-preset', 'veryfast',
            '-b:v', bitrate,
            '-maxrate', bitrate,
            '-bufsize', f'{buf_kbits}k',
            '-pix_fmt', 'yuv420p',
            '-g', str(framerate * 2),
            '-r', str(framerate),
            '-c:a', 'aac',
            '-b:a', '128k',
            '-shortest',
            '-f', 'flv',
            self.rtmp_url,
        ]

        logger.info("Starting FFmpeg (frame pipe -> RTMP)...")
        self.ffmpeg_process = subprocess.Popen(
            ffmpeg_cmd,
            stdin=self.capture_process.stdout,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        # FFmpeg holds its own copy of the pipe now; drop ours so the capture
        # helper gets EPIPE if FFmpeg dies
        try:
            self.capture_process.stdout.close()
        except OSError:
            pass
        time.sleep(2)
        return self.ffmpeg_process.poll() is None

    def stop_streaming(self):
        """Stop all streaming processes"""
        self.streaming = False

        for process in (self.ffmpeg_process, self.capture_process):
            if process:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()

        logger.info("Headless streaming stopped")

    def is_running(self):
        return (self.streaming
                and self.capture_process and self.capture_process.poll() is None
                and self.ffmpeg_process and self.ffmpeg_process.poll() is None)


class HeadlessHTMLStreamer(PipelineStreamer):
    """True headless HTML streaming using the Chrome DevTools screencast API"""

    def __init__(self, stream_key, content_path="https://example.com"):
        super().__init__(stream_key)
        self.content_path = content_path
        self.chrome_process = None
        self.debug_port = 9222

    def start_chromium_headless(self):
        """Start Chromium in true headless mode with remote debugging"""
        browser = find_browser()
        if not browser:
            logger.error("No Chromium/Chrome browser found")
            return False

        # Detect available memory for optimization
        import psutil
        total_memory_mb = psutil.virtual_memory().total // (1024 * 1024)

        chromium_cmd = [
            browser,
            '--headless=new',
            '--disable-gpu',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',  # CRITICAL for low memory
            '--disable-extensions',
            '--disable-plugins',
            '--disable-background-timer-throttling',
            '--disable-backgrounding-occluded-windows',
            '--disable-renderer-backgrounding',
            '--disable-features=TranslateUI',
            '--disable-sync',
            '--disable-background-networking',
            '--disable-default-apps',
            '--disable-component-update',
            '--mute-audio',
            '--hide-scrollbars',
            f'--remote-debugging-port={self.debug_port}',
            '--remote-debugging-address=127.0.0.1',
            f'--remote-allow-origins=http://127.0.0.1:{self.debug_port}',
        ]

        # Add memory optimizations based on available RAM
        if total_memory_mb < 1024:
            chromium_cmd.extend([
                '--single-process',
                '--no-zygote',
                '--js-flags=--max-old-space-size=96',
                '--enable-low-end-device-mode',
                '--disable-site-isolation-trials',
                '--disable-features=site-per-process',
                '--window-size=854,480',  # Smaller window for less memory
            ])
            logger.info(f"Applied low-memory optimizations (system has {total_memory_mb}MB)")
        elif total_memory_mb < 2048:
            chromium_cmd.extend([
                '--js-flags=--max-old-space-size=256',
                '--window-size=1280,720',
            ])
        else:
            chromium_cmd.append('--window-size=1280,720')

        chromium_cmd.append(self.content_path)

        logger.info("Starting Chromium in optimized headless mode...")
        self.chrome_process = subprocess.Popen(
            chromium_cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # Wait for Chrome to be ready
        time.sleep(3)
        return self.chrome_process.poll() is None

    def start_streaming(self):
        """Start the complete headless streaming process"""
        if not self.start_chromium_headless():
            return False, "Failed to start Chromium"

        capture_cmd = [
            sys.executable, os.path.join(BASE_DIR, 'capture_html.py'),
            '--port', str(self.debug_port),
            '--fps', '30',
            '--width', '1280',
            '--height', '720',
        ]
        self.capture_process = subprocess.Popen(
            capture_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )

        if not self._start_ffmpeg(framerate=30, bitrate='2500k'):
            self.stop_streaming()
            return False, "Failed to start FFmpeg"

        self.streaming = True
        return True, "Headless streaming started successfully"

    def stop_streaming(self):
        super().stop_streaming()
        if self.chrome_process:
            self.chrome_process.terminate()
            try:
                self.chrome_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.chrome_process.kill()


class HeadlessPygameStreamer(PipelineStreamer):
    """True headless Pygame streaming - direct surface capture"""

    def __init__(self, stream_key, pygame_script="example_game.py"):
        super().__init__(stream_key)
        self.pygame_script = pygame_script

    def start_streaming(self):
        """Start headless Pygame streaming"""
        if not os.path.exists(self.pygame_script):
            return False, f"Pygame script not found: {self.pygame_script}"

        capture_cmd = [
            sys.executable, os.path.join(BASE_DIR, 'capture_pygame.py'),
            '--fps', '30',
            self.pygame_script,
        ]
        self.capture_process = subprocess.Popen(
            capture_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        time.sleep(2)
        if self.capture_process.poll() is not None:
            return False, "Failed to start pygame capture"

        if not self._start_ffmpeg(framerate=30, bitrate='3000k'):
            self.stop_streaming()
            return False, "Failed to start FFmpeg"

        self.streaming = True
        logger.info("Headless Pygame streaming started")
        return True, "Headless Pygame streaming started"


def main():
    """Main function for standalone headless streaming"""
    stream_key = os.environ.get('YOUTUBE_STREAM_KEY')
    if not stream_key:
        logger.error("YOUTUBE_STREAM_KEY environment variable required")
        sys.exit(1)

    content_path = os.environ.get('CONTENT_PATH')
    if not content_path:
        logger.error("CONTENT_PATH environment variable required")
        logger.error("Example: CONTENT_PATH='https://clock.zone' python3 headless_streamer.py")
        sys.exit(1)

    # Auto-detect mode from content path
    if content_path.endswith('.py'):
        streamer = HeadlessPygameStreamer(stream_key, content_path)
    else:
        streamer = HeadlessHTMLStreamer(stream_key, content_path)

    # Handle graceful shutdown
    def signal_handler(sig, frame):
        logger.info("Stopping headless streaming...")
        streamer.stop_streaming()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start streaming
    success, message = streamer.start_streaming()
    if success:
        logger.info(f"✅ {message}")
        logger.info("🚀 Headless streaming active - no GUI/X11 needed!")
        logger.info("💰 Perfect for cheap VPS instances")

        # Keep running while the pipeline is healthy
        try:
            while streamer.is_running():
                time.sleep(1)
            logger.error("Streaming pipeline exited")
            streamer.stop_streaming()
            sys.exit(1)
        except KeyboardInterrupt:
            streamer.stop_streaming()
    else:
        logger.error(f"❌ Failed to start: {message}")
        sys.exit(1)


if __name__ == "__main__":
    main()
