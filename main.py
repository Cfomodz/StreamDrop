#!/usr/bin/env python3
"""
24/7 HTML/Window Streaming Server for YouTube
Streams a browser window or HTML file continuously
"""

import os
import sys
import time
import logging
import signal
import subprocess
from pathlib import Path
from flask import Flask, render_template, request, jsonify

# Setup logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('html-streamer')

app = Flask(__name__)

class HTMLStreamer:
    def __init__(self, stream_key='', content_path=''):
        self.process = None
        self.display_process = None
        self.ffmpeg_process = None
        self.status = "stopped"
        self.stream_key = stream_key
        self.content_path = content_path
        
    def start_streaming(self):
        """Start streaming HTML content"""
        if not self.stream_key:
            return False, "YouTube stream key not configured"
            
        if not self.content_path:
            return False, "Content path not configured"
            
        if self.status == "running":
            return False, "Stream is already running"
            
        try:
            # Set up virtual display
            self.display_process = subprocess.Popen([
                'Xvfb', ':99', '-screen', '0', '1280x720x24', '-ac'
            ])
            
            # Allow Xvfb to start
            time.sleep(2)
            
            # Set display environment variable
            env = os.environ.copy()
            env['DISPLAY'] = ':99'
            
            # Launch Chrome in headless mode
            chrome_cmd = [
                'google-chrome', 
                '--headless', 
                '--disable-gpu',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--remote-debugging-port=9222',
                '--remote-debugging-address=0.0.0.0',
                self.content_path
            ]
            
            self.process = subprocess.Popen(
                chrome_cmd, 
                env=env,
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE
            )
            
            # Allow Chrome to start
            time.sleep(5)
            
            # Start FFmpeg to capture and stream
            ffmpeg_cmd = [
                'ffmpeg',
                '-f', 'x11grab',  # Capture X11 display
                '-video_size', '1280x720',
                '-framerate', '30',
                '-i', ':99',
                '-c:v', 'libx264',
                '-preset', 'veryfast',
                '-b:v', '2500k',
                '-maxrate', '2500k',
                '-bufsize', '5000k',
                '-pix_fmt', 'yuv420p',
                '-g', '60',
                '-f', 'flv',
                f'rtmp://a.rtmp.youtube.com/live2/{self.stream_key}'
            ]
            
            # Start FFmpeg process
            self.ffmpeg_process = subprocess.Popen(
                ffmpeg_cmd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            self.status = "running"
            logger.info("HTML streaming started")
            return True, "HTML streaming started successfully"
            
        except Exception as e:
            logger.error(f"Error starting stream: {e}")
            self.cleanup()
            return False, f"Error starting stream: {e}"
    
    def stop_streaming(self):
        """Stop the streaming processes"""
        self.cleanup()
        self.status = "stopped"
        logger.info("Streaming stopped")
        return True, "Streaming stopped"
    
    def cleanup(self):
        """Clean up all processes"""
        for proc in [self.process, self.ffmpeg_process, self.display_process]:
            if proc:
                try:
                    proc.terminate()
                    proc.wait(timeout=5)
                except:
                    try:
                        proc.kill()
                    except:
                        pass
                finally:
                    proc = None
    
    def get_status(self):
        """Get current streaming status"""
        return {
            "status": self.status,
            "stream_key_set": bool(self.stream_key),
            "content_path": self.content_path
        }

# Create global streamer instance
streamer = HTMLStreamer()

@app.route('/')
def index():
    """Main dashboard"""
    status = streamer.get_status()
    return render_template('index.html', status=status)

@app.route('/api/start', methods=['POST'])
def api_start():
    """API endpoint to start streaming"""
    success, message = streamer.start_streaming()
    return jsonify({"success": success, "message": message})

@app.route('/api/stop', methods=['POST'])
def api_stop():
    """API endpoint to stop streaming"""
    success, message = streamer.stop_streaming()
    return jsonify({"success": success, "message": message})

@app.route('/api/status', methods=['GET'])
def api_status():
    """API endpoint to get status"""
    return jsonify(streamer.get_status())

@app.route('/api/update_content', methods=['POST'])
def api_update_content():
    """API endpoint to update content path"""
    content_path = request.json.get('content_path')
    if content_path:
        streamer.content_path = content_path
        # Restart stream if running to apply changes
        if streamer.status == "running":
            streamer.stop_streaming()
            time.sleep(2)
            streamer.start_streaming()
        return jsonify({"success": True, "message": "Content path updated"})
    return jsonify({"success": False, "message": "No content path provided"})

def setup_environment():
    """Check and setup required environment"""
    # Check if required packages are available
    try:
        subprocess.run(['ffmpeg', '-version'], 
                      capture_output=True, check=True)
        logger.info("FFmpeg is available")
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.error("FFmpeg is not installed. Please install it with: sudo apt install ffmpeg")
        return False
        
    try:
        subprocess.run(['Xvfb', '-help'], 
                      capture_output=True, check=True)
        logger.info("Xvfb is available")
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.error("Xvfb is not installed. Please install it with: sudo apt install xvfb")
        return False
        
    try:
        subprocess.run(['google-chrome', '--version'], 
                      capture_output=True, check=True)
        logger.info("Google Chrome is available")
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.error("Google Chrome is not installed. Please install it")
        return False
        
    # Check if stream key is set
    if not os.environ.get('YOUTUBE_STREAM_KEY'):
        logger.warning("YOUTUBE_STREAM_KEY environment variable is not set")
        logger.info("You can set it with: export YOUTUBE_STREAM_KEY=your_key_here")
        
    return True

# Handle graceful shutdown
def signal_handler(sig, frame):
    logger.info("Shutting down...")
    streamer.cleanup()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == '__main__':
    # Check environment first
    if not setup_environment():
        sys.exit(1)
        
    # Run the web server
    app.run(host='0.0.0.0', port=5000, debug=True)