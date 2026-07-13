#!/usr/bin/env python3
"""
Chrome DevTools Protocol screencast capture for StreamDrop.

Attaches to a headless Chrome/Chromium instance via its remote debugging
port, starts a Page.startScreencast session, and writes JPEG frames to
stdout at a fixed frame rate (duplicating the last frame when the page is
idle). Designed to be piped straight into FFmpeg:

    capture_html.py --port 9222 --fps 30 | ffmpeg -f image2pipe -framerate 30 -i - ...

All diagnostics go to stderr; stdout carries only JPEG bytes.
"""

import argparse
import base64
import json
import sys
import threading
import time

import requests
import websocket


def log(message):
    print(f"[capture_html] {message}", file=sys.stderr, flush=True)


def get_page_websocket_url(port, timeout=30):
    """Find the first page target's WebSocket debugger URL, waiting for Chrome to come up."""
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            response = requests.get(f"http://127.0.0.1:{port}/json/list", timeout=3)
            targets = response.json()
            for target in targets:
                if target.get("type") == "page" and target.get("webSocketDebuggerUrl"):
                    return target["webSocketDebuggerUrl"]
        except Exception as e:
            last_error = e
        time.sleep(0.5)
    raise RuntimeError(f"No debuggable page found on port {port}: {last_error}")


class ScreencastCapture:
    """Maintains the latest screencast frame from a CDP WebSocket connection."""

    def __init__(self, ws_url, width, height, quality=80):
        self.ws_url = ws_url
        self.width = width
        self.height = height
        self.quality = quality
        self.latest_frame = None
        self.frame_lock = threading.Lock()
        self.connected = threading.Event()
        self.closed = threading.Event()
        self._message_id = 0
        self._id_lock = threading.Lock()
        self.ws = None

    def _next_id(self):
        with self._id_lock:
            self._message_id += 1
            return self._message_id

    def _send(self, method, params=None):
        self.ws.send(json.dumps({
            "id": self._next_id(),
            "method": method,
            "params": params or {},
        }))

    def _on_open(self, ws):
        log("WebSocket connected, starting screencast")
        self._send("Page.enable")
        self._send("Page.startScreencast", {
            "format": "jpeg",
            "quality": self.quality,
            "maxWidth": self.width,
            "maxHeight": self.height,
            "everyNthFrame": 1,
        })
        self.connected.set()

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
        except ValueError:
            return
        if data.get("method") == "Page.screencastFrame":
            params = data.get("params", {})
            frame_data = params.get("data")
            session_id = params.get("sessionId")
            if frame_data:
                frame = base64.b64decode(frame_data)
                with self.frame_lock:
                    self.latest_frame = frame
            if session_id is not None:
                self._send("Page.screencastFrameAck", {"sessionId": session_id})

    def _on_error(self, ws, error):
        log(f"WebSocket error: {error}")

    def _on_close(self, ws, status_code, message):
        log("WebSocket closed")
        self.closed.set()

    def start(self):
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        # suppress_origin: Chrome rejects WebSocket upgrades that carry an
        # Origin header unless launched with --remote-allow-origins
        thread = threading.Thread(
            target=lambda: self.ws.run_forever(suppress_origin=True),
            daemon=True,
        )
        thread.start()

    def get_frame(self):
        with self.frame_lock:
            return self.latest_frame


def main():
    parser = argparse.ArgumentParser(description="CDP screencast to JPEG frame pipe")
    parser.add_argument("--port", type=int, required=True, help="Chrome remote debugging port")
    parser.add_argument("--fps", type=int, default=30, help="Output frame rate")
    parser.add_argument("--width", type=int, default=1280, help="Max frame width")
    parser.add_argument("--height", type=int, default=720, help="Max frame height")
    parser.add_argument("--quality", type=int, default=80, help="JPEG quality (0-100)")
    args = parser.parse_args()

    ws_url = get_page_websocket_url(args.port)
    log(f"Attaching to {ws_url}")

    capture = ScreencastCapture(ws_url, args.width, args.height, args.quality)
    capture.start()

    if not capture.connected.wait(timeout=15):
        log("Timed out waiting for WebSocket connection")
        sys.exit(1)

    # Wait for the first frame before starting the fixed-rate output loop
    deadline = time.time() + 30
    while capture.get_frame() is None:
        if time.time() > deadline or capture.closed.is_set():
            log("No screencast frames received")
            sys.exit(1)
        time.sleep(0.1)

    log(f"Streaming frames at {args.fps} fps")
    out = sys.stdout.buffer
    frame_interval = 1.0 / args.fps
    next_frame_time = time.monotonic()
    frames_written = 0

    while not capture.closed.is_set():
        frame = capture.get_frame()
        try:
            out.write(frame)
            out.flush()
        except (BrokenPipeError, OSError):
            log("Output pipe closed, exiting")
            break

        frames_written += 1
        if frames_written % (args.fps * 60) == 0:
            log(f"{frames_written} frames written")

        next_frame_time += frame_interval
        sleep_for = next_frame_time - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            # Fell behind; reset the schedule instead of bursting
            next_frame_time = time.monotonic()


if __name__ == "__main__":
    main()
