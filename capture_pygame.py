#!/usr/bin/env python3
"""
Headless Pygame frame capture for StreamDrop.

Runs a user's pygame script with SDL's dummy video driver (no display
required) and intercepts pygame.display.flip()/update() to capture the
rendered surface. A background thread writes JPEG frames to stdout at a
FIXED frame rate - duplicating the last frame when the game renders
slower or idles - so the consuming FFmpeg's timeline stays in sync with
wall-clock time:

    capture_pygame.py --fps 30 game.py | ffmpeg -f image2pipe -framerate 30 -i - ...

The wrapped script's own stdout is redirected to stderr so print()
calls can't corrupt the frame stream.
"""

import argparse
import io
import os
import runpy
import sys
import threading
import time

# Must be set before pygame is imported anywhere
os.environ.setdefault("SDL_VIDEODRIVER", "dummy")
os.environ.setdefault("SDL_AUDIODRIVER", "dummy")


def main():
    parser = argparse.ArgumentParser(description="Headless pygame to JPEG frame pipe")
    parser.add_argument("--fps", type=int, default=30, help="Output frame rate")
    parser.add_argument("--quality", type=int, default=85, help="JPEG quality (0-100)")
    parser.add_argument("script", help="Path to the pygame script to run")
    args = parser.parse_args()

    if not os.path.exists(args.script):
        print(f"[capture_pygame] Script not found: {args.script}", file=sys.stderr)
        sys.exit(1)

    frame_output = sys.stdout.buffer
    # The game script must not write to our frame pipe
    sys.stdout = sys.stderr

    import pygame
    from PIL import Image

    frame_interval = 1.0 / args.fps
    frame_lock = threading.Lock()
    state = {"frame": None, "last_capture": 0.0}

    def capture_frame():
        """Encode the current display surface as JPEG (rate-limited)."""
        now = time.monotonic()
        if now - state["last_capture"] < frame_interval:
            return
        surface = pygame.display.get_surface()
        if surface is None:
            return
        try:
            raw = pygame.image.tostring(surface, "RGB")
            image = Image.frombytes("RGB", surface.get_size(), raw)
            buffer = io.BytesIO()
            image.save(buffer, format="JPEG", quality=args.quality)
            with frame_lock:
                state["frame"] = buffer.getvalue()
            state["last_capture"] = now
        except Exception as e:
            print(f"[capture_pygame] Frame capture error: {e}", file=sys.stderr)

    def emitter():
        """Write the latest frame to stdout at a fixed rate, duplicating the
        last frame while the game isn't rendering new ones."""
        next_frame_time = time.monotonic()
        while True:
            with frame_lock:
                frame = state["frame"]
            if frame is not None:
                try:
                    frame_output.write(frame)
                    frame_output.flush()
                except (BrokenPipeError, OSError):
                    print("[capture_pygame] Output pipe closed, exiting", file=sys.stderr)
                    os._exit(0)
            next_frame_time += frame_interval
            sleep_for = next_frame_time - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                next_frame_time = time.monotonic()

    original_flip = pygame.display.flip
    original_update = pygame.display.update

    def patched_flip(*a, **kw):
        result = original_flip(*a, **kw)
        capture_frame()
        return result

    def patched_update(*a, **kw):
        result = original_update(*a, **kw)
        capture_frame()
        return result

    pygame.display.flip = patched_flip
    pygame.display.update = patched_update

    threading.Thread(target=emitter, daemon=True).start()

    script_path = os.path.abspath(args.script)
    sys.path.insert(0, os.path.dirname(script_path))
    sys.argv = [script_path]

    print(f"[capture_pygame] Running {script_path} at {args.fps} fps output", file=sys.stderr)
    try:
        runpy.run_path(script_path, run_name="__main__")
    except SystemExit:
        pass
    print("[capture_pygame] Script finished", file=sys.stderr)


if __name__ == "__main__":
    main()
