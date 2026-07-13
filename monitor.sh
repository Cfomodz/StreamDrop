#!/bin/bash
# StreamDrop Health Check Script

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== StreamDrop Health Check ==="
echo "Timestamp: $(date)"
echo

echo "Service Status:"
systemctl is-active streamdrop.service
echo

echo "Recent Errors (last 10 minutes):"
sudo journalctl -u streamdrop.service --since "10 minutes ago" --no-pager | grep -E "(ERROR|WARN|failed)" | tail -5
echo

echo "Stream Statuses:"
# /api/streams requires a logged-in session; a login redirect means the app is up
http_code=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:5000/api/streams 2>/dev/null)
if [ "$http_code" = "302" ]; then
    echo "Web interface is up (log in at http://localhost:5000 for stream details)"
elif [ "$http_code" = "200" ]; then
    curl -s http://localhost:5000/api/streams | jq -r '.[] | "\(.name): \(.status) (\(.uptime))"' 2>/dev/null
else
    echo "API not accessible (HTTP ${http_code:-no response})"
fi
echo

echo "Active FFmpeg Processes:"
ffmpeg_count=$(pgrep -c -f ffmpeg)
echo "Count: $ffmpeg_count"
if [ "$ffmpeg_count" -gt 0 ]; then
    echo "PIDs: $(pgrep -f ffmpeg | tr '\n' ' ')"
fi
echo

echo "Memory Usage (Top Python Processes):"
ps -o pid,ppid,cmd,%mem,%cpu --sort=-%mem -C python,python3 2>/dev/null | head -5 || echo "No Python processes found"
echo

echo "Disk Space:"
df -h "$SCRIPT_DIR" | tail -1
echo

echo "Recent Stream Events (last hour):"
sqlite3 "$SCRIPT_DIR/streams.db" "
SELECT datetime(timestamp, 'localtime') as time,
       substr(stream_id, 1, 8) as stream,
       event_type
FROM stream_analytics
WHERE timestamp > datetime('now', '-1 hour')
ORDER BY timestamp DESC
LIMIT 10;" 2>/dev/null || echo "Could not access database (is sqlite3 installed?)"

echo
echo "=== End Health Check ==="
