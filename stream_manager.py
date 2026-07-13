#!/usr/bin/env python3
"""
Enhanced Stream Manager for StreamDrop
Supports multiple concurrent streams with individual configurations
"""

import os
import sys
import json
import time
import uuid
import shutil
import hashlib
import logging
import signal
import socket
import sqlite3
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from threading import Thread, RLock
from functools import wraps
from flask import Flask, render_template, request, jsonify, Response, session, redirect, url_for, flash

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('stream-manager')

# Global lock for thread-safe operations. Reentrant: StreamManager methods
# that hold it (update_stream, delete_stream) call stop_stream/start_stream,
# which acquire it again on the same thread.
stream_lock = RLock()

# Directory containing this file (capture helper scripts live alongside it)
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

class StreamDatabase:
    """SQLite database manager for stream configurations"""
    
    def __init__(self, db_path="streams.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize database tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS streams (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                platform TEXT NOT NULL,
                stream_key TEXT NOT NULL,
                source TEXT NOT NULL,
                status TEXT DEFAULT 'stopped',
                quality TEXT DEFAULT 'medium',
                title TEXT DEFAULT '',
                description TEXT DEFAULT '',
                thumbnail TEXT DEFAULT '',
                rtmp_url TEXT DEFAULT '',
                custom_settings TEXT DEFAULT '',
                orientation TEXT DEFAULT 'auto',
                project_id TEXT DEFAULT NULL,
                audio_config TEXT DEFAULT '{}',
                schedule_config TEXT DEFAULT '{}',
                multi_stream_targets TEXT DEFAULT '[]',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                uptime_seconds INTEGER DEFAULT 0,
                start_time TIMESTAMP NULL,
                FOREIGN KEY (project_id) REFERENCES projects (id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stream_analytics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stream_id TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                event_type TEXT,
                data TEXT,
                FOREIGN KEY (stream_id) REFERENCES streams (id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stream_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stream_id TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fps REAL DEFAULT 0,
                bitrate REAL DEFAULT 0,
                frame_drops INTEGER DEFAULT 0,
                cpu_usage REAL DEFAULT 0,
                memory_usage REAL DEFAULT 0,
                bandwidth_mbps REAL DEFAULT 0,
                viewers INTEGER DEFAULT 0,
                duration_seconds INTEGER DEFAULT 0,
                FOREIGN KEY (stream_id) REFERENCES streams (id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stream_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stream_id TEXT,
                alert_type TEXT,
                severity TEXT,
                message TEXT,
                acknowledged BOOLEAN DEFAULT FALSE,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (stream_id) REFERENCES streams (id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stream_recovery (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stream_id TEXT,
                failure_type TEXT,
                recovery_strategy TEXT,
                retry_count INTEGER DEFAULT 0,
                max_retries INTEGER DEFAULT 5,
                last_attempt TIMESTAMP,
                success BOOLEAN DEFAULT FALSE,
                recovery_duration REAL DEFAULT 0,
                failure_reason TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (stream_id) REFERENCES streams (id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stream_health (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stream_id TEXT,
                health_score REAL DEFAULT 100,
                connection_quality REAL DEFAULT 100,
                performance_score REAL DEFAULT 100,
                stability_score REAL DEFAULT 100,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (stream_id) REFERENCES streams (id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS projects (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT DEFAULT '',
                settings TEXT DEFAULT '{}',
                audio_config TEXT DEFAULT '{}',
                schedule_config TEXT DEFAULT '{}',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stream_templates (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT DEFAULT '',
                template_config TEXT NOT NULL,
                category TEXT DEFAULT 'general',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS platform_configs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform_name TEXT NOT NULL,
                display_name TEXT NOT NULL,
                rtmp_url TEXT NOT NULL,
                supports_auth BOOLEAN DEFAULT FALSE,
                max_bitrate INTEGER DEFAULT 6000,
                recommended_settings TEXT DEFAULT '{}',
                active BOOLEAN DEFAULT TRUE
            )
        ''')
        
        conn.commit()
        conn.close()

        # Migrate older databases that predate the orientation column
        self._ensure_column('streams', 'orientation', "TEXT DEFAULT 'auto'")

        # Initialize platform configurations
        self.initialize_platform_configs()

    def _ensure_column(self, table, column, definition):
        """Add a column to an existing table if it's missing (simple migration)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f'PRAGMA table_info({table})')
        columns = [row[1] for row in cursor.fetchall()]
        if column not in columns:
            cursor.execute(f'ALTER TABLE {table} ADD COLUMN {column} {definition}')
            conn.commit()
        conn.close()

    @staticmethod
    def _parse_stream_row(row):
        """Convert a stream DB row to a dict with JSON fields decoded"""
        stream = dict(row)
        json_fields = {
            'custom_settings': {},
            'audio_config': {},
            'schedule_config': {},
            'multi_stream_targets': [],
        }
        for field, default in json_fields.items():
            value = stream.get(field)
            if isinstance(value, str) and value:
                try:
                    stream[field] = json.loads(value)
                except (ValueError, TypeError):
                    stream[field] = default
            elif not isinstance(value, (dict, list)):
                stream[field] = default
        return stream

    def create_stream(self, stream_data):
        """Create a new stream configuration"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        stream_id = str(uuid.uuid4())
        cursor.execute('''
            INSERT INTO streams (id, name, type, platform, stream_key, source,
                               quality, title, description, rtmp_url, custom_settings, orientation)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            stream_id,
            stream_data['name'],
            stream_data['type'],
            stream_data['platform'],
            stream_data['stream_key'],
            stream_data['source'],
            stream_data.get('quality', 'medium'),
            stream_data.get('title', ''),
            stream_data.get('description', ''),
            stream_data.get('rtmp_url', ''),
            json.dumps(stream_data.get('custom_settings', {})),
            stream_data.get('orientation', 'auto')
        ))

        conn.commit()
        conn.close()
        return stream_id

    def get_all_streams(self):
        """Get all stream configurations"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM streams ORDER BY created_at DESC')
        streams = [self._parse_stream_row(row) for row in cursor.fetchall()]

        conn.close()
        return streams

    def get_stream(self, stream_id):
        """Get a specific stream configuration"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM streams WHERE id = ?', (stream_id,))
        row = cursor.fetchone()

        conn.close()

        if not row:
            return None

        return self._parse_stream_row(row)
    
    def update_stream_status(self, stream_id, status, start_time=None):
        """Update stream status"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if status == 'live' and start_time:
            cursor.execute('''
                UPDATE streams 
                SET status = ?, start_time = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (status, start_time, stream_id))
        elif status == 'stopped':
            # Calculate uptime when stopping
            cursor.execute('SELECT start_time, uptime_seconds FROM streams WHERE id = ?', (stream_id,))
            result = cursor.fetchone()
            if result and result[0]:
                start_time_db = datetime.fromisoformat(result[0])
                session_uptime = (datetime.now() - start_time_db).total_seconds()
                total_uptime = result[1] + session_uptime
                
                cursor.execute('''
                    UPDATE streams 
                    SET status = ?, start_time = NULL, uptime_seconds = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (status, int(total_uptime), stream_id))
            else:
                cursor.execute('''
                    UPDATE streams 
                    SET status = ?, start_time = NULL, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (status, stream_id))
        else:
            cursor.execute('''
                UPDATE streams 
                SET status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (status, stream_id))
        
        conn.commit()
        conn.close()
    
    def update_stream(self, stream_id, stream_data):
        """Update a stream configuration"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Build dynamic update query based on provided data
        update_fields = []
        values = []

        allowed_fields = ['name', 'title', 'description', 'quality', 'source',
                          'stream_key', 'rtmp_url', 'platform', 'type', 'orientation']
        json_fields = ['custom_settings', 'audio_config', 'schedule_config', 'multi_stream_targets']

        # Handle JSON-encoded fields (accept dict/list or a pre-encoded string)
        for field in json_fields:
            if field in stream_data:
                value = stream_data[field]
                if not isinstance(value, str):
                    value = json.dumps(value)
                update_fields.append(f'{field} = ?')
                values.append(value)
        for field in allowed_fields:
            if field in stream_data:
                update_fields.append(f'{field} = ?')
                values.append(stream_data[field])
        
        if not update_fields:
            conn.close()
            return False
        
        # Add updated_at timestamp
        update_fields.append('updated_at = CURRENT_TIMESTAMP')
        values.append(stream_id)
        
        query = f'UPDATE streams SET {", ".join(update_fields)} WHERE id = ?'
        cursor.execute(query, values)
        
        success = cursor.rowcount > 0
        conn.commit()
        conn.close()
        
        return success
    
    def delete_stream(self, stream_id):
        """Delete a stream configuration"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM streams WHERE id = ?', (stream_id,))
        cursor.execute('DELETE FROM stream_analytics WHERE stream_id = ?', (stream_id,))
        
        conn.commit()
        conn.close()
    
    def log_event(self, stream_id, event_type, data=None):
        """Log stream analytics event"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO stream_analytics (stream_id, event_type, data)
            VALUES (?, ?, ?)
        ''', (stream_id, event_type, json.dumps(data) if data else None))
        
        conn.commit()
        conn.close()
    
    def log_metrics(self, stream_id, metrics):
        """Log stream performance metrics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO stream_metrics 
            (stream_id, fps, bitrate, frame_drops, cpu_usage, memory_usage, 
             bandwidth_mbps, viewers, duration_seconds)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            stream_id,
            metrics.get('fps', 0),
            metrics.get('bitrate', 0),
            metrics.get('frame_drops', 0),
            metrics.get('cpu_usage', 0),
            metrics.get('memory_usage', 0),
            metrics.get('bandwidth_mbps', 0),
            metrics.get('viewers', 0),
            metrics.get('duration_seconds', 0)
        ))
        
        conn.commit()
        conn.close()
    
    def create_alert(self, stream_id, alert_type, severity, message):
        """Create a stream alert"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO stream_alerts (stream_id, alert_type, severity, message)
            VALUES (?, ?, ?, ?)
        ''', (stream_id, alert_type, severity, message))
        
        conn.commit()
        conn.close()
    
    def get_recent_metrics(self, stream_id, minutes=30):
        """Get recent metrics for a stream"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM stream_metrics 
            WHERE stream_id = ? AND timestamp > datetime('now', '-{} minutes')
            ORDER BY timestamp DESC
        '''.format(minutes), (stream_id,))
        
        metrics = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return metrics
    
    def get_stream_alerts(self, stream_id=None, acknowledged=False):
        """Get stream alerts"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        if stream_id:
            cursor.execute('''
                SELECT * FROM stream_alerts 
                WHERE stream_id = ? AND acknowledged = ?
                ORDER BY timestamp DESC
            ''', (stream_id, acknowledged))
        else:
            cursor.execute('''
                SELECT * FROM stream_alerts 
                WHERE acknowledged = ?
                ORDER BY timestamp DESC
            ''', (acknowledged,))
        
        alerts = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return alerts
    
    def create_recovery_attempt(self, stream_id, failure_type, recovery_strategy, failure_reason=""):
        """Create a new recovery attempt record"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO stream_recovery (stream_id, failure_type, recovery_strategy, failure_reason)
            VALUES (?, ?, ?, ?)
        ''', (stream_id, failure_type, recovery_strategy, failure_reason))
        
        recovery_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return recovery_id
    
    def update_recovery_attempt(self, recovery_id, retry_count, success, recovery_duration=0):
        """Update a recovery attempt with results"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE stream_recovery 
            SET retry_count = ?, success = ?, recovery_duration = ?, last_attempt = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (retry_count, success, recovery_duration, recovery_id))
        
        conn.commit()
        conn.close()
    
    def get_active_recovery(self, stream_id):
        """Get active recovery attempt for a stream"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM stream_recovery 
            WHERE stream_id = ? AND success = FALSE 
            ORDER BY created_at DESC LIMIT 1
        ''', (stream_id,))
        
        recovery = cursor.fetchone()
        conn.close()
        return dict(recovery) if recovery else None
    
    def log_health_score(self, stream_id, health_score, connection_quality=100, performance_score=100, stability_score=100):
        """Log stream health metrics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO stream_health (stream_id, health_score, connection_quality, performance_score, stability_score)
            VALUES (?, ?, ?, ?, ?)
        ''', (stream_id, health_score, connection_quality, performance_score, stability_score))
        
        conn.commit()
        conn.close()
    
    def get_latest_health(self, stream_id):
        """Get latest health score for a stream"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM stream_health 
            WHERE stream_id = ? 
            ORDER BY timestamp DESC LIMIT 1
        ''', (stream_id,))
        
        health = cursor.fetchone()
        conn.close()
        return dict(health) if health else None
    
    def get_recovery_stats(self, stream_id=None):
        """Get recovery statistics"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        if stream_id:
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_attempts,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful_recoveries,
                    AVG(recovery_duration) as avg_recovery_time,
                    AVG(retry_count) as avg_retry_count
                FROM stream_recovery 
                WHERE stream_id = ?
            ''', (stream_id,))
        else:
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_attempts,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful_recoveries,
                    AVG(recovery_duration) as avg_recovery_time,
                    AVG(retry_count) as avg_retry_count
                FROM stream_recovery
            ''')
        
        stats = cursor.fetchone()
        conn.close()
        return dict(stats) if stats else {}
    
    def create_project(self, project_data):
        """Create a new project"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        project_id = str(uuid.uuid4())
        cursor.execute('''
            INSERT INTO projects (id, name, description, settings, audio_config, schedule_config)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            project_id,
            project_data['name'],
            project_data.get('description', ''),
            json.dumps(project_data.get('settings', {})),
            json.dumps(project_data.get('audio_config', {})),
            json.dumps(project_data.get('schedule_config', {}))
        ))
        
        conn.commit()
        conn.close()
        return project_id
    
    def get_all_projects(self):
        """Get all projects"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM projects ORDER BY name')
        projects = []
        for row in cursor.fetchall():
            project = dict(row)
            # Parse JSON fields
            for field in ['settings', 'audio_config', 'schedule_config']:
                if project.get(field):
                    try:
                        project[field] = json.loads(project[field])
                    except:
                        project[field] = {}
                else:
                    project[field] = {}
            projects.append(project)
        
        conn.close()
        return projects
    
    def get_project(self, project_id):
        """Get a specific project"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM projects WHERE id = ?', (project_id,))
        row = cursor.fetchone()
        
        conn.close()
        
        if not row:
            return None
            
        project = dict(row)
        # Parse JSON fields
        for field in ['settings', 'audio_config', 'schedule_config']:
            if project.get(field):
                try:
                    project[field] = json.loads(project[field])
                except:
                    project[field] = {}
            else:
                project[field] = {}
                
        return project
    
    def update_project(self, project_id, project_data):
        """Update a project"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        update_fields = []
        values = []
        
        allowed_fields = ['name', 'description']
        for field in allowed_fields:
            if field in project_data:
                update_fields.append(f'{field} = ?')
                values.append(project_data[field])
        
        # Handle JSON fields
        for field in ['settings', 'audio_config', 'schedule_config']:
            if field in project_data:
                update_fields.append(f'{field} = ?')
                values.append(json.dumps(project_data[field]))
        
        if not update_fields:
            conn.close()
            return False
        
        update_fields.append('updated_at = CURRENT_TIMESTAMP')
        values.append(project_id)
        
        query = f'UPDATE projects SET {", ".join(update_fields)} WHERE id = ?'
        cursor.execute(query, values)
        
        success = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return success
    
    def delete_project(self, project_id):
        """Delete a project and update associated streams"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Update streams to remove project association
        cursor.execute('UPDATE streams SET project_id = NULL WHERE project_id = ?', (project_id,))
        
        # Delete project
        cursor.execute('DELETE FROM projects WHERE id = ?', (project_id,))
        
        conn.commit()
        conn.close()
    
    def create_template(self, template_data):
        """Create a stream template"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        template_id = str(uuid.uuid4())
        cursor.execute('''
            INSERT INTO stream_templates (id, name, description, template_config, category)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            template_id,
            template_data['name'],
            template_data.get('description', ''),
            json.dumps(template_data['template_config']),
            template_data.get('category', 'general')
        ))
        
        conn.commit()
        conn.close()
        return template_id
    
    def get_all_templates(self):
        """Get all stream templates"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM stream_templates ORDER BY category, name')
        templates = []
        for row in cursor.fetchall():
            template = dict(row)
            # Parse template_config JSON
            if template.get('template_config'):
                try:
                    template['template_config'] = json.loads(template['template_config'])
                except:
                    template['template_config'] = {}
            else:
                template['template_config'] = {}
            templates.append(template)
        
        conn.close()
        return templates
    
    def initialize_platform_configs(self):
        """Initialize default platform configurations"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check if platforms are already initialized
        cursor.execute('SELECT COUNT(*) FROM platform_configs')
        if cursor.fetchone()[0] > 0:
            conn.close()
            return
        
        # Default platform configurations
        platforms = [
            {
                'platform_name': 'youtube',
                'display_name': 'YouTube Live',
                'rtmp_url': 'rtmp://a.rtmp.youtube.com/live2/',
                'supports_auth': True,
                'max_bitrate': 9000,
                'recommended_settings': json.dumps({
                    'resolution': '1920x1080',
                    'framerate': 60,
                    'keyframe_interval': 2
                })
            },
            {
                'platform_name': 'twitch',
                'display_name': 'Twitch',
                'rtmp_url': 'rtmp://live.twitch.tv/live/',
                'supports_auth': True,
                'max_bitrate': 6000,
                'recommended_settings': json.dumps({
                    'resolution': '1920x1080',
                    'framerate': 60,
                    'keyframe_interval': 2
                })
            },
            {
                'platform_name': 'facebook',
                'display_name': 'Facebook Live',
                'rtmp_url': 'rtmps://live-api-s.facebook.com:443/rtmp/',
                'supports_auth': True,
                'max_bitrate': 4000,
                'recommended_settings': json.dumps({
                    'resolution': '1280x720',
                    'framerate': 30,
                    'keyframe_interval': 2
                })
            },
            {
                'platform_name': 'linkedin',
                'display_name': 'LinkedIn Live',
                'rtmp_url': 'rtmps://1-46c2-477-4480.live-video.net/live/',
                'supports_auth': True,
                'max_bitrate': 5000,
                'recommended_settings': json.dumps({
                    'resolution': '1920x1080',
                    'framerate': 30,
                    'keyframe_interval': 2
                })
            },
            {
                'platform_name': 'instagram',
                'display_name': 'Instagram Live',
                'rtmp_url': 'rtmps://live-upload.instagram.com/rtmp/',
                'supports_auth': True,
                'max_bitrate': 3500,
                'recommended_settings': json.dumps({
                    'resolution': '1080x1920',
                    'framerate': 30,
                    'keyframe_interval': 2
                })
            },
            {
                'platform_name': 'tiktok',
                'display_name': 'TikTok Live',
                'rtmp_url': 'rtmp://push.tiktokcdn.com/live/',
                'supports_auth': True,
                'max_bitrate': 4000,
                'recommended_settings': json.dumps({
                    'resolution': '1080x1920',
                    'framerate': 30,
                    'keyframe_interval': 2
                })
            }
        ]
        
        for platform in platforms:
            cursor.execute('''
                INSERT INTO platform_configs 
                (platform_name, display_name, rtmp_url, supports_auth, max_bitrate, recommended_settings)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                platform['platform_name'],
                platform['display_name'],
                platform['rtmp_url'],
                platform['supports_auth'],
                platform['max_bitrate'],
                platform['recommended_settings']
            ))
        
        conn.commit()
        conn.close()
    
    def get_platform_configs(self):
        """Get all platform configurations"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM platform_configs WHERE active = 1 ORDER BY display_name')
        platforms = []
        for row in cursor.fetchall():
            platform = dict(row)
            # Parse recommended_settings JSON
            if platform.get('recommended_settings'):
                try:
                    platform['recommended_settings'] = json.loads(platform['recommended_settings'])
                except:
                    platform['recommended_settings'] = {}
            else:
                platform['recommended_settings'] = {}
            platforms.append(platform)

        conn.close()
        return platforms

    def get_template(self, template_id):
        """Get a specific stream template"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM stream_templates WHERE id = ?', (template_id,))
        row = cursor.fetchone()
        conn.close()

        if not row:
            return None

        template = dict(row)
        try:
            template['template_config'] = json.loads(template['template_config'] or '{}')
        except (ValueError, TypeError):
            template['template_config'] = {}
        return template

    def update_template(self, template_id, template_data):
        """Update a stream template"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        update_fields = []
        values = []

        for field in ['name', 'description', 'category']:
            if field in template_data:
                update_fields.append(f'{field} = ?')
                values.append(template_data[field])

        if 'template_config' in template_data:
            update_fields.append('template_config = ?')
            values.append(json.dumps(template_data['template_config']))

        if not update_fields:
            conn.close()
            return False

        values.append(template_id)
        cursor.execute(f'UPDATE stream_templates SET {", ".join(update_fields)} WHERE id = ?', values)

        success = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return success

    def delete_template(self, template_id):
        """Delete a stream template"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('DELETE FROM stream_templates WHERE id = ?', (template_id,))

        success = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return success

    def create_stream_from_template(self, template_id, stream_data):
        """Create a new stream from a template, with per-stream overrides"""
        template = self.get_template(template_id)
        if not template:
            return None

        merged = dict(template.get('template_config', {}))
        merged.update(stream_data or {})

        # A usable stream needs these at minimum
        required = ['name', 'type', 'platform', 'stream_key', 'source']
        if not all(merged.get(field) for field in required):
            return None

        return self.create_stream(merged)

    def create_platform_config(self, platform_data):
        """Create a new platform configuration"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        recommended = platform_data.get('recommended_settings', {})
        if not isinstance(recommended, str):
            recommended = json.dumps(recommended)

        cursor.execute('''
            INSERT INTO platform_configs
            (platform_name, display_name, rtmp_url, supports_auth, max_bitrate, recommended_settings)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            platform_data['platform_name'],
            platform_data.get('display_name', platform_data['platform_name']),
            platform_data['rtmp_url'],
            platform_data.get('supports_auth', False),
            platform_data.get('max_bitrate', 6000),
            recommended
        ))

        platform_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return platform_id

    def get_platform_config(self, platform_name):
        """Get a specific platform configuration"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM platform_configs WHERE platform_name = ?', (platform_name,))
        row = cursor.fetchone()
        conn.close()

        if not row:
            return None

        platform = dict(row)
        try:
            platform['recommended_settings'] = json.loads(platform['recommended_settings'] or '{}')
        except (ValueError, TypeError):
            platform['recommended_settings'] = {}
        return platform

    def update_platform_config(self, platform_name, platform_data):
        """Update a platform configuration"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        update_fields = []
        values = []

        for field in ['display_name', 'rtmp_url', 'supports_auth', 'max_bitrate', 'active']:
            if field in platform_data:
                update_fields.append(f'{field} = ?')
                values.append(platform_data[field])

        if 'recommended_settings' in platform_data:
            recommended = platform_data['recommended_settings']
            if not isinstance(recommended, str):
                recommended = json.dumps(recommended)
            update_fields.append('recommended_settings = ?')
            values.append(recommended)

        if not update_fields:
            conn.close()
            return False

        values.append(platform_name)
        cursor.execute(f'UPDATE platform_configs SET {", ".join(update_fields)} WHERE platform_name = ?', values)

        success = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return success

    def delete_platform_config(self, platform_name):
        """Delete a platform configuration"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('DELETE FROM platform_configs WHERE platform_name = ?', (platform_name,))

        success = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return success

    def get_project_streams(self, project_id):
        """Get all streams belonging to a project"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM streams WHERE project_id = ? ORDER BY created_at DESC', (project_id,))
        streams = [self._parse_stream_row(row) for row in cursor.fetchall()]

        conn.close()
        return streams

    def reset_stale_statuses(self):
        """Mark all streams stopped. Called at startup: no stream can be live
        before the manager has started any processes."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            UPDATE streams
            SET status = 'stopped', start_time = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE status != 'stopped'
        ''')

        reset_count = cursor.rowcount
        conn.commit()
        conn.close()

        if reset_count:
            logger.info(f"Reset {reset_count} stale stream status(es) to stopped")

class StreamInstance:
    """Individual stream instance with process management"""
    
    def __init__(self, stream_config, db):
        self.config = stream_config
        self.db = db
        self.processes = {}
        self.status = "stopped"
        self.metrics = {}
        self.last_metrics_time = time.time()
        self.frame_count = 0
        self.start_time = None
        self.health_score = 100.0
        self.failure_count = 0
        self.last_recovery_attempt = 0
        self.recovery_in_progress = False
        self.active_recovery_id = None

        # Derive a stable per-stream number for the X display / Chrome debug
        # port. Stream IDs are UUIDs, so hash them to a small integer instead
        # of assuming the last character is a digit.
        digest = hashlib.sha1(str(self.config['id']).encode()).hexdigest()
        self.instance_num = int(digest[:6], 16) % 500
        self.display_name = f":{100 + self.instance_num}"
        self.debug_port = 9300 + self.instance_num

        # Quality settings (horizontal presets)
        self.quality_presets = {
            'low': {'resolution': '854x480', 'bitrate': '1000k', 'framerate': '24'},
            'medium': {'resolution': '1280x720', 'bitrate': '2500k', 'framerate': '30'},
            'high': {'resolution': '1920x1080', 'bitrate': '4000k', 'framerate': '30'},
            'ultra': {'resolution': '1920x1080', 'bitrate': '6000k', 'framerate': '60'}
        }
        
        # Vertical quality presets for mobile platforms
        self.vertical_quality_presets = {
            'low': {'resolution': '480x854', 'bitrate': '1000k', 'framerate': '24'},
            'medium': {'resolution': '720x1280', 'bitrate': '2500k', 'framerate': '30'},
            'high': {'resolution': '1080x1920', 'bitrate': '4000k', 'framerate': '30'},
            'ultra': {'resolution': '1080x1920', 'bitrate': '6000k', 'framerate': '60'}
        }
        
        # Platforms that prefer vertical orientation
        self.vertical_platforms = {'tiktok', 'instagram'}
        
        # Load platform configurations from database
        self.platform_configs = {}
        self._load_platform_configs()
        
        # Audio configuration (config values are decoded by StreamDatabase,
        # but guard against raw JSON strings from older code paths)
        audio_config = self.config.get('audio_config', {})
        if isinstance(audio_config, str):
            try:
                audio_config = json.loads(audio_config)
            except (ValueError, TypeError):
                audio_config = {}
        self.audio_config = audio_config if isinstance(audio_config, dict) else {}

        targets = self.config.get('multi_stream_targets', [])
        if isinstance(targets, str):
            try:
                targets = json.loads(targets)
            except (ValueError, TypeError):
                targets = []
        self.multi_stream_targets = targets if isinstance(targets, list) else []


    def start_streaming(self):
        """Start the streaming process"""
        if self.status == "live":
            return False, "Stream is already running"
        
        try:
            quality = self._get_effective_quality()

            # Use smart streaming approach - detects headless vs X11 automatically
            self._start_smart_streaming(quality)
            
            self.status = "live"
            self.start_time = time.time()
            start_time = datetime.now().isoformat()
            self.db.update_stream_status(self.config['id'], 'live', start_time)
            self.db.log_event(self.config['id'], 'stream_started')
            
            logger.info(f"Stream {self.config['name']} started successfully")
            return True, "Stream started successfully"
            
        except Exception as e:
            logger.error(f"Error starting stream {self.config['name']}: {e}")
            self.cleanup()
            return False, f"Error starting stream: {e}"
    
    def _get_effective_quality(self):
        """Resolve the stream's quality settings: custom values if configured,
        otherwise the preset matching the effective orientation"""
        if self.config.get('quality') == 'custom' and self.config.get('custom_settings'):
            custom = self.config['custom_settings']
            return {
                'resolution': custom.get('resolution', '1280x720'),
                'bitrate': str(custom.get('bitrate', '2500')).rstrip('k') + 'k',
                'framerate': str(custom.get('framerate', '30'))
            }

        # Choose quality preset based on orientation preference
        orientation = self.config.get('orientation', 'auto')
        platform = self.config.get('platform', '')

        if orientation == 'auto':
            use_vertical = platform in self.vertical_platforms
        elif orientation == 'vertical':
            use_vertical = True
        else:  # horizontal
            use_vertical = False

        presets = self.vertical_quality_presets if use_vertical else self.quality_presets
        return presets.get(self.config.get('quality', 'medium')) or presets['medium']

    def _start_smart_streaming(self, quality):
        """Start streaming using smart detection with progressive fallbacks"""
        try:
            # Detect if we're on a headless system
            is_headless = self._detect_headless_system()
            
            if is_headless:
                # Try headless streaming first
                try:
                    self._start_headless_streaming(quality)
                except Exception as e:
                    logger.error(f"Headless streaming failed: {e}")
                    # Kill any partially-started pipeline before falling back,
                    # or an orphaned FFmpeg keeps fighting for the RTMP endpoint
                    self.cleanup()
                    # Fallback to simple test pattern streaming for low-memory VPS
                    logger.info("Falling back to test pattern streaming for low-memory system...")
                    self._start_test_pattern_streaming(quality)
            else:
                # Use traditional X11 approach with Xvfb
                try:
                    self._start_x11_streaming(quality)
                except Exception as e:
                    logger.error(f"X11 streaming failed: {e}")
                    self.cleanup()
                    # Fallback to headless
                    logger.info("Falling back to headless streaming...")
                    try:
                        self._start_headless_streaming(quality)
                    except Exception as e2:
                        logger.error(f"Headless fallback also failed: {e2}")
                        self.cleanup()
                        # Final fallback to test pattern
                        logger.info("Final fallback to test pattern streaming...")
                        self._start_test_pattern_streaming(quality)

        except Exception as e:
            logger.error(f"Error in smart streaming setup: {e}")
            # Emergency fallback
            logger.info("Emergency fallback to test pattern streaming...")
            self.cleanup()
            self._start_test_pattern_streaming(quality)
    
    def _detect_headless_system(self):
        """Detect if we're running on a headless system (no X11 available).
        Headless is preferred: it's cheaper and doesn't need Xvfb."""
        try:
            # An active X display means we can use x11grab directly
            if os.environ.get('DISPLAY') and shutil.which('xset'):
                result = subprocess.run(['xset', 'q'], capture_output=True, timeout=5)
                if result.returncode == 0:
                    return False  # X11 available

            # Default to headless if uncertain
            return True

        except Exception:
            # If detection fails, assume headless
            return True
    
    def _start_headless_streaming(self, quality):
        """Start true headless streaming (no X11 required)"""
        logger.info("Starting headless streaming...")
        
        if self.config['type'] == 'html':
            # Use Chrome DevTools Protocol for HTML content
            self._start_headless_html_streaming(quality)
        elif self.config['type'] == 'pygame':
            # Use memory surface for Pygame content  
            self._start_headless_pygame_streaming(quality)
        else:
            raise Exception(f"Unsupported content type for headless streaming: {self.config['type']}")
    
    def _start_x11_streaming(self, quality):
        """Start traditional X11 streaming with Xvfb"""
        logger.info("Starting X11 streaming with virtual display...")

        if not shutil.which('Xvfb'):
            raise Exception("Xvfb is not installed")

        # Start virtual display (display number derived from the stream ID)
        display_port = self.display_name
        self.processes['display'] = subprocess.Popen([
            'Xvfb', display_port, '-screen', '0', f"{quality['resolution']}x24", '-ac'
        ])

        time.sleep(2)  # Allow Xvfb to start

        if self.processes['display'].poll() is not None:
            raise Exception(f"Xvfb failed to start on display {display_port}")

        env = os.environ.copy()
        env['DISPLAY'] = display_port

        # Start content renderer
        if self.config['type'] == 'html':
            self._start_html_renderer(env, quality)
        elif self.config['type'] == 'pygame':
            self._start_pygame_renderer(env)
        else:
            raise Exception(f"Unsupported content type: {self.config['type']}")

        time.sleep(3)  # Allow content to start

        # Start FFmpeg streaming
        self._start_ffmpeg_stream(env, quality)
    
    def _start_headless_html_streaming(self, quality):
        """Start headless HTML streaming: Chrome renders the page, the
        capture_html.py helper pulls real frames out via the DevTools
        screencast API, and FFmpeg encodes them to RTMP."""
        try:
            browser = find_browser()
            if not browser:
                raise Exception("No Chromium/Chrome browser found. Install chromium-browser.")

            chrome_port = self.debug_port  # Unique port per stream

            # Detect available memory for optimization
            import psutil
            total_memory_mb = psutil.virtual_memory().total // (1024 * 1024)

            chrome_cmd = [
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
                '--disable-background-networking',
                '--disable-back-forward-cache',
                '--disable-ipc-flooding-protection',
                '--disable-component-update',
                '--disable-default-apps',
                '--disable-sync',
                '--memory-pressure-off',
                '--mute-audio',
                '--hide-scrollbars',
                f'--window-size={quality["resolution"].replace("x", ",")}',
                f'--remote-debugging-port={chrome_port}',
                '--remote-debugging-address=127.0.0.1',
                f'--remote-allow-origins=http://127.0.0.1:{chrome_port}',
                f'--user-data-dir=/tmp/streamdrop-chrome-{self.instance_num}',
            ]

            # Add aggressive memory optimizations for low-memory systems
            if total_memory_mb < 1024:
                chrome_cmd.extend([
                    '--single-process',  # Run in single process mode
                    '--no-zygote',  # Don't use zygote process
                    '--js-flags=--max-old-space-size=96',
                    '--enable-low-end-device-mode',
                    '--disable-site-isolation-trials',
                    '--disable-features=site-per-process'
                ])
                logger.info(f"Applied low-memory optimizations (system has {total_memory_mb}MB)")
            elif total_memory_mb < 2048:
                chrome_cmd.append('--js-flags=--max-old-space-size=256')

            chrome_cmd.append(self.config['source'])

            logger.info(f"Starting headless Chrome: {' '.join(chrome_cmd[:8])}...")
            # Send Chrome output to a log file: an unread PIPE fills up and
            # blocks Chrome after a while, killing 24/7 streams.
            chrome_log_path = f'/tmp/streamdrop-chrome-{self.instance_num}.log'
            self._chrome_log = open(chrome_log_path, 'wb')
            self.processes['chrome'] = subprocess.Popen(
                chrome_cmd, stderr=self._chrome_log, stdout=self._chrome_log)

            # Wait for Chrome to start and check if it's still running
            time.sleep(3)

            if self.processes['chrome'].poll() is not None:
                # Chrome died immediately
                try:
                    with open(chrome_log_path, 'r', errors='replace') as f:
                        stderr_output = f.read()
                except OSError:
                    stderr_output = "No error output"
                logger.error(f"Chrome died immediately. Error: {stderr_output[:1000]}")

                # Check for common memory-related errors
                if "Shared memory" in stderr_output or "/dev/shm" in stderr_output:
                    logger.error("Chrome crashed due to shared memory limits. System may need more RAM or larger /dev/shm")
                    logger.info("Falling back to test pattern streaming...")
                    self.cleanup()
                    return self._start_test_pattern_streaming(quality)
                elif "memory" in stderr_output.lower() or "oom" in stderr_output.lower():
                    logger.error("Chrome crashed due to out of memory. System needs more RAM.")
                    logger.info("Falling back to test pattern streaming...")
                    self.cleanup()
                    return self._start_test_pattern_streaming(quality)

                raise Exception(f"Chrome failed to start: {stderr_output[:500]}")

            # Verify Chrome is responding on debug port
            max_retries = 15
            for i in range(max_retries):
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(1)
                    result = sock.connect_ex(('127.0.0.1', chrome_port))
                    sock.close()
                    if result == 0:
                        logger.info(f"Chrome debug port {chrome_port} is responding")
                        break
                except OSError:
                    pass
                time.sleep(1)
            else:
                raise Exception(f"Chrome debug port {chrome_port} not responding after {max_retries}s")

            # Start the frame capture helper (JPEG frames on its stdout)
            width, height = quality['resolution'].split('x')
            capture_cmd = [
                sys.executable, os.path.join(BASE_DIR, 'capture_html.py'),
                '--port', str(chrome_port),
                '--fps', str(quality['framerate']),
                '--width', width,
                '--height', height,
            ]
            logger.info("Starting DevTools screencast capture...")
            self.processes['capture'] = subprocess.Popen(
                capture_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL
            )

            # FFmpeg encodes the captured frames and pushes to RTMP
            self._start_pipe_ffmpeg_stream(quality, self.processes['capture'].stdout)

            # Confirm the pipeline survived startup
            time.sleep(3)
            for name in ('capture', 'ffmpeg'):
                if self.processes.get(name) and self.processes[name].poll() is not None:
                    raise Exception(f"Headless pipeline process '{name}' exited during startup")

        except Exception as e:
            logger.error(f"Failed to start headless HTML streaming: {e}")
            raise
    
    def _start_headless_pygame_streaming(self, quality):
        """Start headless Pygame streaming: capture_pygame.py runs the game
        with SDL's dummy driver and pipes real frames to FFmpeg."""
        try:
            if not os.path.exists(self.config['source']):
                raise Exception(f"Pygame script not found: {self.config['source']}")

            capture_cmd = [
                sys.executable, os.path.join(BASE_DIR, 'capture_pygame.py'),
                '--fps', str(quality['framerate']),
                self.config['source'],
            ]

            logger.info(f"Starting headless Pygame capture: {self.config['source']}")
            self.processes['capture'] = subprocess.Popen(
                capture_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL
            )

            time.sleep(2)  # Allow pygame to start

            if self.processes['capture'].poll() is not None:
                raise Exception("Pygame capture process exited during startup")

            # Start FFmpeg to encode the captured frames and stream
            self._start_pipe_ffmpeg_stream(quality, self.processes['capture'].stdout)

            time.sleep(3)
            if self.processes['ffmpeg'].poll() is not None:
                raise Exception("FFmpeg exited during startup")

        except Exception as e:
            logger.error(f"Failed to start headless Pygame streaming: {e}")
            raise
    
    def _start_test_pattern_streaming(self, quality):
        """Start simple test pattern streaming - reliable fallback for low-memory systems"""
        try:
            logger.info("Starting test pattern streaming - reliable mode for low-memory VPS")

            # Create a simple but informative test pattern
            pattern_type = "testsrc2" if "html" in self.config.get('type', 'html').lower() else "mandelbrot"

            # Generate stream info overlay (strip characters that break drawtext)
            stream_name = self.config.get('name', 'StreamDrop Stream')
            for bad_char in (':', "'", '"', '%', '\\'):
                stream_name = stream_name.replace(bad_char, '')
            stream_name = stream_name or 'StreamDrop Stream'
            platform = self.config.get('platform', 'Unknown')
            bottom_y = int(quality["resolution"].split("x")[1]) - 40

            ffmpeg_cmd = [
                'ffmpeg',
                '-re',  # pace generated input at realtime, like a live source
                '-f', 'lavfi',
                '-i', f'{pattern_type}=size={quality["resolution"]}:rate={quality["framerate"]}',
                '-re',
                '-f', 'lavfi',
                '-i', 'anullsrc=channel_layout=stereo:sample_rate=44100',
                '-vf', f'drawtext=text=\'{stream_name}\':x=10:y=10:fontsize=24:fontcolor=white,'
                       f'drawtext=text=\'Platform {platform}\':x=10:y=50:fontsize=18:fontcolor=yellow,'
                       f'drawtext=text=\'StreamDrop Active\':x=10:y=80:fontsize=18:fontcolor=lime,'
                       f'drawtext=text=\'%{{localtime\\:%Y-%m-%d %X}}\':x=10:y={bottom_y}:fontsize=16:fontcolor=white',
                '-c:v', 'libx264',
                '-preset', 'veryfast',
                '-pix_fmt', 'yuv420p',
                '-c:a', 'aac',
                '-b:v', quality['bitrate'],
                '-b:a', '128k',
                '-maxrate', quality['bitrate'],
                '-bufsize', str(int(str(quality['bitrate']).rstrip('k')) * 2) + 'k',
                '-g', str(int(quality['framerate']) * 2),
                '-r', str(quality['framerate']),
            ] + self._ffmpeg_output_args()

            logger.info("Starting test pattern FFmpeg stream - this will work on any VPS size")
            self.processes['ffmpeg'] = subprocess.Popen(
                ffmpeg_cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )

            # No Chrome process needed for test pattern
            logger.info(f"Test pattern stream started successfully for {stream_name}")

        except Exception as e:
            logger.error(f"Failed to start test pattern streaming: {e}")
            raise

    def _ffmpeg_output_args(self):
        """Build the FFmpeg output section: single RTMP target, or tee for multi-streaming"""
        targets = self._get_stream_targets()
        if len(targets) == 1:
            return ['-f', 'flv', targets[0]]
        tee_outputs = '|'.join(f'[f=flv:onfail=ignore]{target}' for target in targets)
        return ['-map', '0:v', '-map', '1:a', '-f', 'tee', tee_outputs]

    def _start_pipe_ffmpeg_stream(self, quality, frame_pipe):
        """Start FFmpeg reading JPEG frames from a capture process pipe and
        pushing encoded video (with a silent audio track) to RTMP"""
        # Normalize frames to the exact preset resolution: capture sources
        # (browser viewports, pygame windows) pick their own sizes, while
        # streaming platforms expect a fixed, even-dimensioned canvas.
        width, height = quality['resolution'].split('x')
        ffmpeg_cmd = [
            'ffmpeg',
            '-f', 'image2pipe',
            '-framerate', str(quality['framerate']),
            '-i', '-',
            '-f', 'lavfi',
            '-i', 'anullsrc=channel_layout=stereo:sample_rate=44100',
            '-vf', f'scale={width}:{height}:force_original_aspect_ratio=decrease,'
                   f'pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:color=black',
        ]

        ffmpeg_cmd.extend([
            '-c:v', 'libx264',
            '-preset', 'veryfast',
            '-b:v', quality['bitrate'],
            '-maxrate', quality['bitrate'],
            '-bufsize', str(int(str(quality['bitrate']).rstrip('k')) * 2) + 'k',
            '-pix_fmt', 'yuv420p',
            '-g', str(int(quality['framerate']) * 2),
            '-r', str(quality['framerate']),
            '-c:a', 'aac',
            '-b:a', '128k',
            '-ar', '44100',
            # anullsrc is infinite: without -shortest, FFmpeg would keep
            # streaming silence forever after the frame pipe closes
            '-shortest',
        ] + self._ffmpeg_output_args())

        logger.info("Starting FFmpeg (frame pipe -> RTMP)...")
        self.processes['ffmpeg'] = subprocess.Popen(
            ffmpeg_cmd,
            stdin=frame_pipe,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        # FFmpeg holds its own copy of the pipe now; drop ours
        try:
            frame_pipe.close()
        except OSError:
            pass


    def _start_html_renderer(self, env, quality):
        """Start Chrome for HTML content, rendering into the virtual X display.
        Must NOT be headless: FFmpeg captures the display with x11grab."""
        browser = find_browser()
        if not browser:
            raise Exception("No Chromium/Chrome browser found. Install chromium-browser.")

        chrome_cmd = [
            browser,
            '--kiosk',
            '--window-position=0,0',
            '--disable-gpu',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-extensions',
            '--disable-plugins',
            '--disable-background-timer-throttling',
            '--disable-backgrounding-occluded-windows',
            '--disable-renderer-backgrounding',
            '--disable-features=TranslateUI',
            '--disable-background-networking',
            '--disable-back-forward-cache',
            '--disable-ipc-flooding-protection',
            '--disable-component-update',
            '--disable-default-apps',
            '--disable-sync',
            '--disable-infobars',
            '--no-first-run',
            '--memory-pressure-off',
            '--mute-audio',
            '--hide-scrollbars',
            f'--window-size={quality["resolution"].replace("x", ",")}',
            f'--user-data-dir=/tmp/streamdrop-chrome-{self.instance_num}',
            self.config['source']
        ]

        self.processes['renderer'] = subprocess.Popen(
            chrome_cmd,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

    def _start_pygame_renderer(self, env):
        """Start pygame script"""
        if not os.path.exists(self.config['source']):
            raise Exception(f"Pygame script not found: {self.config['source']}")

        self.processes['renderer'] = subprocess.Popen([
            sys.executable, self.config['source']
        ], env=env)
    
    def _start_ffmpeg_stream(self, env, quality):
        """Start FFmpeg streaming process with audio and multi-streaming support"""
        display_port = env['DISPLAY']
        
        # Build FFmpeg command
        ffmpeg_cmd = ['ffmpeg']
        
        # Video input - X11 screen capture
        ffmpeg_cmd.extend([
            '-f', 'x11grab',
            '-video_size', quality['resolution'],
            '-framerate', quality['framerate'],
            '-i', display_port
        ])
        
        # Audio input configuration
        audio_enabled = self.audio_config.get('enabled', False)
        if audio_enabled:
            audio_device = self.audio_config.get('device', 'pulse')
            if audio_device == 'pulse':
                ffmpeg_cmd.extend(['-f', 'pulse', '-i', 'default'])
            elif audio_device == 'alsa':
                ffmpeg_cmd.extend(['-f', 'alsa', '-i', 'hw:0'])
            else:
                ffmpeg_cmd.extend(['-f', 'pulse', '-i', audio_device])
        else:
            # No audio - add silent audio track
            ffmpeg_cmd.extend(['-f', 'lavfi', '-i', 'anullsrc'])
        
        # Video encoding settings
        ffmpeg_cmd.extend([
            '-c:v', 'libx264',
            '-preset', self.audio_config.get('video_preset', 'veryfast'),
            '-b:v', quality['bitrate'],
            '-maxrate', quality['bitrate'],
            '-bufsize', str(int(quality['bitrate'].replace('k', '')) * 2) + 'k',
            '-pix_fmt', 'yuv420p',
            '-g', str(int(quality['framerate']) * 2)  # GOP size = 2 * framerate
        ])
        
        # Audio encoding settings
        if audio_enabled:
            ffmpeg_cmd.extend([
                '-c:a', 'aac',
                '-b:a', self.audio_config.get('audio_bitrate', '128k'),
                '-ar', str(self.audio_config.get('sample_rate', 44100))
            ])
        else:
            ffmpeg_cmd.extend(['-c:a', 'aac', '-b:a', '128k'])

        # Output target(s): single RTMP URL or tee muxer for multi-streaming
        ffmpeg_cmd.extend(self._ffmpeg_output_args())

        logger.info(f"Starting FFmpeg with command: {' '.join(ffmpeg_cmd[:10])}... (truncated)")

        self.processes['ffmpeg'] = subprocess.Popen(
            ffmpeg_cmd,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
    
    def _get_stream_targets(self):
        """Get all streaming targets (primary + multi-stream targets)"""
        targets = []
        
        # Primary target
        primary_target = self._build_rtmp_url(self.config['platform'], self.config['stream_key'], self.config.get('rtmp_url'))
        targets.append(primary_target)
        
        # Additional multi-stream targets
        for target in self.multi_stream_targets:
            if target.get('enabled', True):
                target_url = self._build_rtmp_url(target['platform'], target['stream_key'], target.get('rtmp_url'))
                targets.append(target_url)
        
        return targets
    
    def _build_rtmp_url(self, platform, stream_key, custom_url=None):
        """Build RTMP URL for a platform"""
        if platform == 'custom' and custom_url:
            return f"{custom_url}/{stream_key}"
        
        platform_config = self.platform_configs.get(platform)
        if not platform_config:
            raise Exception(f"Unsupported platform: {platform}")
        
        return f"{platform_config['rtmp_url']}{stream_key}"
    
    def stop_streaming(self):
        """Stop the streaming process"""
        self.cleanup()
        self.status = "stopped"
        self.db.update_stream_status(self.config['id'], 'stopped')
        self.db.log_event(self.config['id'], 'stream_stopped')
        
        logger.info(f"Stream {self.config['name']} stopped")
        return True, "Stream stopped successfully"
    
    def cleanup(self):
        """Clean up all processes"""
        for process_name, process in self.processes.items():
            if process:
                try:
                    process.terminate()
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    try:
                        process.kill()
                        process.wait(timeout=2)
                    except:
                        pass
                except:
                    pass

        self.processes.clear()

        # Close the Chrome log file handle if one was opened
        chrome_log = getattr(self, '_chrome_log', None)
        if chrome_log:
            try:
                chrome_log.close()
            except OSError:
                pass
            self._chrome_log = None
    
    def get_uptime(self):
        """Get current stream uptime"""
        if self.status != "live":
            return "0m"
        
        stream_data = self.db.get_stream(self.config['id'])
        if stream_data and stream_data['start_time']:
            start_time = datetime.fromisoformat(stream_data['start_time'])
            uptime_seconds = (datetime.now() - start_time).total_seconds()
            
            hours = int(uptime_seconds // 3600)
            minutes = int((uptime_seconds % 3600) // 60)
            
            if hours > 0:
                return f"{hours}h {minutes}m"
            else:
                return f"{minutes}m"
        
        return "0m"
    
    def collect_metrics(self):
        """Collect current stream performance metrics"""
        if self.status != "live":
            return {}
        
        metrics = {
            'fps': self._get_fps(),
            'bitrate': self._get_bitrate(),
            'frame_drops': self._get_frame_drops(),
            'cpu_usage': self._get_cpu_usage(),
            'memory_usage': self._get_memory_usage(),
            'bandwidth_mbps': self._get_bandwidth(),
            'viewers': 0,  # TODO: Implement viewer tracking
            'duration_seconds': int(time.time() - (self.start_time or time.time()))
        }
        
        # Store metrics for trend analysis
        self.metrics = metrics
        
        return metrics
    
    def _get_fps(self):
        """Calculate current FPS from FFmpeg stats"""
        try:
            if 'ffmpeg' not in self.processes or not self.processes['ffmpeg']:
                return 0
            
            # Try to read FFmpeg stderr for stats
            # This is a simplified implementation
            return float(self.quality_presets.get(self.config.get('quality', 'medium'), {}).get('framerate', '30'))
        except:
            return 0
    
    def _get_bitrate(self):
        """Get current bitrate from FFmpeg stats"""
        try:
            quality_preset = self.quality_presets.get(self.config.get('quality', 'medium'), {})
            bitrate_str = quality_preset.get('bitrate', '2500k')
            return float(bitrate_str.replace('k', '')) if 'k' in bitrate_str else float(bitrate_str)
        except:
            return 0
    
    def _get_frame_drops(self):
        """Get frame drop count"""
        # TODO: Parse actual FFmpeg stats
        return 0
    
    def _get_cpu_usage(self):
        """Get CPU usage for stream processes"""
        try:
            import psutil
            total_cpu = 0
            process_count = 0
            
            for proc_name, proc in self.processes.items():
                if proc and proc.poll() is None:
                    try:
                        p = psutil.Process(proc.pid)
                        total_cpu += p.cpu_percent()
                        process_count += 1
                    except:
                        pass
            
            return total_cpu / max(process_count, 1)
        except ImportError:
            return 0
    
    def _get_memory_usage(self):
        """Get memory usage for stream processes in MB"""
        try:
            import psutil
            total_memory = 0
            
            for proc_name, proc in self.processes.items():
                if proc and proc.poll() is None:
                    try:
                        p = psutil.Process(proc.pid)
                        total_memory += p.memory_info().rss / 1024 / 1024  # Convert to MB
                    except:
                        pass
            
            return total_memory
        except ImportError:
            return 0
    
    def _get_bandwidth(self):
        """Estimate bandwidth usage in Mbps"""
        try:
            bitrate_kbps = self._get_bitrate()
            return bitrate_kbps / 1000  # Convert kbps to Mbps
        except:
            return 0
    
    def calculate_health_score(self):
        """Calculate overall stream health score (0-100)"""
        if self.status != "live":
            return 100.0
        
        try:
            metrics = self.collect_metrics()
            
            # Performance score (CPU, Memory)
            cpu_score = max(0, 100 - (metrics.get('cpu_usage', 0) - 50))  # Penalty above 50%
            memory_penalty = max(0, (metrics.get('memory_usage', 0) - 500) / 10)  # Penalty above 500MB
            performance_score = max(0, min(100, cpu_score - memory_penalty))
            
            # Connection score (frame drops, bitrate stability)
            frame_drops = metrics.get('frame_drops', 0)
            connection_score = max(0, 100 - frame_drops * 2)  # 2 points per frame drop
            
            # Stability score (based on failure history)
            stability_score = max(0, 100 - (self.failure_count * 10))
            
            # Overall health (weighted average)
            health_score = (
                performance_score * 0.4 +
                connection_score * 0.4 +
                stability_score * 0.2
            )
            
            self.health_score = health_score
            
            # Log health metrics
            self.db.log_health_score(
                self.config['id'],
                health_score,
                connection_score,
                performance_score,
                stability_score
            )
            
            return health_score
            
        except Exception as e:
            logger.error(f"Error calculating health score: {e}")
            return 50.0  # Default to moderate health on error
    
    def detect_failure_type(self):
        """Detect the type of failure occurring"""
        failure_types = []
        
        # Check process health
        for process_name, process in self.processes.items():
            if process and process.poll() is not None:
                failure_types.append(f"process_failure_{process_name}")
        
        # Check performance metrics
        metrics = self.collect_metrics()
        if metrics.get('cpu_usage', 0) > 90:
            failure_types.append("high_cpu")
        if metrics.get('memory_usage', 0) > 2000:  # > 2GB
            failure_types.append("memory_exhaustion")
        if metrics.get('frame_drops', 0) > 50:
            failure_types.append("connection_issues")
        
        # Check health score
        if self.health_score < 30:
            failure_types.append("critical_health")
        
        return failure_types if failure_types else ["unknown_failure"]
    
    def attempt_recovery(self, failure_types):
        """Attempt to recover from detected failures"""
        if self.recovery_in_progress:
            return False, "Recovery already in progress"
        
        self.recovery_in_progress = True
        recovery_start_time = time.time()
        
        try:
            # Determine recovery strategy based on failure types
            primary_failure = failure_types[0] if failure_types else "unknown_failure"
            strategy = self._select_recovery_strategy(primary_failure)
            
            # Create recovery attempt record
            self.active_recovery_id = self.db.create_recovery_attempt(
                self.config['id'],
                primary_failure,
                strategy,
                f"Failures detected: {', '.join(failure_types)}"
            )
            
            logger.info(f"Starting recovery for {self.config['name']} using strategy: {strategy}")
            
            # Execute recovery strategy
            success = self._execute_recovery_strategy(strategy, failure_types)
            
            recovery_duration = time.time() - recovery_start_time
            
            # Update recovery record
            if self.active_recovery_id:
                self.db.update_recovery_attempt(
                    self.active_recovery_id,
                    1,  # retry count
                    success,
                    recovery_duration
                )
            
            if success:
                logger.info(f"Recovery successful for {self.config['name']} in {recovery_duration:.1f}s")
                self.failure_count = max(0, self.failure_count - 1)  # Reduce failure count on success
                self.db.log_event(self.config['id'], 'recovery_success', {
                    'strategy': strategy,
                    'duration': recovery_duration,
                    'failure_types': failure_types
                })
            else:
                logger.error(f"Recovery failed for {self.config['name']}")
                self.failure_count += 1
                self.db.create_alert(
                    self.config['id'],
                    'recovery_failed',
                    'critical',
                    f'Failed to recover from {primary_failure} using {strategy}'
                )
            
            self.recovery_in_progress = False
            self.active_recovery_id = None
            
            return success, f"Recovery {'successful' if success else 'failed'}"
            
        except Exception as e:
            self.recovery_in_progress = False
            self.active_recovery_id = None
            logger.error(f"Recovery attempt failed with exception: {e}")
            return False, f"Recovery exception: {e}"
    
    def _select_recovery_strategy(self, failure_type):
        """Select appropriate recovery strategy based on failure type"""
        strategy_map = {
            'process_failure_ffmpeg': 'restart_ffmpeg',
            'process_failure_renderer': 'restart_renderer',
            'process_failure_display': 'restart_display',
            'high_cpu': 'reduce_quality',
            'memory_exhaustion': 'restart_all',
            'connection_issues': 'reconnect_stream',
            'critical_health': 'full_restart',
            'unknown_failure': 'full_restart'
        }
        
        return strategy_map.get(failure_type, 'full_restart')
    
    def _execute_recovery_strategy(self, strategy, failure_types):
        """Execute the selected recovery strategy"""
        try:
            if strategy == 'restart_ffmpeg':
                return self._restart_ffmpeg()
            elif strategy == 'restart_renderer':
                return self._restart_renderer()
            elif strategy == 'restart_display':
                return self._restart_display()
            elif strategy == 'reduce_quality':
                return self._reduce_quality()
            elif strategy == 'reconnect_stream':
                return self._reconnect_stream()
            elif strategy in ['restart_all', 'full_restart']:
                return self._full_restart()
            else:
                logger.warning(f"Unknown recovery strategy: {strategy}")
                return self._full_restart()
                
        except Exception as e:
            logger.error(f"Error executing recovery strategy {strategy}: {e}")
            return False
    
    def _restart_ffmpeg(self):
        """Restart only the FFmpeg process"""
        try:
            # FFmpeg can only be restarted in isolation on the X11 path
            # (x11grab reattaches to the still-running Xvfb display). Headless
            # pipelines feed FFmpeg's stdin, and test-pattern streams have no
            # display at all - restart those pipelines whole.
            if 'capture' in self.processes or 'display' not in self.processes:
                return self._full_restart()

            # Stop FFmpeg
            if 'ffmpeg' in self.processes and self.processes['ffmpeg']:
                self.processes['ffmpeg'].terminate()
                self.processes['ffmpeg'].wait(timeout=5)

            time.sleep(2)

            # Restart FFmpeg (X11 capture path)
            env = os.environ.copy()
            env['DISPLAY'] = self.display_name

            self._start_ffmpeg_stream(env, self._get_effective_quality())

            # Confirm it actually survived startup
            time.sleep(2)
            if self.processes['ffmpeg'].poll() is not None:
                logger.error("Restarted FFmpeg exited immediately")
                return False

            return True

        except Exception as e:
            logger.error(f"Failed to restart FFmpeg: {e}")
            return False

    def _restart_renderer(self):
        """Restart the content renderer process"""
        try:
            # Headless pipelines have no standalone renderer to bounce
            if 'capture' in self.processes:
                return self._full_restart()

            # Stop renderer
            if 'renderer' in self.processes and self.processes['renderer']:
                self.processes['renderer'].terminate()
                self.processes['renderer'].wait(timeout=5)

            time.sleep(2)

            # Restart renderer
            env = os.environ.copy()
            env['DISPLAY'] = self.display_name
            quality = self._get_effective_quality()

            if self.config['type'] == 'html':
                self._start_html_renderer(env, quality)
            elif self.config['type'] == 'pygame':
                self._start_pygame_renderer(env)

            return True

        except Exception as e:
            logger.error(f"Failed to restart renderer: {e}")
            return False
    
    def _restart_display(self):
        """Restart the virtual display"""
        try:
            # This requires a full restart as display affects all other processes
            return self._full_restart()
            
        except Exception as e:
            logger.error(f"Failed to restart display: {e}")
            return False
    
    def _load_platform_configs(self):
        """Load platform configurations from database"""
        try:
            platforms = self.db.get_platform_configs()
            self.platform_configs = {p['platform_name']: p for p in platforms}
        except Exception as e:
            logger.error(f"Error loading platform configs: {e}")
            # Fallback to hardcoded configs
            self.platform_configs = {
                'youtube': {'rtmp_url': 'rtmp://a.rtmp.youtube.com/live2/', 'max_bitrate': 9000},
                'twitch': {'rtmp_url': 'rtmp://live.twitch.tv/live/', 'max_bitrate': 6000},
                'facebook': {'rtmp_url': 'rtmps://live-api-s.facebook.com:443/rtmp/', 'max_bitrate': 4000}
            }
    
    def _reduce_quality(self):
        """Reduce stream quality to improve performance"""
        try:
            current_quality = self.config.get('quality', 'medium')
            quality_levels = ['ultra', 'high', 'medium', 'low']
            
            if current_quality in quality_levels:
                current_index = quality_levels.index(current_quality)
                if current_index < len(quality_levels) - 1:
                    new_quality = quality_levels[current_index + 1]
                    
                    # Update stream quality
                    self.config['quality'] = new_quality
                    self.db.update_stream(self.config['id'], {'quality': new_quality})
                    
                    logger.info(f"Reduced quality from {current_quality} to {new_quality}")
                    
                    # Restart with new quality
                    return self._full_restart()
            
            return False  # Can't reduce quality further
            
        except Exception as e:
            logger.error(f"Failed to reduce quality: {e}")
            return False
    
    def _reconnect_stream(self):
        """Reconnect the stream by restarting FFmpeg with delay"""
        try:
            # Stop FFmpeg with a longer wait
            if 'ffmpeg' in self.processes and self.processes['ffmpeg']:
                self.processes['ffmpeg'].terminate()
                self.processes['ffmpeg'].wait(timeout=10)
            
            # Wait before reconnecting
            time.sleep(5)
            
            # Restart FFmpeg
            return self._restart_ffmpeg()
            
        except Exception as e:
            logger.error(f"Failed to reconnect stream: {e}")
            return False
    
    def _full_restart(self):
        """Perform a complete stream restart"""
        try:
            # Clean up all processes
            self.cleanup()

            # start_streaming() refuses to run while status is "live"
            self.status = "stopped"

            # Wait before restarting
            time.sleep(3)

            # Restart the entire stream
            success, message = self.start_streaming()
            return success

        except Exception as e:
            logger.error(f"Failed to perform full restart: {e}")
            return False

class StreamManager:
    """Main stream manager class"""
    
    def __init__(self):
        self.db = StreamDatabase()
        # Any 'live'/'error' rows in the DB are stale after a restart: no
        # processes exist yet, so reflect reality before serving requests.
        self.db.reset_stale_statuses()
        self.active_streams = {}
        self.monitor_thread = None
        self.monitoring = False
    
    def start_monitoring(self):
        """Start stream monitoring thread"""
        self.monitoring = True
        self.monitor_thread = Thread(target=self._monitor_streams, daemon=True)
        self.monitor_thread.start()
        logger.info("Stream monitoring started")
    
    def stop_monitoring(self):
        """Stop stream monitoring"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
    
    def _monitor_streams(self):
        """Monitor active streams for health and errors"""
        metrics_interval = 30  # Collect metrics every 30 seconds
        last_metrics_time = time.time()
        
        while self.monitoring:
            try:
                current_time = time.time()
                
                with stream_lock:
                    for stream_id, stream_instance in list(self.active_streams.items()):
                        # Check if processes are still running
                        if stream_instance.status == "live":
                            all_running = True
                            dead_processes = []
                            
                            for process_name, process in stream_instance.processes.items():
                                if process and process.poll() is not None:
                                    logger.warning(f"Process {process_name} died for stream {stream_id}")
                                    dead_processes.append(process_name)
                                    all_running = False
                            
                            if not all_running:
                                logger.error(f"Stream {stream_id} failed, attempting intelligent recovery")
                                
                                # Detect failure types for intelligent recovery
                                failure_types = stream_instance.detect_failure_type()
                                
                                # Attempt intelligent recovery
                                recovery_success, recovery_message = stream_instance.attempt_recovery(failure_types)
                                
                                if recovery_success:
                                    logger.info(f"Successfully recovered stream {stream_id}: {recovery_message}")
                                    # Stream should be running again
                                    continue
                                else:
                                    logger.error(f"Recovery failed for stream {stream_id}: {recovery_message}")
                                    stream_instance.cleanup()
                                    stream_instance.status = "error"
                                    self.db.update_stream_status(stream_id, 'error')
                                    self.db.log_event(stream_id, 'recovery_failed', {
                                        'reason': 'auto_recovery_failed',
                                        'dead_processes': dead_processes,
                                        'failure_types': failure_types,
                                        'recovery_message': recovery_message
                                    })
                            
                            # Collect performance metrics and health scoring
                            elif current_time - last_metrics_time >= metrics_interval:
                                try:
                                    metrics = stream_instance.collect_metrics()
                                    if metrics:
                                        self.db.log_metrics(stream_id, metrics)
                                        
                                        # Calculate and track health score
                                        health_score = stream_instance.calculate_health_score()
                                        
                                        # Check for performance issues
                                        self._check_performance_alerts(stream_id, metrics)
                                        
                                        # Predictive recovery for degrading health
                                        if health_score < 50 and not stream_instance.recovery_in_progress:
                                            logger.warning(f"Stream {stream_id} health degrading: {health_score:.1f}%")
                                            
                                            # Attempt preemptive recovery
                                            failure_types = stream_instance.detect_failure_type()
                                            if failure_types and 'unknown_failure' not in failure_types:
                                                logger.info(f"Starting preemptive recovery for {stream_id}")
                                                recovery_success, recovery_message = stream_instance.attempt_recovery(failure_types)
                                                
                                                if recovery_success:
                                                    self.db.create_alert(
                                                        stream_id,
                                                        'preemptive_recovery',
                                                        'info',
                                                        f'Preemptive recovery successful: {recovery_message}'
                                                    )
                                        
                                except Exception as e:
                                    logger.error(f"Error collecting metrics for {stream_id}: {e}")
                
                # Update metrics collection timestamp
                if current_time - last_metrics_time >= metrics_interval:
                    last_metrics_time = current_time
                
                time.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logger.error(f"Error in stream monitoring: {e}")
                time.sleep(5)
    
    def _check_performance_alerts(self, stream_id, metrics):
        """Check metrics for performance issues and create alerts"""
        try:
            # Check CPU usage
            if metrics.get('cpu_usage', 0) > 80:
                self.db.create_alert(
                    stream_id, 
                    'high_cpu', 
                    'warning', 
                    f'High CPU usage: {metrics["cpu_usage"]:.1f}%'
                )
            
            # Check memory usage
            if metrics.get('memory_usage', 0) > 1000:  # > 1GB
                self.db.create_alert(
                    stream_id, 
                    'high_memory', 
                    'warning', 
                    f'High memory usage: {metrics["memory_usage"]:.0f}MB'
                )
            
            # Check frame drops
            if metrics.get('frame_drops', 0) > 100:
                self.db.create_alert(
                    stream_id, 
                    'frame_drops', 
                    'warning', 
                    f'Frame drops detected: {metrics["frame_drops"]} frames'
                )
                
        except Exception as e:
            logger.error(f"Error checking performance alerts: {e}")
    
    def create_stream(self, stream_data):
        """Create a new stream"""
        try:
            required = ['name', 'type', 'platform', 'stream_key', 'source']
            missing = [field for field in required if not stream_data.get(field)]
            if missing:
                return False, None, f"Missing required fields: {', '.join(missing)}"

            stream_id = self.db.create_stream(stream_data)
            logger.info(f"Created stream {stream_data['name']} with ID {stream_id}")
            return True, stream_id, "Stream created successfully"
        except Exception as e:
            logger.error(f"Error creating stream: {e}")
            return False, None, f"Error creating stream: {e}"
    
    def start_stream(self, stream_id):
        """Start a specific stream"""
        try:
            with stream_lock:
                if stream_id in self.active_streams:
                    return self.active_streams[stream_id].start_streaming()
                
                # Load stream config and create instance
                stream_config = self.db.get_stream(stream_id)
                if not stream_config:
                    return False, "Stream not found"
                
                stream_instance = StreamInstance(stream_config, self.db)
                self.active_streams[stream_id] = stream_instance
                
                return stream_instance.start_streaming()
                
        except Exception as e:
            logger.error(f"Error starting stream {stream_id}: {e}")
            return False, f"Error starting stream: {e}"
    
    def stop_stream(self, stream_id):
        """Stop a specific stream"""
        try:
            with stream_lock:
                if stream_id not in self.active_streams:
                    # Stream not in active_streams - check if it needs to be reset
                    stream_config = self.db.get_stream(stream_id)
                    if stream_config and stream_config['status'] != 'stopped':
                        # Reset the stream status to stopped in database
                        self.db.update_stream_status(stream_id, 'stopped')
                        self.db.log_event(stream_id, 'stream_reset', {
                            'reason': 'manual_reset_from_error_state',
                            'previous_status': stream_config['status']
                        })
                        logger.info(f"Reset stream {stream_id} from {stream_config['status']} to stopped")
                        return True, "Stream reset to stopped state"
                    return False, "Stream not active"
                
                result = self.active_streams[stream_id].stop_streaming()
                del self.active_streams[stream_id]
                return result
                
        except Exception as e:
            logger.error(f"Error stopping stream {stream_id}: {e}")
            return False, f"Error stopping stream: {e}"
    
    def update_stream(self, stream_id, stream_data):
        """Update a stream configuration"""
        try:
            with stream_lock:
                # Check if stream exists
                existing_stream = self.db.get_stream(stream_id)
                if not existing_stream:
                    return False, "Stream not found"
                
                # Stop stream if running (will need restart for changes)
                was_running = False
                if stream_id in self.active_streams:
                    was_running = True
                    self.stop_stream(stream_id)
                
                # Update stream in database
                success = self.db.update_stream(stream_id, stream_data)
                if not success:
                    return False, "Failed to update stream"
                
                # Restart stream if it was running
                if was_running:
                    # Give it a moment, then restart
                    import time
                    time.sleep(1)
                    self.start_stream(stream_id)
                
                return True, "Stream updated successfully"
                
        except Exception as e:
            logger.error(f"Error updating stream {stream_id}: {e}")
            return False, f"Error updating stream: {e}"
    
    def delete_stream(self, stream_id):
        """Delete a stream configuration"""
        try:
            with stream_lock:
                # Stop stream if running
                if stream_id in self.active_streams:
                    self.stop_stream(stream_id)
                
                self.db.delete_stream(stream_id)
                return True, "Stream deleted successfully"
                
        except Exception as e:
            logger.error(f"Error deleting stream {stream_id}: {e}")
            return False, f"Error deleting stream: {e}"
    
    def get_all_streams(self):
        """Get all streams with current status"""
        streams = self.db.get_all_streams()
        
        # Update with live status and uptime
        for stream in streams:
            stream_id = stream['id']
            if stream_id in self.active_streams:
                stream_instance = self.active_streams[stream_id]
                stream['status'] = stream_instance.status
                stream['uptime'] = stream_instance.get_uptime()
            else:
                stream['uptime'] = "0m"
        
        return streams
    
    def cleanup_all(self):
        """Clean up all active streams"""
        with stream_lock:
            for stream_instance in self.active_streams.values():
                stream_instance.cleanup()
            self.active_streams.clear()
        
        self.stop_monitoring()
        logger.info("All streams cleaned up")

def _load_or_create_secret_key():
    """Persist the Flask session key so restarts don't log everyone out"""
    env_key = os.environ.get('FLASK_SECRET_KEY')
    if env_key:
        return env_key

    secret_path = os.path.join(BASE_DIR, '.streamdrop_secret')
    try:
        if os.path.exists(secret_path):
            with open(secret_path, 'r') as f:
                key = f.read().strip()
                if key:
                    return key
        key = os.urandom(24).hex()
        with open(secret_path, 'w') as f:
            f.write(key)
        os.chmod(secret_path, 0o600)
        return key
    except OSError as e:
        logger.warning(f"Could not persist secret key ({e}); sessions will reset on restart")
        return os.urandom(24).hex()

# Create Flask app
app = Flask(__name__)
app.secret_key = _load_or_create_secret_key()
stream_manager = StreamManager()

def check_auth(username, password):
    """Check if username and password match stored credentials"""
    # Add 1-second delay to prevent timing attacks and PRNG exploitation
    time.sleep(1)
    
    try:
        if os.path.exists('.streamdrop_auth'):
            with open('.streamdrop_auth', 'r') as f:
                stored_auth = f.read().strip()
                stored_username, stored_password = stored_auth.split(':', 1)
                return username == stored_username and password == stored_password
    except Exception as e:
        logger.error(f"Error checking authentication: {e}")
    return False

def requires_auth(f):
    """Decorator to require login via session"""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated

@app.route('/login', methods=['GET', 'POST'])
def login():
    """Login page"""
    error = None
    
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        if check_auth(username, password):
            session['logged_in'] = True
            session['username'] = username
            return redirect(url_for('dashboard'))
        else:
            error = 'Invalid username or password'
    
    return render_template('login.html', error=error)

@app.route('/logout')
def logout():
    """Logout and clear session"""
    session.clear()
    flash('Successfully logged out!', 'info')
    return redirect(url_for('login'))

@app.route('/')
@requires_auth
def dashboard():
    """Main dashboard"""
    return render_template('dashboard.html')

@app.route('/analytics')
@requires_auth
def analytics():
    """Analytics dashboard"""
    return render_template('analytics.html')

@app.route('/api/streams', methods=['GET'])
@requires_auth
def api_get_streams():
    """Get all streams"""
    streams = stream_manager.get_all_streams()
    return jsonify(streams)

@app.route('/api/streams', methods=['POST'])
@requires_auth
def api_create_stream():
    """Create new stream"""
    try:
        stream_data = request.json
        success, stream_id, message = stream_manager.create_stream(stream_data)
        return jsonify({"success": success, "stream_id": stream_id, "message": message})
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {e}"})

# Project Management APIs
@app.route('/api/projects', methods=['GET'])
def api_get_projects():
    """Get all projects"""
    projects = stream_manager.db.get_all_projects()
    return jsonify(projects)

@app.route('/api/projects', methods=['POST'])
def api_create_project():
    """Create new project"""
    try:
        project_data = request.json
        project_id = stream_manager.db.create_project(project_data)
        return jsonify({"success": True, "project_id": project_id, "message": "Project created successfully"})
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {e}"})

@app.route('/api/projects/<project_id>', methods=['PUT'])
def api_update_project(project_id):
    """Update project"""
    try:
        project_data = request.json
        success = stream_manager.db.update_project(project_id, project_data)
        return jsonify({"success": success, "message": "Project updated successfully" if success else "Project not found"})
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {e}"})

@app.route('/api/projects/<project_id>', methods=['DELETE'])
def api_delete_project(project_id):
    """Delete project"""
    try:
        stream_manager.db.delete_project(project_id)
        return jsonify({"success": True, "message": "Project deleted successfully"})
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {e}"})

# Template Management APIs
@app.route('/api/templates', methods=['GET'])
def api_get_templates():
    """Get all stream templates"""
    templates = stream_manager.db.get_all_templates()
    return jsonify(templates)

@app.route('/api/templates', methods=['POST'])
def api_create_template():
    """Create new template"""
    try:
        template_data = request.json
        template_id = stream_manager.db.create_template(template_data)
        return jsonify({"success": True, "template_id": template_id, "message": "Template created successfully"})
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {e}"})

@app.route('/api/templates/<template_id>', methods=['GET'])
def api_get_template(template_id):
    """Get specific template"""
    try:
        template = stream_manager.db.get_template(template_id)
        if template:
            return jsonify(template)
        else:
            return jsonify({"success": False, "message": "Template not found"})
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {e}"})

@app.route('/api/templates/<template_id>', methods=['PUT'])
def api_update_template(template_id):
    """Update template"""
    try:
        template_data = request.json
        success = stream_manager.db.update_template(template_id, template_data)
        return jsonify({"success": success, "message": "Template updated successfully" if success else "Template not found"})
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {e}"})

@app.route('/api/templates/<template_id>', methods=['DELETE'])
def api_delete_template(template_id):
    """Delete template"""
    try:
        success = stream_manager.db.delete_template(template_id)
        return jsonify({"success": success, "message": "Template deleted successfully" if success else "Template not found"})
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {e}"})

@app.route('/api/streams/from-template', methods=['POST'])
def api_create_stream_from_template():
    """Create stream from template"""
    try:
        data = request.json
        template_id = data.get('template_id')
        stream_data = data.get('stream_data', {})
        
        stream_id = stream_manager.db.create_stream_from_template(template_id, stream_data)
        if stream_id:
            return jsonify({"success": True, "stream_id": stream_id, "message": "Stream created from template"})
        else:
            return jsonify({"success": False, "message": "Failed to create stream from template"})
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {e}"})

# Platform Management APIs

@app.route('/api/platforms', methods=['POST'])
def api_create_platform():
    """Create new platform configuration"""
    try:
        platform_data = request.json
        platform_id = stream_manager.db.create_platform_config(platform_data)
        return jsonify({"success": True, "platform_id": platform_id, "message": "Platform configuration created"})
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {e}"})

@app.route('/api/platforms/<platform_name>', methods=['GET'])
def api_get_platform(platform_name):
    """Get specific platform configuration"""
    try:
        platform = stream_manager.db.get_platform_config(platform_name)
        if platform:
            return jsonify(platform)
        else:
            return jsonify({"success": False, "message": "Platform not found"})
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {e}"})

@app.route('/api/platforms/<platform_name>', methods=['PUT'])
def api_update_platform(platform_name):
    """Update platform configuration"""
    try:
        platform_data = request.json
        success = stream_manager.db.update_platform_config(platform_name, platform_data)
        return jsonify({"success": success, "message": "Platform updated successfully" if success else "Platform not found"})
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {e}"})

@app.route('/api/platforms/<platform_name>', methods=['DELETE'])
def api_delete_platform(platform_name):
    """Delete platform configuration"""
    try:
        success = stream_manager.db.delete_platform_config(platform_name)
        return jsonify({"success": success, "message": "Platform deleted successfully" if success else "Platform not found"})
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {e}"})

# Multi-Stream Management APIs
@app.route('/api/streams/<stream_id>/multi-targets', methods=['POST'])
@requires_auth
def api_add_multi_stream_target(stream_id):
    """Add multi-stream target to a stream"""
    try:
        target_data = request.json
        
        # Get current stream
        stream = stream_manager.db.get_stream(stream_id)
        if not stream:
            return jsonify({"success": False, "message": "Stream not found"})
        
        # Add new target
        current_targets = stream.get('multi_stream_targets', [])
        if isinstance(current_targets, str):
            try:
                current_targets = json.loads(current_targets)
            except:
                current_targets = []
        
        current_targets.append(target_data)
        
        # Update stream
        success = stream_manager.db.update_stream(stream_id, {
            'multi_stream_targets': json.dumps(current_targets)
        })
        
        return jsonify({"success": success, "message": "Multi-stream target added" if success else "Failed to add target"})
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {e}"})

@app.route('/api/streams/<stream_id>/multi-targets', methods=['GET'])
@requires_auth
def api_get_multi_stream_targets(stream_id):
    """Get multi-stream targets for a stream"""
    try:
        stream = stream_manager.db.get_stream(stream_id)
        if not stream:
            return jsonify({"success": False, "message": "Stream not found"})
        
        targets = stream.get('multi_stream_targets', [])
        if isinstance(targets, str):
            try:
                targets = json.loads(targets)
            except:
                targets = []
        
        return jsonify(targets)
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {e}"})

@app.route('/api/streams/<stream_id>/multi-targets/<int:target_index>', methods=['DELETE'])
@requires_auth
def api_remove_multi_stream_target(stream_id, target_index):
    """Remove multi-stream target from a stream"""
    try:
        stream = stream_manager.db.get_stream(stream_id)
        if not stream:
            return jsonify({"success": False, "message": "Stream not found"})
        
        targets = stream.get('multi_stream_targets', [])
        if isinstance(targets, str):
            try:
                targets = json.loads(targets)
            except:
                targets = []
        
        if target_index < 0 or target_index >= len(targets):
            return jsonify({"success": False, "message": "Invalid target index"})
        
        targets.pop(target_index)
        
        # Update stream
        success = stream_manager.db.update_stream(stream_id, {
            'multi_stream_targets': json.dumps(targets)
        })
        
        return jsonify({"success": success, "message": "Multi-stream target removed" if success else "Failed to remove target"})
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {e}"})

# Audio Configuration APIs
@app.route('/api/streams/<stream_id>/audio', methods=['PUT'])
@requires_auth
def api_update_stream_audio(stream_id):
    """Update stream audio configuration"""
    try:
        audio_config = request.json

        success = stream_manager.db.update_stream(stream_id, {
            'audio_config': audio_config
        })
        
        return jsonify({"success": success, "message": "Audio configuration updated" if success else "Failed to update audio"})
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {e}"})

# Project stream management
@app.route('/api/projects/<project_id>/streams', methods=['GET'])
@requires_auth
def api_get_project_streams(project_id):
    """Get all streams for a project"""
    try:
        streams = stream_manager.db.get_project_streams(project_id)
        return jsonify(streams)
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {e}"})

@app.route('/api/streams/<stream_id>/start', methods=['POST'])
@requires_auth
def api_start_stream(stream_id):
    """Start specific stream"""
    success, message = stream_manager.start_stream(stream_id)
    return jsonify({"success": success, "message": message})

@app.route('/api/streams/<stream_id>/stop', methods=['POST'])
@requires_auth
def api_stop_stream(stream_id):
    """Stop specific stream"""
    success, message = stream_manager.stop_stream(stream_id)
    return jsonify({"success": success, "message": message})

@app.route('/api/streams/<stream_id>', methods=['PUT'])
@requires_auth
def api_update_stream(stream_id):
    """Update specific stream"""
    try:
        stream_data = request.json
        success, message = stream_manager.update_stream(stream_id, stream_data)
        return jsonify({"success": success, "message": message})
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {e}"})

@app.route('/api/streams/<stream_id>', methods=['DELETE'])
@requires_auth
def api_delete_stream(stream_id):
    """Delete specific stream"""
    success, message = stream_manager.delete_stream(stream_id)
    return jsonify({"success": success, "message": message})

@app.route('/api/metrics/<stream_id>', methods=['GET'])
@requires_auth
def api_get_stream_metrics(stream_id):
    """Get metrics for specific stream"""
    try:
        minutes = request.args.get('minutes', 30, type=int)
        metrics = stream_manager.db.get_recent_metrics(stream_id, minutes)
        return jsonify(metrics)
    except Exception as e:
        return jsonify({"error": f"Error retrieving metrics: {e}"})

@app.route('/api/alerts', methods=['GET'])
@requires_auth
def api_get_alerts():
    """Get all unacknowledged alerts"""
    try:
        stream_id = request.args.get('stream_id')
        alerts = stream_manager.db.get_stream_alerts(stream_id, acknowledged=False)
        return jsonify(alerts)
    except Exception as e:
        return jsonify({"error": f"Error retrieving alerts: {e}"})

@app.route('/api/alerts/<int:alert_id>/acknowledge', methods=['POST'])
@requires_auth
def api_acknowledge_alert(alert_id):
    """Acknowledge an alert"""
    try:
        # TODO: Implement alert acknowledgment
        return jsonify({"success": True, "message": "Alert acknowledged"})
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {e}"})

@app.route('/api/analytics/overview', methods=['GET'])
@requires_auth
def api_analytics_overview():
    """Get analytics overview for all streams"""
    try:
        streams = stream_manager.get_all_streams()
        total_streams = len(streams)
        active_streams = len([s for s in streams if s['status'] == 'live'])
        
        # Calculate total uptime
        total_uptime_seconds = sum(s.get('uptime_seconds', 0) for s in streams)
        total_uptime_hours = total_uptime_seconds / 3600
        
        # Get recent alerts count
        alerts = stream_manager.db.get_stream_alerts(acknowledged=False)
        
        # Get recovery statistics
        recovery_stats = stream_manager.db.get_recovery_stats()
        
        overview = {
            'total_streams': total_streams,
            'active_streams': active_streams,
            'total_uptime_hours': round(total_uptime_hours, 1),
            'active_alerts': len(alerts),
            'system_status': 'operational' if len(alerts) == 0 else 'issues_detected',
            'recovery_stats': recovery_stats
        }
        
        return jsonify(overview)
    except Exception as e:
        return jsonify({"error": f"Error retrieving overview: {e}"})

@app.route('/api/health/<stream_id>', methods=['GET'])
def api_get_stream_health(stream_id):
    """Get health score for specific stream"""
    try:
        health = stream_manager.db.get_latest_health(stream_id)
        return jsonify(health or {'health_score': 100, 'connection_quality': 100, 'performance_score': 100, 'stability_score': 100})
    except Exception as e:
        return jsonify({"error": f"Error retrieving health: {e}"})

@app.route('/api/recovery/<stream_id>', methods=['GET'])
def api_get_stream_recovery(stream_id):
    """Get recovery statistics for specific stream"""
    try:
        recovery_stats = stream_manager.db.get_recovery_stats(stream_id)
        active_recovery = stream_manager.db.get_active_recovery(stream_id)
        
        return jsonify({
            'stats': recovery_stats,
            'active_recovery': active_recovery
        })
    except Exception as e:
        return jsonify({"error": f"Error retrieving recovery data: {e}"})

@app.route('/api/streams/<stream_id>/recover', methods=['POST'])
@requires_auth
def api_manual_recovery(stream_id):
    """Manually trigger recovery for a specific stream"""
    try:
        with stream_lock:
            if stream_id not in stream_manager.active_streams:
                return jsonify({"success": False, "message": "Stream not active"})
            
            stream_instance = stream_manager.active_streams[stream_id]
            
            if stream_instance.recovery_in_progress:
                return jsonify({"success": False, "message": "Recovery already in progress"})
            
            # Detect current issues
            failure_types = stream_instance.detect_failure_type()
            if not failure_types or failure_types == ['unknown_failure']:
                # Force a diagnostic recovery
                failure_types = ['manual_recovery']
            
            # Attempt recovery
            success, message = stream_instance.attempt_recovery(failure_types)
            
            return jsonify({
                "success": success, 
                "message": message,
                "failure_types": failure_types
            })
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {e}"})

@app.route('/api/platforms', methods=['GET'])
@requires_auth
def api_get_platforms():
    """Get all available streaming platforms"""
    try:
        platforms = stream_manager.db.get_platform_configs()
        return jsonify(platforms)
    except Exception as e:
        return jsonify({"error": f"Error retrieving platforms: {e}"})

# Handle graceful shutdown
def signal_handler(sig, frame):
    logger.info("Shutting down stream manager...")
    stream_manager.cleanup_all()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == '__main__':
    try:
        # Start stream monitoring
        stream_manager.start_monitoring()
        
        # Run Flask app
        logger.info("Starting RTMP Stream Manager on port 5000")
        app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
        
    except KeyboardInterrupt:
        stream_manager.cleanup_all()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        stream_manager.cleanup_all()
