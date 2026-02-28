#!/usr/bin/env python3
"""
Advanced Telegram Bot with Content Management System & Force Join
Secure, Scalable, and Production-Ready
Author: Senior Telegram Bot Developer

Upgraded with:
- Refer & Earn
- View Earnings
- Multi‑Payment Withdrawal System
- Enterprise Admin Panel for Withdrawals
"""

import os
import logging
import sqlite3
import asyncio
import uuid
import re
import html
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple, Optional, Union
from enum import Enum

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    BotCommand,
    BotCommandScopeChat,
    BotCommandScopeDefault,
    Chat,
    Message,
    ChatMember
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
    ConversationHandler
)
from telegram.constants import ParseMode, ChatType, ChatMemberStatus
from telegram.error import BadRequest, Forbidden, TelegramError

# ========================
# CONFIGURATION
# ========================
# ⚠️ REPLACE THESE WITH YOUR ACTUAL VALUES
BOT_TOKEN = "8310083635:AAFd4PcX8UGbJ7PKhAwJ6PobXVF2kRiUp9E"  # ⚠️ CHANGE THIS!
ADMIN_IDS = [
    8212844361,
    8488305795,
    8577263306,
    8370998743
]  # ⚠️ CHANGE THIS!
BACKUP_CHANNEL_ID = -1003759520562  # ⚠️ CHANGE THIS! (Private admin-only channel)

# ⚠️ FORCE JOIN CHANNELS - REPLACE THESE!
FORCE_JOIN_CHANNELS = [
    # {"id": -1002872721854, "link": "https://t.me/workunlimited", "title": "Test Channel 🎫"},
    {"id": -1003898547093, "link": "https://t.me/+A4MtsN7swhM3NzM1", "title": "News Channel "},
]

DATABASE_NAME = "content_bot.db"
# Changed from constant to variable that can be modified
AUTO_DELETE_SECONDS = 3600  # Default: 1 hour auto-delete

# Timezone for timestamps
TIMEZONE = timezone.utc  # Use UTC timezone

# Reward defaults (will be loaded from DB)
REFERRAL_REWARD = 0.01
VIEW_REWARD = 0.01
MIN_WITHDRAWAL = 1.00

# Conversation states for payment setup
(SET_PAYMENT_METHOD, SET_PAYMENT_DETAILS) = range(2)

# ========================
# ENUMS & CONSTANTS
# ========================
class ContentType(Enum):
    FILE = "file"
    VIDEO = "video"
    AUDIO = "audio"
    PHOTO = "photo"
    TEXT = "text"

class MaintenanceMode(Enum):
    ON = "ON"
    OFF = "OFF"

class ProtectionMode(Enum):
    PROTECTED = "protected"
    UNPROTECTED = "unprotected"

# ========================
# LOGGING SETUP
# ========================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ========================
# HTML ESCAPE HELPER
# ========================
def escape_html(text: str) -> str:
    """Escape text for safe use in HTML parse mode."""
    if text is None:
        return ""
    return html.escape(str(text))

# ========================
# NEW HELPER FUNCTIONS (added)
# ========================
def format_delete_time(seconds: int) -> str:
    """Convert seconds to human readable format like '1h 30m', '5m', '30s'."""
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    minutes = minutes % 60
    if minutes == 0:
        return f"{hours}h"
    return f"{hours}h {minutes}m"

def format_amount(value: float) -> str:
    """Format a monetary amount, stripping unnecessary trailing zeros."""
    # Show up to 6 decimal places, then remove trailing zeros and dot
    return f"{value:.6f}".rstrip('0').rstrip('.')

# ========================
# DATABASE MANAGER (MODULAR) - EXTENDED
# ========================
class DatabaseManager:
    """Modular database manager for all operations."""
    
    def __init__(self, db_name: str = DATABASE_NAME):
        self.db_name = db_name
        self.init_database()
    
    def get_connection(self):
        """Get database connection with thread safety."""
        conn = sqlite3.connect(self.db_name, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    
    def init_database(self):
        """Initialize database with all required tables."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Users table (simplified)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    has_joined_all_channels INTEGER DEFAULT 0,
                    is_banned INTEGER DEFAULT 0,
                    ban_reason TEXT,
                    banned_by INTEGER,
                    ban_date TIMESTAMP
                )
            ''')
            
            # Add referred_by column if not exists (for referral system)
            cursor.execute("PRAGMA table_info(users)")
            columns = [col[1] for col in cursor.fetchall()]
            if 'referred_by' not in columns:
                cursor.execute("ALTER TABLE users ADD COLUMN referred_by INTEGER DEFAULT NULL")
            
            # Contents table with comprehensive metadata
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS contents (
                    content_id TEXT PRIMARY KEY,
                    telegram_file_id TEXT,
                    text_data TEXT,
                    content_type TEXT NOT NULL,
                    uploader_user_id INTEGER NOT NULL,
                    uploader_username TEXT,
                    upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    auto_delete_time INTEGER DEFAULT 3600,
                    backup_message_id INTEGER,
                    protection_mode TEXT DEFAULT 'protected',
                    FOREIGN KEY (uploader_user_id) REFERENCES users(user_id)
                )
            ''')
            
            # Settings table for bot configuration
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS settings (
                    setting_name TEXT PRIMARY KEY,
                    setting_value TEXT NOT NULL
                )
            ''')
            
            # Initialize settings (including new ones)
            settings_to_init = [
                ('maintenance_mode', 'OFF'),
                ('auto_delete_time', '3600'),
                ('referral_reward', '0.01'),
                ('view_reward', '0.01'),
                ('min_withdrawal', '1.00')
            ]
            
            for setting_name, setting_value in settings_to_init:
                cursor.execute('''
                    INSERT OR IGNORE INTO settings (setting_name, setting_value)
                    VALUES (?, ?)
                ''', (setting_name, setting_value))
            
            # New tables for earning system
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS referrals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    referrer_id INTEGER NOT NULL,
                    referred_user_id INTEGER UNIQUE NOT NULL,
                    reward_amount REAL DEFAULT 0.01,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (referrer_id) REFERENCES users(user_id),
                    FOREIGN KEY (referred_user_id) REFERENCES users(user_id)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS content_views (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    content_id TEXT NOT NULL,
                    viewer_user_id INTEGER NOT NULL,
                    viewed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(content_id, viewer_user_id),
                    FOREIGN KEY (content_id) REFERENCES contents(content_id),
                    FOREIGN KEY (viewer_user_id) REFERENCES users(user_id)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS earnings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    source_type TEXT NOT NULL,   -- 'referral' or 'view'
                    source_id TEXT NOT NULL,      -- referral id or content id
                    amount REAL NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_payments (
                    user_id INTEGER PRIMARY KEY,
                    payment_method TEXT NOT NULL,
                    payment_details TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS withdrawals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    payment_method TEXT NOT NULL,
                    payment_details TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',  -- pending, completed, rejected
                    requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed_at TIMESTAMP,
                    processed_by INTEGER,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("Database initialized/upgraded successfully")
        except Exception as e:
            logger.error(f"Database initialization error: {e}")
    
    # ---------------------- Existing methods (unchanged) ----------------------
    def add_user(self, user_id: int, username: str = None):
        """Add or update user in database."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO users (user_id, username, join_date, has_joined_all_channels, is_banned)
                VALUES (?, ?, COALESCE(
                    (SELECT join_date FROM users WHERE user_id = ?),
                    CURRENT_TIMESTAMP
                ), ?, COALESCE(
                    (SELECT is_banned FROM users WHERE user_id = ?),
                    0
                ))
            ''', (user_id, username, user_id, 0, user_id))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error adding user: {e}")
    
    def update_user_channel_status(self, user_id: int, has_joined: bool):
        """Update user's channel join status."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE users SET has_joined_all_channels = ? WHERE user_id = ?
            ''', (1 if has_joined else 0, user_id))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error updating user channel status: {e}")
    
    def get_user(self, user_id: int) -> Optional[Dict]:
        """Get user data from database."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
            row = cursor.fetchone()
            conn.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting user: {e}")
            return None
    
    def is_user_banned(self, user_id: int) -> bool:
        """Check if user is banned."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT is_banned FROM users WHERE user_id = ?', (user_id,))
            result = cursor.fetchone()
            conn.close()
            return bool(result[0]) if result else False
        except Exception as e:
            logger.error(f"Error checking if user is banned: {e}")
            return False
    
    def ban_user(self, user_id: int, banned_by: int, reason: str = "No reason provided"):
        """Ban a user from uploading content."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE users 
                SET is_banned = 1, 
                    ban_reason = ?,
                    banned_by = ?,
                    ban_date = CURRENT_TIMESTAMP
                WHERE user_id = ?
            ''', (reason, banned_by, user_id))
            
            if cursor.rowcount == 0:
                cursor.execute('''
                    INSERT INTO users (user_id, is_banned, ban_reason, banned_by, ban_date)
                    VALUES (?, 1, ?, ?, CURRENT_TIMESTAMP)
                ''', (user_id, reason, banned_by))
            
            conn.commit()
            conn.close()
            logger.info(f"User {user_id} banned by {banned_by}. Reason: {reason}")
            return True
        except Exception as e:
            logger.error(f"Error banning user: {e}")
            return False
    
    def unban_user(self, user_id: int):
        """Unban a user."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE users 
                SET is_banned = 0,
                    ban_reason = NULL,
                    banned_by = NULL,
                    ban_date = NULL
                WHERE user_id = ?
            ''', (user_id,))
            conn.commit()
            conn.close()
            logger.info(f"User {user_id} unbanned")
            return True
        except Exception as e:
            logger.error(f"Error unbanning user: {e}")
            return False
    
    def get_banned_users(self) -> List[Dict]:
        """Get list of all banned users."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT user_id, username, ban_reason, banned_by, ban_date 
                FROM users 
                WHERE is_banned = 1 
                ORDER BY ban_date DESC
            ''')
            rows = cursor.fetchall()
            conn.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting banned users: {e}")
            return []
    
    def add_content(self, content_data: Dict) -> str:
        """Add content to database and return generated content_id."""
        try:
            content_id = str(uuid.uuid4())[:12]  # Short unique ID
            
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO contents (
                    content_id, telegram_file_id, text_data, content_type,
                    uploader_user_id, uploader_username, upload_timestamp,
                    auto_delete_time, protection_mode
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                content_id,
                content_data.get('telegram_file_id'),
                content_data.get('text_data'),
                content_data.get('content_type'),
                content_data.get('uploader_user_id'),
                content_data.get('uploader_username'),
                content_data.get('upload_timestamp', datetime.now(TIMEZONE)),
                content_data.get('auto_delete_time', AUTO_DELETE_SECONDS),
                content_data.get('protection_mode', ProtectionMode.PROTECTED.value)
            ))
            conn.commit()
            conn.close()
            
            logger.info(f"Content added with ID: {content_id}")
            return content_id
        except Exception as e:
            logger.error(f"Error adding content: {e}")
            raise
    
    def get_content(self, content_id: str) -> Optional[Dict]:
        """Get content by ID."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM contents WHERE content_id = ?', (content_id,))
            row = cursor.fetchone()
            conn.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting content: {e}")
            return None
    
    def delete_content(self, content_id: str, user_id: int) -> bool:
        """Delete content if user is the uploader or admin."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('SELECT uploader_user_id FROM contents WHERE content_id = ?', (content_id,))
            content = cursor.fetchone()
            
            if not content:
                return False
            
            uploader_id = content[0]
            if user_id != uploader_id and user_id not in ADMIN_IDS:
                return False
            
            cursor.execute('DELETE FROM contents WHERE content_id = ?', (content_id,))
            conn.commit()
            conn.close()
            
            logger.info(f"Content {content_id} deleted by user {user_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting content: {e}")
            return False
    
    def update_backup_message_id(self, content_id: str, message_id: int):
        """Update backup channel message ID for content."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE contents SET backup_message_id = ? WHERE content_id = ?',
                (message_id, content_id)
            )
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error updating backup message ID: {e}")
    
    def get_user_contents(self, user_id: int) -> List[Dict]:
        """Get all contents uploaded by a user."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(
                'SELECT * FROM contents WHERE uploader_user_id = ? ORDER BY upload_timestamp DESC',
                (user_id,)
            )
            rows = cursor.fetchall()
            conn.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting user contents: {e}")
            return []
    
    def get_content_stats_by_user(self, user_id: int) -> Dict:
        """Get content statistics for a specific user."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT content_type, COUNT(*) as count 
                FROM contents 
                WHERE uploader_user_id = ? 
                GROUP BY content_type
            ''', (user_id,))
            
            stats = {}
            for row in cursor.fetchall():
                stats[row[0]] = row[1]
            
            conn.close()
            return stats
        except Exception as e:
            logger.error(f"Error getting user stats: {e}")
            return {}
    
    def get_global_stats(self) -> Dict:
        """Get global bot statistics."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            stats = {}
            
            # Total users
            cursor.execute('SELECT COUNT(*) FROM users')
            stats['total_users'] = cursor.fetchone()[0]
            
            # Users who joined all channels
            cursor.execute('SELECT COUNT(*) FROM users WHERE has_joined_all_channels = 1')
            stats['verified_users'] = cursor.fetchone()[0]
            
            # Banned users
            cursor.execute('SELECT COUNT(*) FROM users WHERE is_banned = 1')
            stats['banned_users'] = cursor.fetchone()[0]
            
            # Total contents
            cursor.execute('SELECT COUNT(*) FROM contents')
            stats['total_contents'] = cursor.fetchone()[0]
            
            # Contents by type
            cursor.execute('''
                SELECT content_type, COUNT(*) 
                FROM contents 
                GROUP BY content_type
            ''')
            stats['contents_by_type'] = dict(cursor.fetchall())
            
            # Contents by protection
            cursor.execute('''
                SELECT protection_mode, COUNT(*) 
                FROM contents 
                GROUP BY protection_mode
            ''')
            stats['contents_by_protection'] = dict(cursor.fetchall())
            
            # New earnings stats
            cursor.execute('SELECT SUM(amount) FROM earnings')
            stats['total_earnings'] = cursor.fetchone()[0] or 0.0
            
            cursor.execute('SELECT COUNT(*) FROM referrals')
            stats['total_referrals'] = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM content_views')
            stats['total_views'] = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM withdrawals')
            stats['total_withdrawals'] = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM withdrawals WHERE status = "pending"')
            stats['pending_withdrawals'] = cursor.fetchone()[0]
            
            conn.close()
            return stats
        except Exception as e:
            logger.error(f"Error getting global stats: {e}")
            return {}
    
    def get_maintenance_mode(self) -> str:
        """Get current maintenance mode status."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(
                'SELECT setting_value FROM settings WHERE setting_name = ?',
                ('maintenance_mode',)
            )
            result = cursor.fetchone()
            conn.close()
            return result[0] if result else 'OFF'
        except Exception as e:
            logger.error(f"Error getting maintenance mode: {e}")
            return 'OFF'
    
    def set_maintenance_mode(self, mode: str):
        """Set maintenance mode status."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO settings (setting_name, setting_value)
                VALUES (?, ?)
            ''', ('maintenance_mode', mode))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error setting maintenance mode: {e}")
    
    def get_auto_delete_time(self) -> int:
        """Get auto-delete time in seconds."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(
                'SELECT setting_value FROM settings WHERE setting_name = ?',
                ('auto_delete_time',)
            )
            result = cursor.fetchone()
            conn.close()
            return int(result[0]) if result else AUTO_DELETE_SECONDS
        except Exception as e:
            logger.error(f"Error getting auto-delete time: {e}")
            return AUTO_DELETE_SECONDS
    
    def set_auto_delete_time(self, seconds: int):
        """Set auto-delete time in seconds."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO settings (setting_name, setting_value)
                VALUES (?, ?)
            ''', ('auto_delete_time', str(seconds)))
            conn.commit()
            conn.close()
            
            global AUTO_DELETE_SECONDS
            AUTO_DELETE_SECONDS = seconds
            
            logger.info(f"Auto-delete time set to {seconds} seconds ({seconds//3600} hours)")
        except Exception as e:
            logger.error(f"Error setting auto-delete time: {e}")
    
    # ---------------------- NEW METHODS for Earning System ----------------------
    
    def get_setting(self, name: str, default: str = None) -> str:
        """Get a setting value."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT setting_value FROM settings WHERE setting_name = ?', (name,))
            result = cursor.fetchone()
            conn.close()
            return result[0] if result else default
        except Exception as e:
            logger.error(f"Error getting setting {name}: {e}")
            return default
    
    def set_setting(self, name: str, value: str):
        """Set a setting value."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO settings (setting_name, setting_value)
                VALUES (?, ?)
            ''', (name, value))
            conn.commit()
            conn.close()
            logger.info(f"Setting {name} set to {value}")
        except Exception as e:
            logger.error(f"Error setting {name}: {e}")
    
    def process_referral(self, referrer_id: int, referred_user_id: int) -> bool:
        """
        Process a referral.
        Returns True if reward was given, False otherwise.
        """
        if referrer_id == referred_user_id:
            return False
        
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            # Check if referred user already exists in referrals
            cursor.execute('SELECT 1 FROM referrals WHERE referred_user_id = ?', (referred_user_id,))
            if cursor.fetchone():
                return False
            
            # Check if referrer exists (if not, add them)
            cursor.execute('INSERT OR IGNORE INTO users (user_id) VALUES (?)', (referrer_id,))
            
            # Get reward amount from settings
            reward = float(self.get_setting('referral_reward', '0.01'))
            
            # Insert referral
            cursor.execute('''
                INSERT INTO referrals (referrer_id, referred_user_id, reward_amount)
                VALUES (?, ?, ?)
            ''', (referrer_id, referred_user_id, reward))
            
            # Add earning for referrer
            cursor.execute('''
                INSERT INTO earnings (user_id, source_type, source_id, amount)
                VALUES (?, 'referral', ?, ?)
            ''', (referrer_id, str(referred_user_id), reward))
            
            # Update referred_by in users table
            cursor.execute('UPDATE users SET referred_by = ? WHERE user_id = ?', (referrer_id, referred_user_id))
            
            conn.commit()
            logger.info(f"Referral reward: {reward} to {referrer_id} for {referred_user_id}")
            return True
        except Exception as e:
            conn.rollback()
            logger.error(f"Error processing referral: {e}")
            return False
        finally:
            conn.close()
    
    def record_view(self, content_id: str, viewer_user_id: int) -> Optional[float]:
        """
        Record a unique view for content.
        Returns the reward amount for uploader if new view, else None.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            # Get content info
            cursor.execute('SELECT uploader_user_id FROM contents WHERE content_id = ?', (content_id,))
            content = cursor.fetchone()
            if not content:
                return None
            uploader_id = content[0]
            
            # No self-view reward
            if uploader_id == viewer_user_id:
                return None
            
            # Check if this view already exists
            cursor.execute('''
                SELECT 1 FROM content_views WHERE content_id = ? AND viewer_user_id = ?
            ''', (content_id, viewer_user_id))
            if cursor.fetchone():
                return None
            
            # Get view reward from settings
            reward = float(self.get_setting('view_reward', '0.01'))
            
            # Insert view record
            cursor.execute('''
                INSERT INTO content_views (content_id, viewer_user_id)
                VALUES (?, ?)
            ''', (content_id, viewer_user_id))
            
            # Add earning for uploader
            cursor.execute('''
                INSERT INTO earnings (user_id, source_type, source_id, amount)
                VALUES (?, 'view', ?, ?)
            ''', (uploader_id, content_id, reward))
            
            conn.commit()
            logger.info(f"View reward: {reward} to {uploader_id} for content {content_id} from {viewer_user_id}")
            return reward
        except Exception as e:
            conn.rollback()
            logger.error(f"Error recording view: {e}")
            return None
        finally:
            conn.close()
    
    def get_user_balance(self, user_id: int) -> float:
        """
        Calculate user's available balance (total earnings - completed withdrawals - pending withdrawals).
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            # Total earnings
            cursor.execute('SELECT COALESCE(SUM(amount), 0) FROM earnings WHERE user_id = ?', (user_id,))
            total_earned = cursor.fetchone()[0]
            
            # Total completed withdrawals
            cursor.execute('SELECT COALESCE(SUM(amount), 0) FROM withdrawals WHERE user_id = ? AND status = "completed"', (user_id,))
            total_withdrawn = cursor.fetchone()[0]
            
            # Total pending withdrawals
            cursor.execute('SELECT COALESCE(SUM(amount), 0) FROM withdrawals WHERE user_id = ? AND status = "pending"', (user_id,))
            total_pending = cursor.fetchone()[0]
            
            return total_earned - total_withdrawn - total_pending
        except Exception as e:
            logger.error(f"Error getting user balance: {e}")
            return 0.0
        finally:
            conn.close()
    
    def get_user_earnings_summary(self, user_id: int) -> Dict:
        """Get detailed earnings summary for a user."""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            # Referral earnings
            cursor.execute('''
                SELECT COALESCE(SUM(amount), 0) FROM earnings 
                WHERE user_id = ? AND source_type = 'referral'
            ''', (user_id,))
            referral_earnings = cursor.fetchone()[0]
            
            # View earnings
            cursor.execute('''
                SELECT COALESCE(SUM(amount), 0) FROM earnings 
                WHERE user_id = ? AND source_type = 'view'
            ''', (user_id,))
            view_earnings = cursor.fetchone()[0]
            
            # Total referrals count
            cursor.execute('SELECT COUNT(*) FROM referrals WHERE referrer_id = ?', (user_id,))
            total_referrals = cursor.fetchone()[0]
            
            # Total views (as uploader)
            cursor.execute('''
                SELECT COUNT(DISTINCT cv.id) FROM content_views cv
                JOIN contents c ON cv.content_id = c.content_id
                WHERE c.uploader_user_id = ?
            ''', (user_id,))
            total_views = cursor.fetchone()[0]
            
            # Total withdrawn
            cursor.execute('SELECT COALESCE(SUM(amount), 0) FROM withdrawals WHERE user_id = ? AND status = "completed"', (user_id,))
            total_withdrawn = cursor.fetchone()[0]
            
            # Available balance
            balance = self.get_user_balance(user_id)
            
            return {
                'referral_earnings': referral_earnings,
                'view_earnings': view_earnings,
                'total_earnings': referral_earnings + view_earnings,
                'total_referrals': total_referrals,
                'total_views': total_views,
                'total_withdrawn': total_withdrawn,
                'balance': balance
            }
        except Exception as e:
            logger.error(f"Error getting earnings summary: {e}")
            return {}
        finally:
            conn.close()
    
    def set_user_payment(self, user_id: int, method: str, details: str) -> bool:
        """Set or update user's payment method."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO user_payments (user_id, payment_method, payment_details, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ''', (user_id, method, details))
            conn.commit()
            conn.close()
            logger.info(f"Payment method set for user {user_id}: {method}")
            return True
        except Exception as e:
            logger.error(f"Error setting payment: {e}")
            return False
    
    def get_user_payment(self, user_id: int) -> Optional[Dict]:
        """Get user's payment method."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM user_payments WHERE user_id = ?', (user_id,))
            row = cursor.fetchone()
            conn.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting payment: {e}")
            return None
    
    def create_withdrawal(self, user_id: int, amount: float, method: str, details: str) -> Optional[int]:
        """
        Create a withdrawal request.
        Returns withdrawal ID if successful, else None.
        Uses transaction to ensure balance check.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            # Check balance
            balance = self.get_user_balance(user_id)
            if balance < amount:
                return None
            
            # Insert withdrawal record
            cursor.execute('''
                INSERT INTO withdrawals (user_id, amount, payment_method, payment_details, status)
                VALUES (?, ?, ?, ?, 'pending')
            ''', (user_id, amount, method, details))
            withdrawal_id = cursor.lastrowid
            
            conn.commit()
            logger.info(f"Withdrawal request #{withdrawal_id} created for user {user_id}, amount {amount}")
            return withdrawal_id
        except Exception as e:
            conn.rollback()
            logger.error(f"Error creating withdrawal: {e}")
            return None
        finally:
            conn.close()
    
    def get_pending_withdrawals(self) -> List[Dict]:
        """Get all pending withdrawals."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT w.*, u.username 
                FROM withdrawals w
                LEFT JOIN users u ON w.user_id = u.user_id
                WHERE w.status = 'pending'
                ORDER BY w.requested_at ASC
            ''')
            rows = cursor.fetchall()
            conn.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting pending withdrawals: {e}")
            return []
    
    def process_withdrawal(self, withdrawal_id: int, admin_id: int, status: str) -> bool:
        """
        Approve or reject a withdrawal.
        If rejected, refund the amount (no action needed as we never deducted).
        If approved, mark as completed.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT status FROM withdrawals WHERE id = ?', (withdrawal_id,))
            row = cursor.fetchone()
            if not row or row[0] != 'pending':
                return False
            
            cursor.execute('''
                UPDATE withdrawals 
                SET status = ?, processed_at = CURRENT_TIMESTAMP, processed_by = ?
                WHERE id = ?
            ''', (status, admin_id, withdrawal_id))
            
            conn.commit()
            logger.info(f"Withdrawal #{withdrawal_id} {status} by admin {admin_id}")
            return True
        except Exception as e:
            conn.rollback()
            logger.error(f"Error processing withdrawal: {e}")
            return False
        finally:
            conn.close()

# Initialize database manager
db = DatabaseManager()

# ========================
# FORCE JOIN SYSTEM (unchanged)
# ========================
async def check_channel_membership(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check if user is member of all required channels."""
    if not FORCE_JOIN_CHANNELS:
        return True
    
    for channel in FORCE_JOIN_CHANNELS:
        try:
            member = await context.bot.get_chat_member(
                chat_id=channel["id"],
                user_id=user_id
            )
            if member.status in [
                ChatMemberStatus.LEFT,
                ChatMemberStatus.BANNED,
                ChatMemberStatus.RESTRICTED
            ]:
                return False
        except (BadRequest, Forbidden) as e:
            logger.error(f"Error checking membership for channel {channel['id']}: {e}")
            return False
    return True

def create_join_keyboard() -> InlineKeyboardMarkup:
    """Create keyboard with join buttons for each required channel."""
    keyboard = []
    for channel in FORCE_JOIN_CHANNELS:
        keyboard.append([
            InlineKeyboardButton(
                f"👉 Join {channel['title']}",
                url=channel["link"]
            )
        ])
    keyboard.append([
        InlineKeyboardButton("✅ I've Joined - Check Now", callback_data="recheck_membership")
    ])
    return InlineKeyboardMarkup(keyboard)

async def require_channel_join(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check if user needs to join channels. Returns True if needs to join."""
    user = update.effective_user
    if user.id in ADMIN_IDS:
        return False
    
    has_joined = await check_channel_membership(user.id, context)
    if not has_joined:
        db.update_user_channel_status(user.id, False)
        join_message = (
            f"👋 Welcome {user.first_name}!\n\n"
            f"🔒 **Access Restricted**\n\n"
            f"To use this bot, you must join the required channels.\n\n"
            f"👇 Click the buttons below to join\n"
            f"✅ Then press **I've Joined – Check Now**\n\n"
            f"⚠️ You must stay in the channels to keep access."
        )
        if update.callback_query:
            await update.callback_query.edit_message_text(
                join_message,
                reply_markup=create_join_keyboard(),
                parse_mode=ParseMode.MARKDOWN
            )
        elif update.message:
            await update.message.reply_text(
                join_message,
                reply_markup=create_join_keyboard(),
                parse_mode=ParseMode.MARKDOWN
            )
        return True
    
    # --- UPGRADE 2: Process pending referral after verification ---
    db.update_user_channel_status(user.id, True)

    # Process referral ONLY after successful verification
    pending_referrer = context.user_data.get("pending_referrer")

    if pending_referrer:
        user_data = db.get_user(user.id)

        # Ensure referral not already processed
        if not user_data.get("referred_by"):
            db.process_referral(pending_referrer, user.id)

        # Clear session variable
        context.user_data.pop("pending_referrer", None)

    return False

# ========================
# SECURITY & ADMIN GUARD (unchanged)
# ========================
def admin_only(func):
    """Decorator to restrict command to admins only."""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        if user.id not in ADMIN_IDS:
            if update.message:
                await update.message.delete()
            elif update.callback_query:
                await update.callback_query.answer()
            return
        return await func(update, context, *args, **kwargs)
    return wrapper

async def check_maintenance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check if bot is in maintenance mode and handle user access."""
    user = update.effective_user
    if user.id in ADMIN_IDS:
        return False
    
    if db.get_maintenance_mode() == 'ON':
        maintenance_msg = (
            "🔧 **Maintenance Mode**\n\n"
            "The bot is currently undergoing maintenance.\n"
            "Please try again later.\n\n"
            "Thank you for your patience! ❤️"
        )
        if update.message:
            await update.message.reply_text(maintenance_msg, parse_mode=ParseMode.MARKDOWN)
        elif update.callback_query:
            await update.callback_query.answer(maintenance_msg, show_alert=True)
        return True
    return False

async def check_ban_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check if user is banned from uploading content."""
    user = update.effective_user
    if user.id in ADMIN_IDS:
        return False
    
    if db.is_user_banned(user.id):
        user_data = db.get_user(user.id)
        ban_reason = user_data.get('ban_reason', 'No reason provided')
        ban_date = user_data.get('ban_date', 'Unknown date')
        if isinstance(ban_date, str):
            ban_date_display = ban_date
        else:
            try:
                ban_date_display = ban_date.strftime('%Y-%m-%d %H:%M:%S')
            except:
                ban_date_display = str(ban_date)
        
        ban_msg = (
            "🚫 **Account Banned**\n\n"
            "Your account has been restricted from uploading content.\n\n"
            f"**Reason:** {ban_reason}\n"
            f"**Date:** {ban_date_display}\n\n"
            "If you believe this is a mistake, contact the administrator."
        )
        if update.message:
            await update.message.reply_text(ban_msg, parse_mode=ParseMode.MARKDOWN)
        elif update.callback_query:
            await update.callback_query.answer(ban_msg, show_alert=True)
        return True
    return False

# ========================
# COMMAND VISIBILITY SYSTEM (updated)
# ========================
async def set_command_scopes(application: Application):
    """Set different command scopes for users and admins."""
    # User commands (visible to everyone)
    user_commands = [
        BotCommand("start", "Start the bot"),
        BotCommand("get", "Get content by secret ID"),
        BotCommand("profile", "Your profile & earnings"),
        BotCommand("delete", "Delete your content"),
        BotCommand("withdraw", "Withdraw your earnings"),
        BotCommand("setpayment", "Set your payment method"),
        BotCommand("stats", "Bot Status (admin only)"),
        BotCommand("help", "This help message"),
    ]
    
    # Admin commands (only visible to admins)
    admin_commands = user_commands + [
        BotCommand("upload", "Upload content"),
        BotCommand("adms", "Broadcast"),
        BotCommand("maintenance", "Toggle maintenance mode"),
        BotCommand("settime", "Set auto-delete time"),
        BotCommand("ban", "Ban a user"),
        BotCommand("unban", "Unban a user"),
        BotCommand("banned", "List banned users"),
        BotCommand("withdrawals", "Manage withdrawals"),
        BotCommand("setreward", "Set referral/view reward"),
        BotCommand("setminwithdraw", "Set minimum withdrawal"),
        BotCommand("find", "Inspect any user (admin)"),
    ]
    
    try:
        await application.bot.set_my_commands(
            commands=user_commands,
            scope=BotCommandScopeDefault()
        )
        for admin_id in ADMIN_IDS:
            try:
                await application.bot.set_my_commands(
                    commands=admin_commands,
                    scope=BotCommandScopeChat(chat_id=admin_id)
                )
                logger.info(f"Admin commands set for user: {admin_id}")
            except Exception as e:
                logger.error(f"Failed to set admin commands for {admin_id}: {e}")
        logger.info("Command scopes set successfully")
    except Exception as e:
        logger.error(f"Error setting command scopes: {e}")

# ========================
# UPLOAD HANDLER (unchanged)
# ========================
async def handle_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle content upload from any user."""
    if await check_maintenance(update, context):
        return
    if await check_ban_status(update, context):
        return
    
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        if await require_channel_join(update, context):
            return
    
    message = update.message
    if not (message.photo or message.video or message.audio or message.document or message.text):
        await message.reply_text(
            "📤 **How to upload content:**\n\n"
            "Simply send me:\n"
            "• Any file (document)\n"
            "• Video\n"
            "• Audio\n"
            "• Photo\n"
            "• Text message\n\n"
            "I'll generate a unique Content ID that you can share!",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    try:
        db.add_user(user.id, user.username)
        
        content_data = {
            'uploader_user_id': user.id,
            'uploader_username': user.username,
            'upload_timestamp': datetime.now(TIMEZONE),
            'auto_delete_time': db.get_auto_delete_time(),
        }
        
        if message.text:
            content_data['content_type'] = ContentType.TEXT.value
            content_data['text_data'] = message.text
            content_data['telegram_file_id'] = None
        elif message.photo:
            content_data['content_type'] = ContentType.PHOTO.value
            content_data['telegram_file_id'] = message.photo[-1].file_id
            content_data['text_data'] = message.caption or ""
        elif message.video:
            content_data['content_type'] = ContentType.VIDEO.value
            content_data['telegram_file_id'] = message.video.file_id
            content_data['text_data'] = message.caption or ""
        elif message.audio:
            content_data['content_type'] = ContentType.AUDIO.value
            content_data['telegram_file_id'] = message.audio.file_id
            content_data['text_data'] = message.caption or ""
        elif message.document:
            content_data['content_type'] = ContentType.FILE.value
            content_data['telegram_file_id'] = message.document.file_id
            content_data['text_data'] = message.caption or ""
        
        context.user_data['pending_upload'] = content_data
        
        keyboard = [
            [
                InlineKeyboardButton("🔒 Protected", callback_data="protection_protected"),
                InlineKeyboardButton("🔓 Unprotected", callback_data="protection_unprotected")
            ],
            [InlineKeyboardButton("❌ Cancel Upload", callback_data="cancel_upload")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await message.reply_text(
            "🔒 **Protection Mode**\n\n"
            "How would you like to protect this content?\n\n"
            "**🔒 Protected** - Users cannot save/forward\n"
            "**🔓 Unprotected** - No restrictions\n\n"
            "Please choose an option below:",
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"Error handling upload: {e}")
        await message.reply_text(
            "❌ An error occurred while processing your upload. Please try again.",
            parse_mode=ParseMode.MARKDOWN
        )

async def complete_upload(update: Update, context: ContextTypes.DEFAULT_TYPE, content_data: Dict):
    """Complete the upload process after protection mode is selected."""
    try:
        user = update.effective_user
        content_id = db.add_content(content_data)
        
        # Forward to backup channel - with improved error handling inside the function
        backup_msg = await forward_to_backup_channel(update, context, content_id, content_data)
        if backup_msg:
            db.update_backup_message_id(content_id, backup_msg.message_id)
        
        if 'pending_upload' in context.user_data:
            del context.user_data['pending_upload']
        
        protection_emoji = "🔒" if content_data.get('protection_mode') == 'protected' else "🔓"
        protection_text = "Protected (no save/forward)" if content_data.get('protection_mode') == 'protected' else "Unprotected"
        auto_delete_str = format_delete_time(content_data.get('auto_delete_time', AUTO_DELETE_SECONDS))  # FIXED
        
        # --- UPGRADE 1: Replace secret ID with one‑click deep link ---
        bot_username = (await context.bot.get_me()).username
        content_link = f"https://t.me/{bot_username}?start={content_id}"
        
        confirmation_text = (
            f"✅ **Content uploaded successfully!**\n\n"
            
            f"🕵️ **Secret ID (Manual Access)**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"`{content_id}`\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            
            f"🔗 **One-Click Access Link**\n"
            f"{content_link}\n\n"
            
            f"📁 **Type:** {content_data['content_type']}\n"
            f"{protection_emoji} **Protection:** {protection_text}\n"
            f"🕒 **Uploaded:** {content_data['upload_timestamp'].strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
            f"⏰ **Auto-delete:** {auto_delete_str}\n\n"
            
            f"📥 Users can either:\n"
            f"• Use `/get {content_id}`\n"
            f"• Or click the link above\n\n"
            
            f"Share This Link with anyone for instant access safely 🚀"
        )
        
        # NEW: Add copy button
        copy_button = InlineKeyboardButton(
            "📋 Copy Link",
            callback_data=f"copy_{content_id}"
        )
        reply_markup = InlineKeyboardMarkup([[copy_button]])
        
        if update.callback_query:
            await update.callback_query.edit_message_text(
                confirmation_text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await context.bot.send_message(
                chat_id=user.id,
                text=confirmation_text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
    except Exception as e:
        logger.error(f"Error completing upload: {e}")
        if update.callback_query:
            await update.callback_query.edit_message_text(
                "❌ An error occurred while processing your upload. Please try again.",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await context.bot.send_message(
                chat_id=user.id,
                text="❌ An error occurred while processing your upload. Please try again.",
                parse_mode=ParseMode.MARKDOWN
            )

# ========================
# FIXED BACKUP FORWARDING FUNCTION (using HTML with escaped user data)
# ========================
async def forward_to_backup_channel(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                   content_id: str, content_data: Dict) -> Optional[Message]:
    """Forward content to admin-only backup channel with metadata."""
    try:
        user = update.effective_user
        # Ensure we have a user object
        if not user:
            logger.error("No effective user in update for backup forwarding")
            return None
            
        protection_emoji = "🔒" if content_data.get('protection_mode') == 'protected' else "🔓"
        
        # Escape user‑supplied fields for safe HTML
        safe_full_name = escape_html(user.full_name)
        safe_username = escape_html(user.username) if user.username else "Not set"
        safe_user_id = escape_html(str(user.id))
        safe_content_id = escape_html(content_id)
        safe_content_type = escape_html(content_data['content_type'])
        
        # Use HTML for reliable user tagging (handles special characters)
        metadata = (
            f"📋 CONTENT BACKUP\n\n"
            f"🕵️ Secret ID: <code>{safe_content_id}</code>\n"
            f"👤 Uploader: <a href='tg://user?id={user.id}'>{safe_full_name}</a>\n"
            f"🆔 User ID: <code>{safe_user_id}</code>\n"
            f"👤 Username: @{safe_username}\n"
            f"📁 Type: {safe_content_type}\n"
            f"⏰ Date & Time: {datetime.now(ZoneInfo('Asia/Dhaka')).strftime('%d %b %Y, %I:%M %p')}\n"
        )
        
        backup_msg = None
        
        if content_data['content_type'] == ContentType.TEXT.value:
            # For text, combine content and metadata
            safe_text_data = escape_html(content_data['text_data'])
            full_text = f"{safe_text_data}\n\n{metadata}"
            try:
                backup_msg = await context.bot.send_message(
                    chat_id=BACKUP_CHANNEL_ID,
                    text=full_text,
                    parse_mode=ParseMode.HTML
                )
                logger.info(f"Text backup sent to channel for content {content_id}")
            except Exception as e:
                logger.error(f"Failed to send text backup to channel with HTML: {e}")
                # Try without parse mode if HTML fails
                try:
                    backup_msg = await context.bot.send_message(
                        chat_id=BACKUP_CHANNEL_ID,
                        text=full_text
                    )
                    logger.info(f"Text backup sent (without HTML) for content {content_id}")
                except Exception as e2:
                    logger.error(f"Also failed without HTML: {e2}")
        else:
            # For media, send the media with metadata as caption
            try:
                if content_data['content_type'] == ContentType.PHOTO.value:
                    backup_msg = await context.bot.send_photo(
                        chat_id=BACKUP_CHANNEL_ID,
                        photo=content_data['telegram_file_id'],
                        caption=metadata,
                        parse_mode=ParseMode.HTML
                    )
                elif content_data['content_type'] == ContentType.VIDEO.value:
                    backup_msg = await context.bot.send_video(
                        chat_id=BACKUP_CHANNEL_ID,
                        video=content_data['telegram_file_id'],
                        caption=metadata,
                        parse_mode=ParseMode.HTML
                    )
                elif content_data['content_type'] == ContentType.AUDIO.value:
                    backup_msg = await context.bot.send_audio(
                        chat_id=BACKUP_CHANNEL_ID,
                        audio=content_data['telegram_file_id'],
                        caption=metadata,
                        parse_mode=ParseMode.HTML
                    )
                elif content_data['content_type'] == ContentType.FILE.value:
                    backup_msg = await context.bot.send_document(
                        chat_id=BACKUP_CHANNEL_ID,
                        document=content_data['telegram_file_id'],
                        caption=metadata,
                        parse_mode=ParseMode.HTML
                    )
                logger.info(f"Media backup sent to channel for content {content_id}")
            except Exception as e:
                logger.error(f"Failed to send media backup to channel with HTML: {e}")
                # Try without parse mode
                try:
                    if content_data['content_type'] == ContentType.PHOTO.value:
                        backup_msg = await context.bot.send_photo(
                            chat_id=BACKUP_CHANNEL_ID,
                            photo=content_data['telegram_file_id'],
                            caption=metadata
                        )
                    elif content_data['content_type'] == ContentType.VIDEO.value:
                        backup_msg = await context.bot.send_video(
                            chat_id=BACKUP_CHANNEL_ID,
                            video=content_data['telegram_file_id'],
                            caption=metadata
                        )
                    elif content_data['content_type'] == ContentType.AUDIO.value:
                        backup_msg = await context.bot.send_audio(
                            chat_id=BACKUP_CHANNEL_ID,
                            audio=content_data['telegram_file_id'],
                            caption=metadata
                        )
                    elif content_data['content_type'] == ContentType.FILE.value:
                        backup_msg = await context.bot.send_document(
                            chat_id=BACKUP_CHANNEL_ID,
                            document=content_data['telegram_file_id'],
                            caption=metadata
                        )
                    logger.info(f"Media backup sent (without HTML) for content {content_id}")
                except Exception as e2:
                    logger.error(f"Also failed without HTML: {e2}")
        
        return backup_msg
        
    except Exception as e:
        logger.error(f"Unexpected error in forward_to_backup_channel: {e}")
        return None

# ========================
# CONTENT DELIVERY SYSTEM (updated with view tracking)
# ========================
async def get_content_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /get command to retrieve content by ID."""
    if await check_maintenance(update, context):
        return
    
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        if await require_channel_join(update, context):
            return
    
    if not context.args:
        await update.message.reply_text(
            "🕵️ **Access via Secret ID**\n\n"
            "Usage: `/get <secret_id>`\n\n"
            "Example: `/get abc123xyz456`\n\n"
            f"The content will auto-delete after {format_delete_time(AUTO_DELETE_SECONDS)}.",  # FIXED
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    content_id = context.args[0].strip()
    
    try:
        content = db.get_content(content_id)
        if not content:
            await update.message.reply_text(
                "❌ **Content not found!**\n\n"
                "The provided **Secret ID** is invalid or has expired.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        content_type_names = {
            'file': '📄 File',
            'video': '🎬 Video',
            'audio': '🎵 Audio',
            'photo': '🖼 Photo',
            'text': '📝 Text'
        }
        content_type_display = content_type_names.get(content['content_type'], content['content_type'])
        protection_mode = content.get('protection_mode', 'protected')
        protect_content = protection_mode == 'protected'
        protection_text = "🔒 Protected (no save/forward)" if protect_content else "🔓 Unprotected"
        
        # Record view and reward uploader if unique
        view_reward = db.record_view(content_id, user.id)
        
        caption = (
            f"{content_type_display}\n\n"
            f"{protection_text}\n\n"
            f"⚠️ **This message will auto-delete in {format_delete_time(AUTO_DELETE_SECONDS)}**"  # FIXED
        )
        
        send_methods = {
            'text': lambda: update.message.reply_text(
                f"**📝 Text Content**\n\n{content['text_data']}\n\n{caption}",
                parse_mode=ParseMode.MARKDOWN
            ),
            'photo': lambda: context.bot.send_photo(
                chat_id=user.id,
                photo=content['telegram_file_id'],
                caption=caption,
                parse_mode=ParseMode.MARKDOWN,
                protect_content=protect_content
            ),
            'video': lambda: context.bot.send_video(
                chat_id=user.id,
                video=content['telegram_file_id'],
                caption=caption,
                parse_mode=ParseMode.MARKDOWN,
                protect_content=protect_content
            ),
            'audio': lambda: context.bot.send_audio(
                chat_id=user.id,
                audio=content['telegram_file_id'],
                caption=caption,
                parse_mode=ParseMode.MARKDOWN,
                protect_content=protect_content
            ),
            'file': lambda: context.bot.send_document(
                chat_id=user.id,
                document=content['telegram_file_id'],
                caption=caption,
                parse_mode=ParseMode.MARKDOWN,
                protect_content=protect_content
            )
        }
        
        sent_message = await send_methods[content['content_type']]()
        
        if hasattr(sent_message, 'message_id'):
            asyncio.create_task(
                delete_message_after_delay(context, user.id, sent_message.message_id, AUTO_DELETE_SECONDS)
            )
        
        # Include view reward info if earned
        reply_text = (
            f"✅ **Content sent!**\n\n"
            f"📁 Type: {content_type_display}\n"
            f"{protection_text}\n"
            f"⏰ Auto-delete: {format_delete_time(AUTO_DELETE_SECONDS)}\n\n"  # FIXED
        )
        if view_reward:
            reply_text += f"💰 Upload & earn **${format_amount(view_reward)}** per view!"  # FIXED
        else:
            reply_text += "Check your chat with me for the content."
        
        await update.message.reply_text(reply_text, parse_mode=ParseMode.MARKDOWN)
        
    except Exception as e:
        logger.error(f"Error getting content: {e}")
        await update.message.reply_text(
            "❌ An error occurred while retrieving the content. Please try again.",
            parse_mode=ParseMode.MARKDOWN
        )

async def delete_message_after_delay(context: ContextTypes.DEFAULT_TYPE, 
                                    chat_id: int, 
                                    message_id: int, 
                                    delay_seconds: int):
    """Delete a message after specified delay."""
    try:
        await asyncio.sleep(delay_seconds)
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        logger.info(f"Auto-deleted message {message_id} after {delay_seconds}s")
    except Exception as e:
        logger.error(f"Error auto-deleting message: {e}")

# ========================
# DELETE CONTENT COMMAND (unchanged)
# ========================
async def delete_content_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /delete command to delete user's own content."""
    if await check_maintenance(update, context):
        return
    
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        if await require_channel_join(update, context):
            return
    
    if not context.args:
        await update.message.reply_text(
            "🗑️ **Delete Content**\n\n"
            "Usage: `/delete <content_id>`\n\n"
            "Example: `/delete abc123xyz456`\n\n"
            "You can only delete content that you uploaded.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    content_id = context.args[0].strip()
    
    try:
        success = db.delete_content(content_id, user.id)
        if success:
            await update.message.reply_text(
                f"✅ **Content deleted successfully!**\n\n"
                f"Content ID: `{content_id}`\n"
                f"The content has been permanently removed from the bot.",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text(
                "❌ **Cannot delete content!**\n\n"
                "Possible reasons:\n"
                "• Content ID not found\n"
                "• You are not the uploader\n"
                "• Content already deleted",
                parse_mode=ParseMode.MARKDOWN
            )
    except Exception as e:
        logger.error(f"Error deleting content: {e}")
        await update.message.reply_text(
            "❌ An error occurred while deleting the content. Please try again.",
            parse_mode=ParseMode.MARKDOWN
        )

# ========================
# PROFILE COMMAND (UPGRADED with earnings)
# ========================
def escape_md(text):
    """Escape special characters for Telegram MarkdownV1."""
    if text is None:
        return ""
    escape_chars = r'_*`[]()'
    return ''.join(f'\\{c}' if c in escape_chars else c for c in str(text))

async def profile_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /profile command to show user statistics and earnings."""
    if await check_maintenance(update, context):
        return
    
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        if await require_channel_join(update, context):
            return
    
    try:
        user_data = db.get_user(user.id)
        if not user_data:
            db.add_user(user.id, user.username)
            user_data = db.get_user(user.id)
        
        # Get earnings summary
        earnings = db.get_user_earnings_summary(user.id)
        
        # Get payment method
        payment = db.get_user_payment(user.id)
        payment_info = "Not set"
        if payment:
            method = payment['payment_method']
            details = payment['payment_details']
            # Mask details (show only last 4 chars)
            masked = details[-4:] if len(details) > 4 else details
            payment_info = f"{method} - ...{masked}"
        
        # Channel status
        if user.id in ADMIN_IDS:
            channel_status = "👑 Admin (bypass)"
        else:
            channel_status = "✅ Verified" if user_data.get('has_joined_all_channels') else "❌ Not verified"
        
        ban_status = "🚫 BANNED" if db.is_user_banned(user.id) else "✅ Active"
        
        # Content stats
        user_contents = db.get_user_contents(user.id)
        total_uploads = len(user_contents)
        content_stats = db.get_content_stats_by_user(user.id)
        stats_text = ""
        for content_type in ContentType:
            count = content_stats.get(content_type.value, 0)
            if count > 0:
                emoji = {'file':'📄','video':'🎬','audio':'🎵','photo':'🖼','text':'📝'}.get(content_type.value,'📁')
                stats_text += f"{emoji} {content_type.value.title()}: {count}\n"
        
        # Recent content IDs
        recent_ids = [content['content_id'] for content in user_contents[:10]]
        
        # Build profile message
        profile_msg = (
            f"👤 **Your Profile**\n\n"
            f"🆔 **User ID:** `{user.id}`\n"
            f"👤 **Username:** @{escape_md(user.username) or 'Not set'}\n"
            f"📅 **Join Date:** {escape_md(user_data.get('join_date', 'N/A'))}\n"
            f"📢 **Channel Status:** {channel_status}\n"
            f"🚫 **Account Status:** {ban_status}\n\n"
            f"💰 **Earnings Summary**\n"
            f"📊 **Total Balance:** `${format_amount(earnings.get('balance', 0))}`\n"  # FIXED
            f"• Referral Earnings: `${format_amount(earnings.get('referral_earnings', 0))}`\n"  # FIXED
            f"• View Earnings: `${format_amount(earnings.get('view_earnings', 0))}`\n"  # FIXED
            f"• Total Withdrawn: `${format_amount(earnings.get('total_withdrawn', 0))}`\n"  # FIXED
            f"👥 **Total Referrals:** {earnings.get('total_referrals', 0)}\n"
            f"👁 **Total Paid Views:** {earnings.get('total_views', 0)}\n\n"
            f"💳 **Current Payment Method:**\n`{payment_info}`\n\n"
            f"🔗 **Your Referral Link:**\n"
            f"`https://t.me/{(await context.bot.get_me()).username}?start={user.id}`\n\n"
            f"📊 **Upload Statistics**\n"
            f"📈 **Total Uploads:** {total_uploads}\n"
        )
        if stats_text:
            profile_msg += f"\n**Breakdown by Type:**\n{stats_text}"
        if recent_ids:
            profile_msg += f"\n**Recent Content IDs:**\n"
            for content_id in recent_ids:
                profile_msg += f"• `{content_id}`\n"
            if len(user_contents) > 10:
                profile_msg += f"\n... and {len(user_contents) - 10} more"
        
        if db.is_user_banned(user.id):
            profile_msg += f"\n\n🚫 **Ban Information:**\n"
            profile_msg += f"• **Reason:** {escape_md(user_data.get('ban_reason', 'No reason'))}\n"
            profile_msg += f"• **Date:** {escape_md(user_data.get('ban_date', 'Unknown'))}"
        
        keyboard = []
        if total_uploads > 0:
            keyboard.append([InlineKeyboardButton("📥 View All Uploads", callback_data=f"view_uploads_{user.id}")])
        keyboard.append([InlineKeyboardButton("💳 Set Payment Method", callback_data="set_payment")])
        keyboard.append([InlineKeyboardButton("💰 Withdraw", callback_data="withdraw_help")])
        reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
        
        await update.message.reply_text(
            profile_msg,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"Error in profile command: {e}")
        await update.message.reply_text(
            "❌ An error occurred while loading your profile.",
            parse_mode=ParseMode.MARKDOWN
        )

# ========================
# NEW COMMANDS: REFERRAL HANDLER (in /start)
# ========================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command with referral."""
    user = update.effective_user
    db.add_user(user.id, user.username)
    
    # --- UPGRADE 2: Modified referral handling ---
    if context.args and len(context.args) > 0:
        arg = context.args[0].strip()

        # Case 1: Referral (numeric only)
        if arg.isdigit():
            referrer_id = int(arg)

            if referrer_id != user.id and db.get_user(referrer_id):
                # Store referral temporarily (DO NOT reward yet)
                context.user_data["pending_referrer"] = referrer_id

        # Case 2: Content deep link
        else:
            context.args = [arg]
            await get_content_command(update, context)
            return
    
    # Force join check (unless admin)
    if user.id not in ADMIN_IDS:
        if await require_channel_join(update, context):
            return
    
    # Fetch reward settings
    referral_reward = float(db.get_setting('referral_reward', '0.01'))
    view_reward = float(db.get_setting('view_reward', '0.01'))
    
    welcome_msg = (
    f"👋 Hii {user.first_name}!\n\n"
    f"🚀 *Welcome to Kaizen X Share*\n\n"
    
    f"📤 *Upload & Share Instantly*\n"
    f"• Files, Videos, Audio\n"
    f"• Photos & Text\n\n"
    
    f"🆔 *Private Secret ID System*\n"
    f"• Get a unique Secret ID\n"
    f"• Share securely with anyone\n"
    f"• Auto-deletes after {format_delete_time(AUTO_DELETE_SECONDS)}\n\n"  # FIXED
    
    f"💰 *Earn Real Rewards*\n"
    f"• Referral Bonus: ${format_amount(referral_reward)} per user\n"  # FIXED
    f"• View Reward: ${format_amount(view_reward)} per unique view\n\n"  # FIXED
    
    f"📌 *Main Commands*\n"
    f"• /get — Access content via Secret ID\n"
    f"• /profile — View earnings & uploads\n"
    f"• /delete — Remove your content\n"
    f"• /withdraw — Request payout\n"
    f"• /setpayment — Set payout method\n"
    f"• /help — Help & guide\n\n"
    
    f"✨ Simply send any content to get started!"
)
    
    keyboard = [
        [InlineKeyboardButton("📤 Upload Content", callback_data="upload_help")],
        [InlineKeyboardButton("👤 My Profile", callback_data="profile")],
        [InlineKeyboardButton("❓ Help", callback_data="help")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        welcome_msg,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command."""
    if await check_maintenance(update, context):
        return
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        if await require_channel_join(update, context):
            return
    
    referral_reward = float(db.get_setting('referral_reward', '0.01'))
    view_reward = float(db.get_setting('view_reward', '0.01'))
    min_withdrawal = float(db.get_setting('min_withdrawal', '1.00'))
    
    help_msg = (
        "❓ **Help & Guide**\n\n"
        "**📤 How to Upload:**\n"
        "1. Send me any content (file, video, audio, photo, or text)\n"
        "2. I'll generate a unique **Secret ID**\n"
        "3. Share this ID with anyone\n\n"
        "**📥 How to Retrieve:**\n"
        "1. Use `/get <secret_id>`\n"
        f"2. It auto-deletes after {format_delete_time(AUTO_DELETE_SECONDS)}\n\n"  # FIXED
        "**💰 Earnings:**\n"
        f"• Referral: `${format_amount(referral_reward)}` per new user\n"  # FIXED
        f"• View: `${format_amount(view_reward)}` per view of your content\n"  # FIXED
        f"• Minimum withdrawal: `${min_withdrawal:.2f}`\n\n"  # Keep .2f for withdrawal amount
        "**💳 Payment Methods:**\n"
        "• Binance (UID or Email)\n"
        "• PayPal (Email)\n"
        "• TRX (TRC20 wallet)\n"
        "• BEP20 (USDT/BNB wallet)\n\n"
        "Use `/setpayment` to set your method.\n\n"
        "**🗑️ How to Delete:**\n"
        "Use `/delete <secret_id>` (only your own)\n\n"
        "**👤 Your Profile:**\n"
        "`/profile` shows your earnings, referral link, and stats.\n\n"
        f"📢 **Channel Requirements:**\n"
        f"You must join {len(FORCE_JOIN_CHANNELS)} channel(s) to use the bot."
    )
    await update.message.reply_text(help_msg, parse_mode=ParseMode.MARKDOWN)

# ========================
# PAYMENT SETUP COMMAND (Conversation)
# ========================
async def setpayment_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start the payment setup process."""
    user = update.effective_user
    if await check_maintenance(update, context) or await check_ban_status(update, context):
        return ConversationHandler.END
    if user.id not in ADMIN_IDS and await require_channel_join(update, context):
        return ConversationHandler.END
    
    keyboard = [
        [InlineKeyboardButton("Binance", callback_data="pay_binance")],
        [InlineKeyboardButton("PayPal", callback_data="pay_paypal")],
        [InlineKeyboardButton("TRX (TRC20)", callback_data="pay_trx")],
        [InlineKeyboardButton("BEP20", callback_data="pay_bep20")],
        [InlineKeyboardButton("❌ Cancel", callback_data="pay_cancel")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "💳 **Set Payment Method**\n\n"
        "Please choose your preferred payment method:",
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )
    return SET_PAYMENT_METHOD

async def setpayment_method(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle payment method selection."""
    query = update.callback_query
    await query.answer()
    
    method_map = {
        'pay_binance': 'Binance',
        'pay_paypal': 'PayPal',
        'pay_trx': 'TRX (TRC20)',
        'pay_bep20': 'BEP20'
    }
    method = method_map.get(query.data)
    if not method:
        await query.edit_message_text("❌ Setup cancelled.")
        return ConversationHandler.END
    
    context.user_data['payment_method'] = method
    
    prompt = {
        'Binance': "Please send your **Binance UID or Email**.",
        'PayPal': "Please send your **PayPal Email**.",
        'TRX (TRC20)': "Please send your **TRX (TRC20) wallet address**.",
        'BEP20': "Please send your **BEP20 wallet address** (for USDT/BNB)."
    }[method]
    
    await query.edit_message_text(
        f"💳 **{method}**\n\n{prompt}\n\n"
        f"Send the details as a text message.\n"
        f"Type /cancel to abort.",
        parse_mode=ParseMode.MARKDOWN
    )
    return SET_PAYMENT_DETAILS

async def setpayment_details(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Receive and store payment details."""
    user = update.effective_user
    details = update.message.text.strip()
    method = context.user_data.get('payment_method')
    
    if not method:
        await update.message.reply_text("❌ Session expired. Please start again with /setpayment.")
        return ConversationHandler.END
    
    # Basic validation
    valid = False
    if method == 'Binance':
        # Simple check: if it looks like email or numeric UID
        if '@' in details or details.isdigit():
            valid = True
    elif method == 'PayPal':
        if '@' in details and '.' in details:
            valid = True
    elif method in ('TRX (TRC20)', 'BEP20'):
        # Simple length check (typical wallet addresses are 34-42 chars)
        if len(details) >= 30 and len(details) <= 50:
            valid = True
    
    if not valid:
        await update.message.reply_text(
            "❌ Invalid format. Please check and try again.\n"
            "Send /setpayment to restart."
        )
        return SET_PAYMENT_DETAILS
    
    # Store in database
    if db.set_user_payment(user.id, method, details):
        await update.message.reply_text(
            f"✅ **Payment method saved!**\n\n"
            f"**Method:** {method}\n"
            f"**Details:** `{details}`\n\n"
            f"You can now use /withdraw to request payout.",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text("❌ Failed to save. Please try again later.")
    
    context.user_data.pop('payment_method', None)
    return ConversationHandler.END

async def setpayment_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancel payment setup."""
    if update.callback_query:
        await update.callback_query.edit_message_text("❌ Setup cancelled.")
    else:
        await update.message.reply_text("❌ Setup cancelled.")
    context.user_data.pop('payment_method', None)
    return ConversationHandler.END

# ========================
# WITHDRAW COMMAND
# ========================
async def withdraw_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /withdraw command."""
    user = update.effective_user
    if await check_maintenance(update, context) or await check_ban_status(update, context):
        return
    if user.id not in ADMIN_IDS and await require_channel_join(update, context):
        return
    
    # Check if payment method is set
    payment = db.get_user_payment(user.id)
    if not payment:
        await update.message.reply_text(
            "❌ **No payment method set!**\n\n"
            "Please set your payment method first using /setpayment.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Check balance
    balance = db.get_user_balance(user.id)
    min_withdrawal = float(db.get_setting('min_withdrawal', '1.00'))
    
    if balance < min_withdrawal:
        await update.message.reply_text(
            f"❌ **Insufficient balance for withdrawal.**\n\n"
            f"Your available balance: **${balance:.2f}**\n"
            f"Minimum withdrawal: **${min_withdrawal:.2f}**\n\n"
            f"Earn more by referring friends and uploading content!",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # If amount provided as argument
    if context.args:
        try:
            amount = float(context.args[0])
            if amount < min_withdrawal:
                await update.message.reply_text(f"❌ Minimum withdrawal is ${min_withdrawal:.2f}.")
                return
            if amount > balance:
                await update.message.reply_text(f"❌ Insufficient balance. You have ${balance:.2f} available.")
                return
            
            # Create withdrawal
            withdrawal_id = db.create_withdrawal(user.id, amount, payment['payment_method'], payment['payment_details'])
            if withdrawal_id:
                await update.message.reply_text(
                    f"✅ **Withdrawal request submitted!**\n\n"
                    f"**Amount:** ${amount:.2f}\n"
                    f"**Payment Method:** {payment['payment_method']}\n"
                    f"**Details:** `{payment['payment_details']}`\n"
                    f"**Status:** Pending\n\n"
                    f"Your request ID: `{withdrawal_id}`\n\n"
                    f"Admins will process it soon. You'll be notified.",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text("❌ Failed to create withdrawal. Please try again.")
        except ValueError:
            await update.message.reply_text("❌ Invalid amount. Please enter a number.")
    else:
        # Ask for amount
        await update.message.reply_text(
            f"💰 **Withdraw Funds**\n\n"
            f"Your available balance: **${balance:.2f}**\n"
            f"Minimum withdrawal: **${min_withdrawal:.2f}**\n\n"
            f"**Payment Method:** {payment['payment_method']}\n"
            f"**Details:** `{payment['payment_details']}`\n\n"
            f"To request withdrawal, use:\n"
            f"`/withdraw <amount>`\n\n"
            f"Example: `/withdraw {min_withdrawal}`",
            parse_mode=ParseMode.MARKDOWN
        )

# ========================
# ADMIN WITHDRAWAL MANAGEMENT (UPGRADED)
# ========================
@admin_only
async def withdrawals_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /withdrawals command (admin only) - complete management panel."""
    args = context.args
    page = 0
    status_filter = "pending"
    user_filter = None
    min_amount = None
    max_amount = None

    # Parse arguments
    for arg in args:
        if arg.isdigit() and len(arg) > 5:
            user_filter = int(arg)
        elif arg.lower() in ["pending", "approved", "rejected", "all"]:
            status_filter = arg.lower()
        elif arg.replace('.', '', 1).isdigit():
            if min_amount is None:
                min_amount = float(arg)
            else:
                max_amount = float(arg)

    if args and args[-1].isdigit() and len(args[-1]) <= 3:
        page = int(args[-1])

    # Build dynamic query
    conn = db.get_connection()
    cursor = conn.cursor()

    query = '''
        SELECT w.*, u.username
        FROM withdrawals w
        LEFT JOIN users u ON w.user_id = u.user_id
        WHERE 1=1
    '''
    params = []

    if status_filter != "all":
        query += " AND w.status = ?"
        params.append(status_filter)

    if user_filter:
        query += " AND w.user_id = ?"
        params.append(user_filter)

    if min_amount is not None:
        query += " AND w.amount >= ?"
        params.append(min_amount)

    if max_amount is not None:
        query += " AND w.amount <= ?"
        params.append(max_amount)

    query += " ORDER BY w.requested_at DESC"

    cursor.execute(query, tuple(params))
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()

    if not results:
        await update.message.reply_text("❌ No matching withdrawals found.")
        return

    # Pagination
    per_page = 10
    total_pages = (len(results) - 1) // per_page + 1
    page = max(0, min(page, total_pages - 1))

    start = page * per_page
    end = start + per_page
    withdrawals_slice = results[start:end]

    for w in withdrawals_slice:
        buttons = []
        if w["status"] == "pending":
            buttons = [
                [
                    InlineKeyboardButton("✅ Approve", callback_data=f"wd_approve_{w['id']}"),
                    InlineKeyboardButton("❌ Reject", callback_data=f"wd_reject_{w['id']}")
                ]
            ]

        keyboard = InlineKeyboardMarkup(buttons) if buttons else None

        # Escape username to prevent Markdown breakage
        username = w.get('username', 'N/A')
        if username != 'N/A':
            username = escape_md(username)

        text = (
            f"💰 *Withdrawal #{w['id']}*\n\n"
            f"👤 User ID: `{w['user_id']}`\n"
            f"👤 Username: @{username}\n"
            f"💵 Amount: `${w['amount']:.2f}`\n"
            f"📊 Status: *{w['status'].upper()}*\n"
            f"💳 Method: {w['payment_method']}\n"
            f"📝 Details: `{w['payment_details']}`\n"
            f"📅 Requested: {w['requested_at']}"
        )

        await update.message.reply_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=keyboard
        )

    # Navigation buttons
    nav = []

    if page > 0:
        nav.append(InlineKeyboardButton("⬅ Previous", callback_data=f"wd_page_{page-1}"))

    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("Next ➡", callback_data=f"wd_page_{page+1}"))

    if nav:
        await update.message.reply_text(
            f"📄 Page {page+1}/{total_pages}",
            reply_markup=InlineKeyboardMarkup([nav])
        )

@admin_only
async def handle_withdrawal_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle withdrawal approval/rejection and pagination."""
    query = update.callback_query
    await query.answer()

    data = query.data.split("_")

    if data[1] == "page":
        # Pagination: call withdrawals_command with page number
        context.args = [data[2]]
        await withdrawals_command(update, context)
        return

    action = data[1]
    withdrawal_id = int(data[2])

    success = db.process_withdrawal(
        withdrawal_id,
        query.from_user.id,
        "completed" if action == "approve" else "rejected"
    )

    if not success:
        await query.edit_message_text("⚠️ Already processed or invalid withdrawal.")
        return

    # Fetch withdrawal info for user notification
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, amount FROM withdrawals WHERE id = ?", (withdrawal_id,))
    row = cursor.fetchone()
    conn.close()

    if row:
        user_id = row["user_id"]
        amount = row["amount"]

        try:
            msg = (
                f"🎉 Withdrawal Approved!\n\n💵 Amount: ${amount:.2f}"
                if action == "approve"
                else f"❌ Withdrawal Rejected\n\n💵 Amount: ${amount:.2f}"
            )

            await context.bot.send_message(chat_id=user_id, text=msg)
        except Exception as e:
            logger.error(f"Failed to notify user {user_id}: {e}")

    await query.edit_message_text(
        f"💰 Withdrawal #{withdrawal_id}\n\n"
        f"{'✅ Approved' if action == 'approve' else '❌ Rejected'}\n"
        f"Processed by admin {query.from_user.id}"
    )

# ========================
# ADMIN REWARD SETTINGS
# ========================
@admin_only
async def setreward_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /setreward <referral|view> <amount>."""
    if len(context.args) != 2:
        await update.message.reply_text(
            "Usage: `/setreward referral 0.02`\n"
            "       `/setreward view 0.015`",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    reward_type = context.args[0].lower()
    try:
        amount = float(context.args[1])
        if amount < 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Invalid amount. Must be a positive number.")
        return
    
    if reward_type == 'referral':
        db.set_setting('referral_reward', str(amount))
        await update.message.reply_text(f"✅ Referral reward set to `${format_amount(amount)}`.")  # FIXED
    elif reward_type == 'view':
        db.set_setting('view_reward', str(amount))
        await update.message.reply_text(f"✅ View reward set to `${format_amount(amount)}`.")  # FIXED
    else:
        await update.message.reply_text("❌ Type must be 'referral' or 'view'.")

@admin_only
async def setminwithdraw_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /setminwithdraw <amount>."""
    if len(context.args) != 1:
        await update.message.reply_text("Usage: `/setminwithdraw 2.0`", parse_mode=ParseMode.MARKDOWN)
        return
    
    try:
        amount = float(context.args[0])
        if amount < 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Invalid amount.")
        return
    
    db.set_setting('min_withdrawal', str(amount))
    await update.message.reply_text(f"✅ Minimum withdrawal set to `${amount:.2f}`.")

# ========================
# ADMIN STATS COMMAND (UPGRADED)
# ========================
@admin_only
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /stats command (admin only) - global statistics."""
    try:
        stats = db.get_global_stats()
        maintenance_mode = db.get_maintenance_mode()
        auto_delete_time = db.get_auto_delete_time()
        
        content_stats_text = ""
        contents_by_type = stats.get('contents_by_type', {})
        for content_type, count in contents_by_type.items():
            emoji = {'file':'📄','video':'🎬','audio':'🎵','photo':'🖼','text':'📝'}.get(content_type,'📁')
            content_stats_text += f"{emoji} {content_type.title()}: {count}\n"
        
        protection_stats_text = ""
        contents_by_protection = stats.get('contents_by_protection', {})
        for protection_mode, count in contents_by_protection.items():
            emoji = "🔒" if protection_mode == 'protected' else "🔓"
            protection_stats_text += f"{emoji} {protection_mode.title()}: {count}\n"
        
        channels_info = "\n".join(
            [f"• {channel['title']} (ID: {channel['id']})" for channel in FORCE_JOIN_CHANNELS]
        ) if FORCE_JOIN_CHANNELS else "No channels configured"
        
        # New stats
        total_earnings = stats.get('total_earnings', 0)
        total_referrals = stats.get('total_referrals', 0)
        total_views = stats.get('total_views', 0)
        total_withdrawals = stats.get('total_withdrawals', 0)
        pending_withdrawals = stats.get('pending_withdrawals', 0)
        
        stats_msg = (
            f"📊 **Bot Statistics**\n\n"
            f"👥 **Total Users:** {stats.get('total_users', 0)}\n"
            f"✅ **Verified Users:** {stats.get('verified_users', 0)}\n"
            f"🚫 **Banned Users:** {stats.get('banned_users', 0)}\n"
            f"📁 **Total Contents:** {stats.get('total_contents', 0)}\n"
            f"👑 **Active Admins:** {len(ADMIN_IDS)}\n"
            f"🔧 **Maintenance Mode:** {maintenance_mode}\n"
            f"⏰ **Auto-delete Time:** {format_delete_time(auto_delete_time)}\n\n"  # FIXED
            f"💰 **Earnings Distributed:** `${total_earnings:.2f}`\n"
            f"👥 **Total Referrals:** {total_referrals}\n"
            f"👁 **Total Paid Views:** {total_views}\n"
            f"💳 **Total Withdrawals:** {total_withdrawals}\n"
            f"⏳ **Pending Withdrawals:** {pending_withdrawals}\n\n"
        )
        
        if content_stats_text:
            stats_msg += f"**Contents by Type:**\n{content_stats_text}\n"
        if protection_stats_text:
            stats_msg += f"**Contents by Protection:**\n{protection_stats_text}\n"
        
        stats_msg += (
            f"📢 **Force Join Channels:**\n{channels_info}\n\n"
            f"🔒 **Backup Channel:** {'✅ Connected' if BACKUP_CHANNEL_ID else '❌ Not set'}"
        )
        
        await update.message.reply_text(stats_msg, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Error in stats command: {e}")
        await update.message.reply_text("❌ An error occurred while fetching statistics.")

# ========================
# ADMIN COMMANDS (MISSING) - ADDED BACK
# ========================
@admin_only
async def maintenance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /maintenance command (admin only)."""
    current_mode = db.get_maintenance_mode()
    if not context.args:
        await update.message.reply_text(
            f"🔧 **Maintenance Mode**\n\n"
            f"Current status: **{current_mode}**\n\n"
            f"Usage: `/maintenance <ON|OFF>`\n"
            f"Example: `/maintenance ON`\n"
            f"Example: `/maintenance OFF`",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    new_mode = context.args[0].upper()
    if new_mode not in ['ON', 'OFF']:
        await update.message.reply_text("❌ Invalid mode. Use 'ON' or 'OFF'.", parse_mode=ParseMode.MARKDOWN)
        return
    db.set_maintenance_mode(new_mode)
    status_text = "🔴 ACTIVATED" if new_mode == 'ON' else "🟢 DEACTIVATED"
    message_text = f"✅ **Maintenance Mode {status_text}**\n\n"
    if new_mode == 'ON':
        message_text += "Normal users will now see maintenance message.\nAdmins can still use all commands."
    else:
        message_text += "All users can now access the bot normally."
    await update.message.reply_text(message_text, parse_mode=ParseMode.MARKDOWN)

@admin_only
async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /broadcast command (admin only)."""
    if not context.args and not update.message.reply_to_message:
        await update.message.reply_text(
            "📢 **Broadcast Message**\n\n"
            "Usage: `/broadcast <message>`\n"
            "Or reply to a message with `/broadcast`\n\n"
            "This will send your message to all bot users.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT user_id FROM users')
        users = cursor.fetchall()
        conn.close()
        total_users = len(users)
        if total_users == 0:
            await update.message.reply_text("❌ No users found in database.")
            return
        status_msg = await update.message.reply_text(f"📤 Starting broadcast to {total_users} users...\n⏳ Please wait...")
        success_count = 0
        fail_count = 0
        if update.message.reply_to_message:
            replied = update.message.reply_to_message
            for user_tuple in users:
                try:
                    await replied.forward(chat_id=user_tuple[0])
                    success_count += 1
                except:
                    fail_count += 1
        else:
            broadcast_text = " ".join(context.args)
            for user_tuple in users:
                try:
                    await context.bot.send_message(
                        chat_id=user_tuple[0],
                        text=f"⚠️ **Important Notice**\n\n{broadcast_text}",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    success_count += 1
                except:
                    fail_count += 1
        await status_msg.edit_text(
            f"✅ **Broadcast Complete!**\n\n"
            f"📊 **Statistics:**\n"
            f"• Total users: {total_users}\n"
            f"• ✅ Successful: {success_count}\n"
            f"• ❌ Failed: {fail_count}\n"
            f"• 📈 Success rate: {(success_count/total_users*100):.1f}%\n\n"
            f"{'🎉 All messages sent successfully!' if fail_count == 0 else '⚠️ Some messages failed to deliver.'}",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"Error in broadcast command: {e}")
        await update.message.reply_text("❌ An error occurred during broadcast.", parse_mode=ParseMode.MARKDOWN)

@admin_only
async def settime_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /settime command (admin only) - set auto-delete time."""
    current_time = db.get_auto_delete_time()
    if not context.args:
        await update.message.reply_text(
            f"⏰ **Auto-Delete Time**\n\n"
            f"Current setting: **{format_delete_time(current_time)}** ({current_time} seconds)\n\n"  # FIXED
            f"Usage: `/settime <hours>`\n"
            f"Example: `/settime 2` (for 2 hours)\n"
            f"Example: `/settime 0.5` (for 30 minutes)\n\n"
            f"Note: Changes apply to new uploads only.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    try:
        hours = float(context.args[0])
        if hours <= 0:
            await update.message.reply_text("❌ Time must be greater than 0.")
            return
        seconds = int(hours * 3600)
        db.set_auto_delete_time(seconds)
        await update.message.reply_text(
            f"✅ **Auto-delete time updated!**\n\n"
            f"New setting: **{format_delete_time(seconds)}** ({seconds} seconds)\n"  # FIXED
            f"This will apply to all new uploads.\n\n"
            f"Existing content will use their original settings.",
            parse_mode=ParseMode.MARKDOWN
        )
    except ValueError:
        await update.message.reply_text("❌ Invalid time format. Use a number (e.g., 1, 2, 0.5).")
    except Exception as e:
        logger.error(f"Error in settime command: {e}")
        await update.message.reply_text("❌ An error occurred while setting time.")

@admin_only
async def ban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /ban command - ban a user from uploading content."""
    if not context.args and not update.message.reply_to_message:
        await update.message.reply_text(
            "🚫 **Ban User**\n\n"
            "Usage: `/ban <user_id> [reason]`\n"
            "Or reply to a user's message with `/ban [reason]`\n\n"
            "Examples:\n"
            "• `/ban 123456789` (default reason)\n"
            "• `/ban 123456789 Spam uploads`\n"
            "• Reply to message: `/ban Violating terms`\n\n"
            "Banned users cannot upload new content.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    user = update.effective_user
    target_user_id = None
    reason = "No reason provided"
    if update.message.reply_to_message:
        target_user_id = update.message.reply_to_message.from_user.id
        if context.args:
            reason = " ".join(context.args)
    else:
        try:
            target_user_id = int(context.args[0])
            if len(context.args) > 1:
                reason = " ".join(context.args[1:])
        except ValueError:
            await update.message.reply_text("❌ Invalid user ID. User ID must be a number.", parse_mode=ParseMode.MARKDOWN)
            return
    if not target_user_id:
        await update.message.reply_text("❌ Could not identify user to ban.", parse_mode=ParseMode.MARKDOWN)
        return
    if target_user_id == user.id:
        await update.message.reply_text("❌ You cannot ban yourself!")
        return
    if target_user_id in ADMIN_IDS:
        await update.message.reply_text("❌ You cannot ban another admin!")
        return
    try:
        target_user = await context.bot.get_chat(target_user_id)
        username = target_user.username
        user_full_name = target_user.full_name
    except:
        username = "Unknown"
        user_full_name = f"User {target_user_id}"
    success = db.ban_user(target_user_id, user.id, reason)
    if success:
        await update.message.reply_text(
            f"✅ **User banned successfully!**\n\n"
            f"👤 **User:** {user_full_name}\n"
            f"🆔 **ID:** `{target_user_id}`\n"
            f"🚫 **Reason:** {reason}\n"
            f"👑 **Banned by:** {user.first_name}\n\n"
            f"This user can no longer upload content.",
            parse_mode=ParseMode.MARKDOWN
        )
        try:
            await context.bot.send_message(
                chat_id=target_user_id,
                text=f"🚫 **Account Banned**\n\n"
                     f"Your account has been banned from uploading content.\n\n"
                     f"**Reason:** {reason}\n"
                     f"**Banned by:** Admin\n\n"
                     f"If you believe this is a mistake, contact the administrator.",
                parse_mode=ParseMode.MARKDOWN
            )
        except:
            pass
    else:
        await update.message.reply_text("❌ Failed to ban user. Please try again.", parse_mode=ParseMode.MARKDOWN)

@admin_only
async def unban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /unban command - unban a user."""
    if not context.args and not update.message.reply_to_message:
        await update.message.reply_text(
            "✅ **Unban User**\n\n"
            "Usage: `/unban <user_id>`\n"
            "Or reply to a message with `/unban`\n\n"
            "Examples:\n"
            "• `/unban 123456789`\n"
            "• Reply to message: `/unban`\n\n"
            "Unbanned users can upload content again.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    user = update.effective_user
    target_user_id = None
    if update.message.reply_to_message:
        target_user_id = update.message.reply_to_message.from_user.id
    else:
        try:
            target_user_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("❌ Invalid user ID. User ID must be a number.", parse_mode=ParseMode.MARKDOWN)
            return
    if not target_user_id:
        await update.message.reply_text("❌ Could not identify user to unban.", parse_mode=ParseMode.MARKDOWN)
        return
    if not db.is_user_banned(target_user_id):
        await update.message.reply_text(f"ℹ️ User `{target_user_id}` is not currently banned.", parse_mode=ParseMode.MARKDOWN)
        return
    success = db.unban_user(target_user_id)
    if success:
        await update.message.reply_text(
            f"✅ **User unbanned successfully!**\n\n"
            f"🆔 **User ID:** `{target_user_id}`\n"
            f"👑 **Unbanned by:** {user.first_name}\n\n"
            f"This user can now upload content again.",
            parse_mode=ParseMode.MARKDOWN
        )
        try:
            await context.bot.send_message(
                chat_id=target_user_id,
                text="✅ **Account Unbanned**\n\n"
                     "Your account has been unbanned.\n"
                     "You can now upload content again.",
                parse_mode=ParseMode.MARKDOWN
            )
        except:
            pass
    else:
        await update.message.reply_text("❌ Failed to unban user. Please try again.", parse_mode=ParseMode.MARKDOWN)

@admin_only
async def banned_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /banned command - list all banned users."""
    try:
        banned_users = db.get_banned_users()
        if not banned_users:
            await update.message.reply_text(
                "✅ **No banned users found.**\n\n"
                "All users are currently allowed to upload content.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        banned_list_msg = "🚫 **Banned Users List**\n\n"
        for i, banned_user in enumerate(banned_users, 1):
            user_id = banned_user['user_id']
            username = banned_user['username'] or "No username"
            ban_reason = banned_user['ban_reason'] or "No reason"
            banned_by = banned_user['banned_by'] or "Unknown"
            ban_date = banned_user['ban_date'] or "Unknown"
            if isinstance(ban_date, str):
                ban_date_display = ban_date
            else:
                try:
                    ban_date_display = ban_date.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    ban_date_display = str(ban_date)
            banned_list_msg += (
                f"{i}. **User ID:** `{user_id}`\n"
                f"   **Username:** @{username}\n"
                f"   **Reason:** {ban_reason}\n"
                f"   **Banned by:** {banned_by}\n"
                f"   **Date:** {ban_date_display}\n\n"
            )
        banned_list_msg += f"**Total banned users:** {len(banned_users)}"
        keyboard = [
            [InlineKeyboardButton("🔄 Refresh List", callback_data="refresh_banned_list")],
            [InlineKeyboardButton("🗑️ Clear All Bans", callback_data="clear_all_bans_confirm")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(banned_list_msg, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Error in banned command: {e}")
        await update.message.reply_text("❌ An error occurred while fetching banned users list.", parse_mode=ParseMode.MARKDOWN)

# ========================
# ADMIN USER INSPECTION COMMAND (FIND)
# ========================
@admin_only
async def find_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /find <user_id> – admin only: show full details of any user."""
    if not context.args:
        await update.message.reply_text(
            "🔎 **Usage:** `/find <user_id>`\n\n"
            "Example: `/find 123456789`",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID. Must be a number.")
        return

    # Check if user exists in database
    user_data = db.get_user(target_id)
    if not user_data:
        await update.message.reply_text("❌ User not found in database.")
        return

    # Gather all required data
    earnings = db.get_user_earnings_summary(target_id)
    payment = db.get_user_payment(target_id)
    contents = db.get_user_contents(target_id)
    content_stats = db.get_content_stats_by_user(target_id)
    ban_status = db.is_user_banned(target_id)

    # Payment info with masking
    payment_info = "Not set"
    if payment:
        method = payment['payment_method']
        details = payment['payment_details']
        masked = details[-4:] if len(details) > 4 else details
        payment_info = f"{method} - ...{masked}"

    # Channel verification status
    channel_status = "✅ Verified" if user_data.get('has_joined_all_channels') else "❌ Not verified"

    # Ban details
    ban_info = "✅ Active"
    if ban_status:
        ban_info = (
            f"🚫 **BANNED**\n"
            f"   **Reason:** {escape_md(user_data.get('ban_reason', 'No reason'))}\n"
            f"   **Date:** {escape_md(user_data.get('ban_date', 'Unknown'))}\n"
            f"   **Banned by:** `{escape_md(str(user_data.get('banned_by', 'Unknown')))}`"
        )

    # Content statistics
    content_breakdown = ""
    for ctype in ContentType:
        count = content_stats.get(ctype.value, 0)
        if count > 0:
            emoji = {'file':'📄','video':'🎬','audio':'🎵','photo':'🖼','text':'📝'}.get(ctype.value,'📁')
            content_breakdown += f"   {emoji} {ctype.value.title()}: {count}\n"

    # Last 5 content IDs
    recent_ids = [c['content_id'] for c in contents[:5]]

    # Withdrawal statistics
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT status, COUNT(*) FROM withdrawals WHERE user_id = ? GROUP BY status', (target_id,))
    withdraw_stats = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.execute('SELECT COUNT(*) FROM withdrawals WHERE user_id = ?', (target_id,))
    total_withdrawals = cursor.fetchone()[0]
    conn.close()

    pending_w = withdraw_stats.get('pending', 0)
    completed_w = withdraw_stats.get('completed', 0)
    rejected_w = withdraw_stats.get('rejected', 0)

    # Build the message
    msg = (
        f"🔎 **ADMIN USER INSPECTION PANEL**\n\n"
        f"👤 **Basic Info**\n"
        f"   🆔 **User ID:** `{target_id}`\n"
        f"   👤 **Username:** @{escape_md(user_data.get('username', 'N/A'))}\n"
        f"   📅 **Join Date:** {escape_md(user_data.get('join_date', 'N/A'))}\n"
        f"   📢 **Channel Status:** {channel_status}\n"
        f"   🚫 **Ban Status:** {ban_info}\n\n"
        f"💰 **Earnings**\n"
        f"   💵 **Total Balance:** `${format_amount(earnings.get('balance',0))}`\n"  # FIXED
        f"   • Referral: `${format_amount(earnings.get('referral_earnings',0))}`\n"  # FIXED
        f"   • View: `${format_amount(earnings.get('view_earnings',0))}`\n"  # FIXED
        f"   • Total Earnings: `${format_amount(earnings.get('total_earnings',0))}`\n"  # FIXED
        f"   • Total Withdrawn: `${format_amount(earnings.get('total_withdrawn',0))}`\n"  # FIXED
        f"   👥 **Referrals:** {earnings.get('total_referrals',0)}\n"
        f"   👁 **Paid Views:** {earnings.get('total_views',0)}\n\n"
        f"📤 **Content**\n"
        f"   📈 **Total Uploads:** {len(contents)}\n"
        f"{content_breakdown}"
        f"   **Recent Content IDs:**\n"
    )
    if recent_ids:
        for cid in recent_ids:
            msg += f"      • `{cid}`\n"
    else:
        msg += "      No uploads\n"

    msg += (
        f"\n💳 **Payment Method**\n"
        f"   `{payment_info}`\n\n"
        f"🏦 **Withdrawal History**\n"
        f"   **Total Requests:** {total_withdrawals}\n"
        f"   ⏳ Pending: {pending_w}\n"
        f"   ✅ Completed: {completed_w}\n"
        f"   ❌ Rejected: {rejected_w}"
    )

    # Build inline buttons for ban/unban
    keyboard = []
    if ban_status:
        keyboard.append([InlineKeyboardButton("🟢 Unban User", callback_data=f"admin_unban_{target_id}")])
    else:
        keyboard.append([InlineKeyboardButton("🔴 Ban User", callback_data=f"admin_ban_{target_id}")])

    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(msg, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)

# ========================
# ADMIN BAN/UNBAN CALLBACK HANDLER (for /find buttons)
# ========================
async def handle_admin_user_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle ban/unban callbacks from the /find panel."""
    query = update.callback_query
    user = query.from_user
    await query.answer()

    # Admin only
    if user.id not in ADMIN_IDS:
        await query.answer("⛔ Admin only!", show_alert=True)
        return

    data = query.data
    if not (data.startswith("admin_ban_") or data.startswith("admin_unban_")):
        return

    parts = data.split("_")
    action = parts[1]  # "ban" or "unban"
    target_id = int(parts[2])

    if action == "ban":
        # Ban with default reason
        reason = "Banned via admin panel"
        success = db.ban_user(target_id, user.id, reason)
        if success:
            await query.edit_message_text(
                f"✅ User `{target_id}` has been **banned**.\n\n"
                f"Use `/find {target_id}` to see updated info.",
                parse_mode=ParseMode.MARKDOWN
            )
            # Notify the banned user
            try:
                await context.bot.send_message(
                    target_id,
                    "🚫 **Account Banned**\n\n"
                    "Your account has been restricted from uploading content.\n"
                    f"Reason: {reason}"
                )
            except Exception:
                pass
        else:
            await query.edit_message_text(f"❌ Failed to ban user `{target_id}`.", parse_mode=ParseMode.MARKDOWN)

    elif action == "unban":
        success = db.unban_user(target_id)
        if success:
            await query.edit_message_text(
                f"✅ User `{target_id}` has been **unbanned**.\n\n"
                f"Use `/find {target_id}` to see updated info.",
                parse_mode=ParseMode.MARKDOWN
            )
            # Notify the unbanned user
            try:
                await context.bot.send_message(
                    target_id,
                    "✅ **Account Unbanned**\n\n"
                    "You can now upload content again."
                )
            except Exception:
                pass
        else:
            await query.edit_message_text(f"❌ Failed to unban user `{target_id}`.", parse_mode=ParseMode.MARKDOWN)

# ========================
# NEW CALLBACK HANDLER FOR COPY BUTTON
# ========================
async def handle_copy_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the 'Copy Link' button – send the deep link in a clean message."""
    query = update.callback_query
    await query.answer()

    data = query.data
    if not data.startswith("copy_"):
        return

    content_id = data[5:]  # remove "copy_"
    bot_username = (await context.bot.get_me()).username
    deep_link = f"https://t.me/{bot_username}?start={content_id}"

    # Send a new message with the link inside <code> for easy copying
    await query.message.reply_text(
        f"🔗 **Your Content Link:**\n<code>{deep_link}</code>",
        parse_mode=ParseMode.HTML
    )

# ========================
# CALLBACK QUERY HANDLER (UPDATED)
# ========================
async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle callback queries from inline keyboards."""
    query = update.callback_query
    user = update.effective_user
    await query.answer()
    
    data = query.data
    
    # Withdrawal admin actions (new)
    if data.startswith("wd_"):
        await handle_withdrawal_action(update, context)
        return
    
    # Admin user actions (from /find)
    if data.startswith("admin_ban_") or data.startswith("admin_unban_"):
        await handle_admin_user_action(update, context)
        return
    
    # Copy link callback (new)
    if data.startswith("copy_"):
        await handle_copy_callback(update, context)
        return
    
    # Existing callbacks
    if data == "recheck_membership":
        has_joined = await check_channel_membership(user.id, context)
        if has_joined:
            db.update_user_channel_status(user.id, True)
            welcome_msg = (
                f"✅ **Welcome {user.first_name}!**\n\n"
                "You have successfully joined all required channels.\n\n"
                "**Content Sharing Bot**\n\n"
                "📤 **Upload any content:**\n"
                "• Files, Videos, Audio\n"
                "• Photos, Text\n\n"
                "🆔 **Get a unique Content ID**\n"
                "📤 **Share with anyone**\n"
                f"⏰ **Auto-deletes after {format_delete_time(AUTO_DELETE_SECONDS)}**\n\n"  # FIXED
                "**Simply send me any content to get started!**"
            )
            keyboard = [
                [InlineKeyboardButton("📤 Upload Content", callback_data="upload_help")],
                [InlineKeyboardButton("👤 My Profile", callback_data="profile")],
                [InlineKeyboardButton("❓ Help", callback_data="help")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(
                welcome_msg,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await query.answer(
                "❌ You haven't joined all required channels yet!\n"
                "Please join ALL channels and try again.",
                show_alert=True
            )
    
    elif data == "upload_help":
        await query.edit_message_text(
            "📤 **How to upload content:**\n\n"
            "Simply send me:\n"
            "• Any file (document)\n"
            "• Video\n"
            "• Audio\n"
            "• Photo\n"
            "• Text message\n\n"
            "I'll show you buttons to choose protection mode, then generate a unique Content ID!\n\n"
            "Go ahead and send me something now!",
            parse_mode=ParseMode.MARKDOWN
        )
    
    elif data == "profile":
        # Show profile (similar to /profile but from callback)
        try:
            earnings = db.get_user_earnings_summary(user.id)
            payment = db.get_user_payment(user.id)
            payment_info = "Not set"
            if payment:
                method = payment['payment_method']
                details = payment['payment_details']
                masked = details[-4:] if len(details) > 4 else details
                payment_info = f"{method} - ...{masked}"
            
            profile_msg = (
                f"👤 **Your Profile**\n\n"
                f"🆔 **User ID:** `{user.id}`\n"
                f"👤 **Username:** @{escape_md(user.username) or 'Not set'}\n"
                f"💰 **Available Balance:** `${format_amount(earnings.get('balance',0))}`\n"  # FIXED
                f"💳 **Payment:** `{payment_info}`\n\n"
                f"Use /profile for full details."
            )
            keyboard = [
                [InlineKeyboardButton("💳 Set Payment", callback_data="set_payment")],
                [InlineKeyboardButton("💰 Withdraw", callback_data="withdraw_help")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(profile_msg, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            logger.error(f"Error in profile callback: {e}")
            await query.edit_message_text("❌ Error loading profile.", parse_mode=ParseMode.MARKDOWN)
    
    elif data == "help":
        help_text = (
            "❓ **Help & Guide**\n\n"
            "**Available Commands:**\n"
            "• `/start` - Start the bot\n"
            "• `/get <id>` - Get content by secret ID\n"
            "• `/profile` - Your profile & earnings\n"
            "• `/delete <id>` - Delete your content\n"
            "• `/withdraw` - Withdraw earnings\n"
            "• `/setpayment` - Set payment method\n"
            "• `/help` - This help message\n\n"
            "**Simply send any content to upload it!**"
        )
        await query.edit_message_text(help_text, parse_mode=ParseMode.MARKDOWN)
    
    elif data == "delete_content_help":
        await query.edit_message_text(
            "🗑️ **Delete Content**\n\n"
            "To delete your uploaded content:\n"
            "1. Use `/delete <content_id>`\n"
            "2. You can only delete your own uploads\n"
            "3. Find your Content IDs in your profile\n\n"
            "Example: `/delete abc123xyz456`",
            parse_mode=ParseMode.MARKDOWN
        )
    
    elif data in ("protection_protected", "protection_unprotected", "cancel_upload"):
        # Upload related
        if data == "protection_protected":
            if 'pending_upload' in context.user_data:
                content_data = context.user_data['pending_upload']
                content_data['protection_mode'] = 'protected'
                await complete_upload(update, context, content_data)
            else:
                await query.edit_message_text("❌ Upload session expired. Please send your content again.", parse_mode=ParseMode.MARKDOWN)
        elif data == "protection_unprotected":
            if 'pending_upload' in context.user_data:
                content_data = context.user_data['pending_upload']
                content_data['protection_mode'] = 'unprotected'
                await complete_upload(update, context, content_data)
            else:
                await query.edit_message_text("❌ Upload session expired. Please send your content again.", parse_mode=ParseMode.MARKDOWN)
        elif data == "cancel_upload":
            if 'pending_upload' in context.user_data:
                del context.user_data['pending_upload']
            await query.edit_message_text("❌ Upload cancelled. You can send content again anytime.", parse_mode=ParseMode.MARKDOWN)
    
    elif data.startswith("view_uploads_"):
        user_id = int(data.split("_")[2])
        if user.id != user_id and user.id not in ADMIN_IDS:
            await query.answer("❌ Access denied!", show_alert=True)
            return
        user_contents = db.get_user_contents(user_id)
        if not user_contents:
            await query.edit_message_text("📭 No uploads found for this user.", parse_mode=ParseMode.MARKDOWN)
            return
        uploads_text = f"📁 **Uploads for User {user_id}**\n\n"
        for i, content in enumerate(user_contents[:20], 1):
            protection_emoji = "🔒" if content.get('protection_mode') == 'protected' else "🔓"
            uploads_text += (
                f"{i}. **ID:** `{content['content_id']}`\n"
                f"   **Type:** {content['content_type']}\n"
                f"   **Protection:** {protection_emoji}\n"
                f"   **Date:** {content['upload_timestamp']}\n\n"
            )
        if len(user_contents) > 20:
            uploads_text += f"... and {len(user_contents) - 20} more"
        await query.edit_message_text(uploads_text, parse_mode=ParseMode.MARKDOWN)
    
    elif data == "set_payment":
        # Trigger /setpayment via callback
        await query.edit_message_text(
            "💳 To set your payment method, use the command:\n`/setpayment`",
            parse_mode=ParseMode.MARKDOWN
        )
    
    elif data == "withdraw_help":
        await query.edit_message_text(
            "💰 To withdraw your earnings, use:\n`/withdraw <amount>`\n\n"
            "First set your payment method with `/setpayment`.",
            parse_mode=ParseMode.MARKDOWN
        )
    
    # Admin banned list actions
    elif data == "refresh_banned_list":
        if user.id not in ADMIN_IDS:
            await query.answer("❌ Admin only!", show_alert=True)
            return
        banned_users = db.get_banned_users()
        if not banned_users:
            await query.edit_message_text("✅ **No banned users found.**\n\nAll users are currently allowed to upload content.", parse_mode=ParseMode.MARKDOWN)
            return
        banned_list_msg = "🚫 **Banned Users List**\n\n"
        for i, banned_user in enumerate(banned_users, 1):
            user_id = banned_user['user_id']
            username = banned_user['username'] or "No username"
            ban_reason = banned_user['ban_reason'] or "No reason"
            banned_by = banned_user['banned_by'] or "Unknown"
            ban_date = banned_user['ban_date'] or "Unknown"
            if isinstance(ban_date, str):
                ban_date_display = ban_date
            else:
                try:
                    ban_date_display = ban_date.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    ban_date_display = str(ban_date)
            banned_list_msg += (
                f"{i}. **User ID:** `{user_id}`\n"
                f"   **Username:** @{username}\n"
                f"   **Reason:** {ban_reason}\n"
                f"   **Banned by:** {banned_by}\n"
                f"   **Date:** {ban_date_display}\n\n"
            )
        banned_list_msg += f"**Total banned users:** {len(banned_users)}"
        await query.edit_message_text(banned_list_msg, parse_mode=ParseMode.MARKDOWN)
        await query.answer("✅ List refreshed!", show_alert=False)
    
    elif data == "clear_all_bans_confirm":
        if user.id not in ADMIN_IDS:
            await query.answer("❌ Admin only!", show_alert=True)
            return
        banned_users = db.get_banned_users()
        total_banned = len(banned_users)
        if total_banned == 0:
            await query.answer("No banned users to clear!", show_alert=True)
            return
        keyboard = [
            [
                InlineKeyboardButton("✅ Yes, Clear All", callback_data="clear_all_bans"),
                InlineKeyboardButton("❌ Cancel", callback_data="cancel_clear_bans")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"⚠️ **Confirm Clear All Bans**\n\n"
            f"Are you sure you want to unban ALL {total_banned} users?\n\n"
            f"This action cannot be undone!",
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
    
    elif data == "clear_all_bans":
        if user.id not in ADMIN_IDS:
            await query.answer("❌ Admin only!", show_alert=True)
            return
        banned_users = db.get_banned_users()
        total_banned = len(banned_users)
        if total_banned == 0:
            await query.edit_message_text("✅ No banned users to clear.", parse_mode=ParseMode.MARKDOWN)
            return
        success_count = 0
        for banned_user in banned_users:
            if db.unban_user(banned_user['user_id']):
                success_count += 1
        await query.edit_message_text(
            f"✅ **All bans cleared successfully!**\n\n"
            f"**Total unbanned:** {success_count}/{total_banned}\n\n"
            f"All users can now upload content again.",
            parse_mode=ParseMode.MARKDOWN
        )
    
    elif data == "cancel_clear_bans":
        if user.id not in ADMIN_IDS:
            await query.answer("❌ Admin only!", show_alert=True)
            return
        await query.edit_message_text("❌ Operation cancelled.\n\nNo bans were cleared.", parse_mode=ParseMode.MARKDOWN)

# ========================
# MESSAGE HANDLER (unchanged)
# ========================
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all incoming messages."""
    if await check_maintenance(update, context):
        return
    if await check_ban_status(update, context):
        return
    
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        if await require_channel_join(update, context):
            return
    
    if update.message and update.message.text and update.message.text.startswith('/'):
        return
    
    # Check if it looks like a content ID
    if update.message and update.message.text:
        text = update.message.text.strip()
        if text.isalnum() and 8 <= len(text) <= 20:
            context.args = [text]
            await get_content_command(update, context)
            return
    
    await handle_upload(update, context)

# ========================
# MAIN APPLICATION
# ========================
async def main():
    """Start the bot with all features enabled."""
    
    print("=" * 50)
    print("🤖 Starting Advanced Content Bot...")
    print(f"📱 Bot Token: {BOT_TOKEN[:10]}...")
    print(f"👑 Admins: {len(ADMIN_IDS)}")
    print(f"📢 Force Join Channels: {len(FORCE_JOIN_CHANNELS)}")
    print(f"🔒 Backup Channel: {BACKUP_CHANNEL_ID}")
    print(f"💾 Database: {DATABASE_NAME}")
    print(f"⏰ Auto-delete: {format_delete_time(AUTO_DELETE_SECONDS)}")  # FIXED
    print("=" * 50)
    
    try:
        application = Application.builder().token(BOT_TOKEN).build()
        
        # Command handlers
        application.add_handler(CommandHandler("start", start_command))
        application.add_handler(CommandHandler("help", help_command))
        application.add_handler(CommandHandler("get", get_content_command))
        application.add_handler(CommandHandler("profile", profile_command))
        application.add_handler(CommandHandler("delete", delete_content_command))
        application.add_handler(CommandHandler("withdraw", withdraw_command))
        
        # Payment setup conversation
        payment_conv = ConversationHandler(
            entry_points=[CommandHandler("setpayment", setpayment_start)],
            states={
                SET_PAYMENT_METHOD: [CallbackQueryHandler(setpayment_method, pattern=r"^pay_")],
                SET_PAYMENT_DETAILS: [MessageHandler(filters.TEXT & ~filters.COMMAND, setpayment_details)]
            },
            fallbacks=[CommandHandler("cancel", setpayment_cancel),
                       CallbackQueryHandler(setpayment_cancel, pattern="pay_cancel")],
            allow_reentry=True  # <--- FIX: allow users to run /setpayment multiple times
        )
        application.add_handler(payment_conv)
        
        # Admin commands
        application.add_handler(CommandHandler("upload", handle_upload))
        application.add_handler(CommandHandler("maintenance", maintenance_command))
        application.add_handler(CommandHandler("stats", stats_command))
        application.add_handler(CommandHandler("adms", broadcast_command))
        application.add_handler(CommandHandler("settime", settime_command))
        application.add_handler(CommandHandler("ban", ban_command))
        application.add_handler(CommandHandler("unban", unban_command))
        application.add_handler(CommandHandler("banned", banned_command))
        application.add_handler(CommandHandler("withdrawals", withdrawals_command))
        application.add_handler(CommandHandler("setreward", setreward_command))
        application.add_handler(CommandHandler("setminwithdraw", setminwithdraw_command))
        application.add_handler(CommandHandler("find", find_command))
        
        # Callback query handlers
        application.add_handler(CallbackQueryHandler(handle_callback_query))
        # New copy callback handler (already included in handle_callback_query via condition)
        # application.add_handler(CallbackQueryHandler(handle_copy_callback, pattern="^copy_"))  # optional, but already in main callback
        
        # Message handler
        application.add_handler(MessageHandler(
            filters.TEXT | filters.PHOTO | filters.VIDEO | 
            filters.AUDIO | filters.Document.ALL,
            handle_message
        ))
        
        async def post_init(application: Application):
            await set_command_scopes(application)
        
        application.post_init = post_init
        
        print("✅ Bot started successfully!")
        print("📡 Listening for updates...")
        
        await application.initialize()
        await application.start()
        await application.updater.start_polling()
        
        await asyncio.Event().wait()
        
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")
        print(f"❌ Bot crashed: {e}")

def run_bot():
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
        print(f"❌ Bot crashed: {e}")

if __name__ == '__main__':
    run_bot()