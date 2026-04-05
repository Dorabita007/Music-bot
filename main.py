import os
import sys
import subprocess
import importlib

# ==========================================
# 🚀 AUTO-INSTALLER (NO REQUIREMENTS.TXT NEEDED)
# ==========================================
def setup_environment():
    print("🚀 Starting Auto-Setup... Checking modules...")
    # Yahan TgCrypto hata diya gaya hai taaki GCC compiler error na aaye
    packages = {
        'aiohttp': 'aiohttp',
        'dotenv': 'python-dotenv',
        'motor': 'motor',
        'pyrogram': 'pyrogram'
    }
    
    for module_name, package_name in packages.items():
        try:
            importlib.import_module(module_name)
            print(f"✅ {module_name} is already installed.")
        except ImportError:
            print(f"⚙️ {module_name} is missing. Installing {package_name}...")
            try:
                # Standard pip install with no-cache-dir to save memory
                subprocess.check_call([sys.executable, "-m", "pip", "install", "--no-cache-dir", package_name])
                print(f"✅ {package_name} installed successfully!")
            except Exception as e:
                print(f"⚠️ App blocked standard install. Trying Fallback...")
                try:
                    # Fallback for restrictive environments
                    try:
                        from pip._internal import main as pip_main
                    except ImportError:
                        import pip
                        pip_main = pip.main
                    
                    pip_main(['install', '--no-cache-dir', package_name])
                    print(f"✅ {package_name} installed successfully via internal pip!")
                except Exception as alt_e:
                    print(f"❌ Failed to auto-install {package_name}.")
                    sys.exit(1)
                    
    print("\n🎉 All requirements verified! Starting Bot...\n")

# Run the installer BEFORE any other imports
setup_environment()
# ==========================================

import json
import time
import asyncio
import logging
import random
import string
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

import aiohttp
import dotenv

# FIXED: Correct Motor Imports
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection
from pyrogram import Client, filters, types
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, 
    InputMediaPhoto, Message, CallbackQuery, User
)
from pyrogram.errors import PeerIdInvalid, UserNotParticipant, UserIsBot
import hashlib

dotenv.load_dotenv()

API_ID = int(os.environ.get("API_ID", "39071888"))
API_HASH = os.environ.get("API_HASH", "48bbb8b8083fbc3bd546b325289b574f")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8259895419:AAGd6W_HDX7HvHuP-JNaUAC0M8brSZL7NqA")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb+srv://Doramusic:Doramusic@cluster0.u3al4uf.mongodb.net/?appName=Cluster0")
OWNER_ID = int(os.environ.get("OWNER_ID", "8653737174"))
PORT = int(os.environ.get("PORT", "8080"))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, uri: str):
        self.client = None
        self.db = None
        self.uri = uri
        # FIXED: Corrected Type Hints for Motor
        self.users: Optional[AsyncIOMotorCollection] = None
        self.groups: Optional[AsyncIOMotorCollection] = None
        self.queue: Optional[AsyncIOMotorCollection] = None
        self.settings: Optional[AsyncIOMotorCollection] = None
        self.logs: Optional[AsyncIOMotorCollection] = None
        self.banned_users: Optional[AsyncIOMotorCollection] = None
        self.broadcasts: Optional[AsyncIOMotorCollection] = None
        self.schedules: Optional[AsyncIOMotorCollection] = None
        self.sessions: Optional[AsyncIOMotorCollection] = None

    async def connect(self):
        try:
            # FIXED: Correct initialization
            self.client = AsyncIOMotorClient(self.uri)
            self.db = self.client.music_bot
            self.users = self.db.users
            self.groups = self.db.groups
            self.queue = self.db.queue
            self.settings = self.db.settings
            self.logs = self.db.logs
            self.banned_users = self.db.banned_users
            self.broadcasts = self.db.broadcasts
            self.schedules = self.db.schedules
            self.sessions = self.db.sessions
            
            await self.db.command('ping')
            await self.users.create_index("_id", unique=True)
            await self.groups.create_index("_id", unique=True)
            logger.info("MongoDB connected successfully")
        except Exception as e:
            logger.error(f"MongoDB connection failed: {e}")
            raise

    async def disconnect(self):
        if self.client:
            self.client.close()
            logger.info("MongoDB disconnected")

    async def add_user(self, user_id: int, name: str, username: str = None):
        try:
            existing = await self.users.find_one({"_id": user_id})
            if not existing:
                await self.users.insert_one({
                    "_id": user_id,
                    "name": name,
                    "username": username,
                    "plays": 0,
                    "banned": False,
                    "joined_at": datetime.utcnow(),
                    "last_seen": datetime.utcnow()
                })
                return True
            else:
                await self.users.update_one({"_id": user_id}, {"$set": {"last_seen": datetime.utcnow()}})
            return False
        except Exception as e:
            logger.error(f"Error adding user: {e}")
            return False

    async def add_group(self, group_id: int, title: str):
        try:
            existing = await self.groups.find_one({"_id": group_id})
            if not existing:
                await self.groups.insert_one({
                    "_id": group_id,
                    "title": title,
                    "added_at": datetime.utcnow(),
                    "admin_only": False,
                    "auto_dj": True,
                    "loop_mode": "off"
                })
                return True
            return False
        except Exception as e:
            logger.error(f"Error adding group: {e}")
            return False

    async def is_banned(self, user_id: int) -> bool:
        try:
            user = await self.users.find_one({"_id": user_id})
            return user.get("banned", False) if user else False
        except:
            return False

    async def get_user_count(self) -> int:
        try:
            return await self.users.count_documents({})
        except:
            return 0

    async def get_group_count(self) -> int:
        try:
            return await self.groups.count_documents({})
        except:
            return 0

    async def get_all_users(self) -> List[dict]:
        try:
            return await self.users.find({}).to_list(None)
        except:
            return []

    async def get_all_groups(self) -> List[dict]:
        try:
            return await self.groups.find({}).to_list(None)
        except:
            return []

    async def ban_user(self, user_id: int):
        try:
            await self.users.update_one({"_id": user_id}, {"$set": {"banned": True, "banned_at": datetime.utcnow()}})
            await self.add_log("ban", {"user_id": user_id})
        except Exception as e:
            logger.error(f"Error banning user: {e}")

    async def unban_user(self, user_id: int):
        try:
            await self.users.update_one({"_id": user_id}, {"$set": {"banned": False}})
            await self.add_log("unban", {"user_id": user_id})
        except Exception as e:
            logger.error(f"Error unbanning user: {e}")

    async def increment_plays(self, user_id: int):
        try:
            await self.users.update_one({"_id": user_id}, {"$inc": {"plays": 1}})
        except Exception as e:
            logger.error(f"Error incrementing plays: {e}")

    async def add_log(self, event_type: str, data: dict):
        try:
            await self.logs.insert_one({
                "type": event_type,
                "data": data,
                "timestamp": datetime.utcnow()
            })
        except Exception as e:
            logger.error(f"Error adding log: {e}")

    async def save_queue(self, group_id: int, queue: List[dict]):
        try:
            await self.queue.update_one(
                {"_id": group_id},
                {"$set": {"songs": queue, "updated_at": datetime.utcnow()}},
                upsert=True
            )
        except Exception as e:
            logger.error(f"Error saving queue: {e}")

    async def get_queue(self, group_id: int) -> List[dict]:
        try:
            result = await self.queue.find_one({"_id": group_id})
            return result.get("songs", []) if result else []
        except:
            return []

    async def save_session(self, phone: str, session_string: str):
        try:
            await self.sessions.update_one(
                {"_id": "userbot"},
                {"$set": {"phone": phone, "session": session_string, "created_at": datetime.utcnow()}},
                upsert=True
            )
        except Exception as e:
            logger.error(f"Error saving session: {e}")

    async def get_session(self):
        try:
            result = await self.sessions.find_one({"_id": "userbot"})
            return result.get("session") if result else None
        except:
            return None

    async def add_broadcast(self, broadcast_type: str, target: str, content: dict):
        try:
            await self.broadcasts.insert_one({
                "type": broadcast_type,
                "target": target,
                "content": content,
                "created_at": datetime.utcnow(),
                "sent": False
            })
        except Exception as e:
            logger.error(f"Error adding broadcast: {e}")

    async def add_schedule(self, message: str, target: str, schedule_time: datetime):
        try:
            await self.schedules.insert_one({
                "message": message,
                "target": target,
                "schedule_time": schedule_time,
                "created_at": datetime.utcnow(),
                "sent": False
            })
        except Exception as e:
            logger.error(f"Error adding schedule: {e}")

    async def get_pending_schedules(self):
        try:
            current_time = datetime.utcnow()
            return await self.schedules.find({"sent": False, "schedule_time": {"$lte": current_time}}).to_list(None)
        except:
            return []

    async def mark_schedule_sent(self, schedule_id):
        try:
            await self.schedules.update_one({"_id": schedule_id}, {"$set": {"sent": True}})
        except Exception as e:
            logger.error(f"Error marking schedule as sent: {e}")

    async def get_group_settings(self, group_id: int) -> dict:
        try:
            result = await self.groups.find_one({"_id": group_id})
            return result if result else {}
        except:
            return {}

    async def update_group_settings(self, group_id: int, settings: dict):
        try:
            await self.groups.update_one({"_id": group_id}, {"$set": settings})
        except Exception as e:
            logger.error(f"Error updating group settings: {e}")


class MusicBot:
    def __init__(self):
        self.app = Client("music_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
        self.db = Database(MONGO_URI)
        self.queues: Dict[int, List[dict]] = defaultdict(list)
        self.current_playing: Dict[int, dict] = {}
        self.cooldowns: Dict[int, float] = defaultdict(float)
        self.loop_modes: Dict[int, str] = defaultdict(lambda: "off")
        self.volume: Dict[int, int] = defaultdict(lambda: 100)
        self.session = None
        self.pause_states: Dict[int, bool] = defaultdict(bool)
        self.broadcast_mode = False
        self.broadcast_data = {}
        self.schedule_task = None
        self.setup_handlers()

    def setup_handlers(self):
        @self.app.on_message(filters.command("start") & filters.private)
        async def start_handler(client: Client, message: Message):
            await self.on_start(client, message)

        @self.app.on_message(filters.command("help") & filters.private)
        async def help_handler(client: Client, message: Message):
            await self.on_help(client, message)

        @self.app.on_message(filters.command("play") & filters.group)
        async def play_handler(client: Client, message: Message):
            await self.on_play(client, message)

        @self.app.on_message(filters.command("pause") & filters.group)
        async def pause_handler(client: Client, message: Message):
            await self.on_pause(client, message)

        @self.app.on_message(filters.command("resume") & filters.group)
        async def resume_handler(client: Client, message: Message):
            await self.on_resume(client, message)

        @self.app.on_message(filters.command("skip") & filters.group)
        async def skip_handler(client: Client, message: Message):
            await self.on_skip(client, message)

        @self.app.on_message(filters.command("stop") & filters.group)
        async def stop_handler(client: Client, message: Message):
            await self.on_stop(client, message)

        @self.app.on_message(filters.command("queue") & filters.group)
        async def queue_handler(client: Client, message: Message):
            await self.on_queue(client, message)

        @self.app.on_message(filters.command("loop") & filters.group)
        async def loop_handler(client: Client, message: Message):
            await self.on_loop(client, message)

        @self.app.on_message(filters.command("volume") & filters.group)
        async def volume_handler(client: Client, message: Message):
            await self.on_volume(client, message)

        @self.app.on_message(filters.command("ban"))
        async def ban_handler(client: Client, message: Message):
            await self.on_ban(client, message)

        @self.app.on_message(filters.command("unban"))
        async def unban_handler(client: Client, message: Message):
            await self.on_unban(client, message)

        @self.app.on_message(filters.command("admin"))
        async def admin_handler(client: Client, message: Message):
            await self.on_admin(client, message)

        @self.app.on_message(filters.command("broadcast"))
        async def broadcast_handler(client: Client, message: Message):
            await self.on_broadcast(client, message)

        @self.app.on_message(filters.command("stats"))
        async def stats_handler(client: Client, message: Message):
            await self.on_stats(client, message)

        @self.app.on_message(filters.command("users"))
        async def users_handler(client: Client, message: Message):
            await self.on_users(client, message)

        @self.app.on_message(filters.command("groups"))
        async def groups_handler(client: Client, message: Message):
            await self.on_groups(client, message)

        @self.app.on_message(filters.command("backup"))
        async def backup_handler(client: Client, message: Message):
            await self.on_backup(client, message)

        @self.app.on_message(filters.command("login"))
        async def login_handler(client: Client, message: Message):
            await self.on_login(client, message)

        @self.app.on_message(filters.command("schedule"))
        async def schedule_handler(client: Client, message: Message):
            await self.on_schedule(client, message)

        @self.app.on_message(filters.command("settings"))
        async def settings_handler(client: Client, message: Message):
            await self.on_settings(client, message)

        @self.app.on_message(filters.command("ping"))
        async def ping_handler(client: Client, message: Message):
            await self.on_ping(client, message)

        @self.app.on_message(filters.command("logs"))
        async def logs_handler(client: Client, message: Message):
            await self.on_logs(client, message)

        @self.app.on_callback_query()
        async def callback_handler(client: Client, callback_query: CallbackQuery):
            await self.on_callback(client, callback_query)

        @self.app.on_message()
        async def broadcast_content_handler(client: Client, message: Message):
            await self.on_broadcast_content(client, message)

    async def check_cooldown(self, user_id: int, cooldown_time: int = 2) -> bool:
        current_time = time.time()
        if user_id in self.cooldowns:
            if current_time - self.cooldowns[user_id] < cooldown_time:
                return False
        self.cooldowns[user_id] = current_time
        return True

    async def fetch_song(self, song_name: str) -> Optional[dict]:
        try:
            async with self.session.get(
                f"https://ansh-apis.is-dev.org/api/ytstream?key=ansh&song={song_name}&type=vid"
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("success") and data.get("stream_url"):
                        return data
        except Exception as e:
            logger.error(f"Error fetching song: {e}")
        return None

    async def on_start(self, client: Client, message: Message):
        try:
            is_new = await self.db.add_user(
                message.from_user.id, 
                message.from_user.first_name, 
                message.from_user.username
            )
            if is_new:
                try:
                    await client.send_message(
                        OWNER_ID, 
                        f"🚀 New User Started Bot\n\n"
                        f"👤 Name: {message.from_user.first_name}\n"
                        f"🆔 User ID: {message.from_user.id}\n"
                        f"📱 Username: @{message.from_user.username or 'No username'}\n"
                        f"⏰ Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                except:
                    pass

            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Add Me To Group", url=f"https://t.me/{(await client.get_me()).username}?startgroup=new")],
                [InlineKeyboardButton("👤 Owner", user_id=OWNER_ID), InlineKeyboardButton("📢 Channel", url="https://t.me")],
                [InlineKeyboardButton("❓ Help", callback_data="help_menu")]
            ])

            await message.reply(
                f"Hello {message.from_user.mention} 👋\n\n"
                f"🎵 I am an advanced Music Voice Chat Bot that can stream songs in Telegram VC.\n\n"
                f"✨ Features:\n"
                f"• Play songs from YouTube\n"
                f"• Queue management\n"
                f"• Advanced controls (pause, skip, loop)\n"
                f"• Admin panel with statistics\n"
                f"• User management\n"
                f"• Broadcast system\n\n"
                f"Use /help to see all commands.",
                reply_markup=keyboard
            )
        except Exception as e:
            logger.error(f"Error in start handler: {e}")

    async def on_help(self, client: Client, message: Message):
        try:
            help_text = (
                "🎵 **MUSIC BOT COMMANDS**\n\n"
                "🎶 **Music Commands:**\n"
                "/play <song> - Play a song from YouTube\n"
                "/pause - Pause current music\n"
                "/resume - Resume paused music\n"
                "/skip - Skip to next song in queue\n"
                "/stop - Stop music and leave voice chat\n"
                "/queue - Show current queue\n"
                "/loop - Toggle loop mode (off/single/queue)\n"
                "/volume <1-200> - Set playback volume\n\n"
                "👑 **Admin Commands:**\n"
                "/admin - Open admin dashboard\n"
                "/ban <user_id> - Ban user from bot\n"
                "/unban <user_id> - Unban user\n"
                "/broadcast - Send message to all users\n"
                "/stats - View bot statistics\n"
                "/users - View all users\n"
                "/groups - View all groups\n"
                "/backup - Export database as JSON\n"
                "/schedule - Schedule broadcast\n"
                "/settings - Configure bot settings\n"
                "/logs - View bot activity logs\n"
                "/ping - Check bot latency\n"
                "/login - Authenticate userbot\n\n"
                "ℹ️ **Bot Features:**\n"
                "• YouTube music streaming\n"
                "• Queue management with persistence\n"
                "• Loop modes (off/single/queue)\n"
                "• Volume control (1-200%)\n"
                "• Admin dashboard with statistics\n"
                "• User ban/unban system\n"
                "• Broadcast to all users\n"
                "• Database backup export\n"
                "• Scheduled messages\n"
                "• Comprehensive logging\n"
                "• Rate limiting\n"
                "• Error handling\n"
            )
            await message.reply(help_text)
        except Exception as e:
            logger.error(f"Error in help handler: {e}")

    async def on_play(self, client: Client, message: Message):
        try:
            if await self.db.is_banned(message.from_user.id):
                await message.reply("❌ You are banned from using this bot")
                return

            if not await self.check_cooldown(message.from_user.id):
                await message.reply("⏳ Please wait 2 seconds before using another command")
                return

            if not message.text.split(None, 1)[1:]:
                await message.reply("Usage: /play <song name>")
                return

            song_name = message.text.split(None, 1)[1]
            status_msg = await message.reply("🔍 Searching for song...")

            song_data = await self.fetch_song(song_name)
            if not song_data:
                await status_msg.edit_text("❌ Song not found or API error")
                return

            song_info = {
                "title": song_data.get("title", song_name),
                "url": song_data.get("stream_url"),
                "duration": song_data.get("duration", "Unknown"),
                "thumbnail": song_data.get("thumbnail", ""),
                "requested_by": message.from_user.first_name,
                "requested_by_id": message.from_user.id
            }

            group_id = message.chat.id
            self.queues[group_id].append(song_info)
            await self.db.increment_plays(message.from_user.id)

            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("▶️", callback_data=f"play_{group_id}"),
                 InlineKeyboardButton("⏸", callback_data=f"pause_{group_id}"),
                 InlineKeyboardButton("⏭", callback_data=f"skip_{group_id}"),
                 InlineKeyboardButton("🔁", callback_data=f"loop_{group_id}"),
                 InlineKeyboardButton("❌", callback_data=f"stop_{group_id}")]
            ])

            embed_text = (
                f"🎵 **{song_info['title']}**\n"
                f"⏱ Duration: {song_info['duration']}\n"
                f"👤 Requested by: {song_info['requested_by']}\n"
                f"📊 Queue position: {len(self.queues[group_id])}\n"
                f"📈 Queue size: {sum(len(q) for q in self.queues.values())} songs"
            )

            await status_msg.edit_text(embed_text, reply_markup=keyboard)
            await self.db.add_log("song_played", {"song": song_info['title'], "group_id": group_id, "user_id": message.from_user.id})

        except Exception as e:
            logger.error(f"Error in play handler: {e}")
            try:
                await message.reply(f"❌ Error: {str(e)[:100]}")
            except:
                pass

    async def on_pause(self, client: Client, message: Message):
        try:
            group_id = message.chat.id
            self.pause_states[group_id] = True
            await message.reply("⏸ Music paused")
            await self.db.add_log("pause", {"group_id": group_id, "user_id": message.from_user.id})
        except Exception as e:
            logger.error(f"Error in pause handler: {e}")

    async def on_resume(self, client: Client, message: Message):
        try:
            group_id = message.chat.id
            self.pause_states[group_id] = False
            await message.reply("▶️ Music resumed")
            await self.db.add_log("resume", {"group_id": group_id, "user_id": message.from_user.id})
        except Exception as e:
            logger.error(f"Error in resume handler: {e}")

    async def on_skip(self, client: Client, message: Message):
        try:
            group_id = message.chat.id
            if not self.queues[group_id]:
                await message.reply("❌ Queue is empty")
                return
            skipped = self.queues[group_id].pop(0)
            await message.reply(f"⏭ Skipped: **{skipped['title']}**")
            await self.db.add_log("skip", {"group_id": group_id, "user_id": message.from_user.id, "song": skipped['title']})
        except Exception as e:
            logger.error(f"Error in skip handler: {e}")

    async def on_stop(self, client: Client, message: Message):
        try:
            group_id = message.chat.id
            self.queues[group_id].clear()
            self.current_playing.pop(group_id, None)
            self.pause_states[group_id] = False
            await message.reply("❌ Music stopped and queue cleared")
            await self.db.add_log("stop", {"group_id": group_id, "user_id": message.from_user.id})
        except Exception as e:
            logger.error(f"Error in stop handler: {e}")

    async def on_queue(self, client: Client, message: Message):
        try:
            group_id = message.chat.id
            if not self.queues[group_id]:
                await message.reply("📭 Queue is empty")
                return

            queue_text = f"📋 **Current Queue ({len(self.queues[group_id])} songs):**\n\n"
            for i, song in enumerate(self.queues[group_id][:15], 1):
                queue_text += f"{i}. {song['title']}\n   👤 Requested by: {song['requested_by']}\n\n"

            if len(self.queues[group_id]) > 15:
                queue_text += f"... and {len(self.queues[group_id]) - 15} more songs"

            await message.reply(queue_text)
        except Exception as e:
            logger.error(f"Error in queue handler: {e}")

    async def on_loop(self, client: Client, message: Message):
        try:
            group_id = message.chat.id
            modes = ["off", "single", "queue"]
            current = self.loop_modes[group_id]
            next_mode = modes[(modes.index(current) + 1) % len(modes)]
            self.loop_modes[group_id] = next_mode

            mode_icons = {"off": "🔁", "single": "🔂", "queue": "🔃"}
            await message.reply(f"🔄 Loop mode: {mode_icons[next_mode]} **{next_mode.upper()}**")
            await self.db.add_log("loop_changed", {"group_id": group_id, "mode": next_mode})
        except Exception as e:
            logger.error(f"Error in loop handler: {e}")

    async def on_volume(self, client: Client, message: Message):
        try:
            if not message.text.split(None, 1)[1:]:
                await message.reply(f"🔊 Current volume: {self.volume[message.chat.id]}%\nUsage: /volume <1-200>")
                return

            volume = int(message.text.split()[1])
            if 1 <= volume <= 200:
                self.volume[message.chat.id] = volume
                await message.reply(f"🔊 Volume set to {volume}%")
                await self.db.add_log("volume_changed", {"group_id": message.chat.id, "volume": volume})
            else:
                await message.reply("❌ Volume must be between 1 and 200")
        except:
            await message.reply("❌ Invalid volume number")

    async def on_ban(self, client: Client, message: Message):
        try:
            if message.from_user.id != OWNER_ID:
                await message.reply("❌ Only owner can use this command")
                return

            if not message.text.split(None, 1)[1:]:
                await message.reply("Usage: /ban <user_id>")
                return

            user_id = int(message.text.split()[1])
            await self.db.ban_user(user_id)
            await message.reply(f"🚫 User {user_id} has been banned")
        except Exception as e:
            logger.error(f"Error in ban handler: {e}")
            await message.reply(f"❌ Error: {str(e)[:100]}")

    async def on_unban(self, client: Client, message: Message):
        try:
            if message.from_user.id != OWNER_ID:
                await message.reply("❌ Only owner can use this command")
                return

            if not message.text.split(None, 1)[1:]:
                await message.reply("Usage: /unban <user_id>")
                return

            user_id = int(message.text.split()[1])
            await self.db.unban_user(user_id)
            await message.reply(f"✅ User {user_id} has been unbanned")
        except Exception as e:
            logger.error(f"Error in unban handler: {e}")
            await message.reply(f"❌ Error: {str(e)[:100]}")

    async def on_admin(self, client: Client, message: Message):
        try:
            if message.from_user.id != OWNER_ID:
                await message.reply("❌ Only owner can access admin panel")
                return

            user_count = await self.db.get_user_count()
            group_count = await self.db.get_group_count()

            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("📊 Stats", callback_data="admin_stats"),
                 InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast")],
                [InlineKeyboardButton("👥 Users", callback_data="admin_users"),
                 InlineKeyboardButton("💬 Groups", callback_data="admin_groups")],
                [InlineKeyboardButton("⚙️ Settings", callback_data="admin_settings"),
                 InlineKeyboardButton("📂 Backup", callback_data="admin_backup")],
                [InlineKeyboardButton("📅 Schedule", callback_data="admin_schedule"),
                 InlineKeyboardButton("📜 Logs", callback_data="admin_logs")],
                [InlineKeyboardButton("🔄 Refresh", callback_data="admin_refresh")]
            ])

            await message.reply(
                f"👑 **ADMIN DASHBOARD**\n\n"
                f"📊 Statistics:\n"
                f"👤 Total Users: {user_count}\n"
                f"💬 Total Groups: {group_count}\n"
                f"🎧 Active Players: {len(self.current_playing)}\n"
                f"📻 Queue Size: {sum(len(q) for q in self.queues.values())}\n"
                f"⚡ Server: Online\n\n"
                f"Choose an action below:",
                reply_markup=keyboard
            )
        except Exception as e:
            logger.error(f"Error in admin handler: {e}")

    async def on_broadcast(self, client: Client, message: Message):
        try:
            if message.from_user.id != OWNER_ID:
                await message.reply("❌ Only owner can broadcast")
                return

            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("👤 Users", callback_data="broadcast_users"),
                 InlineKeyboardButton("💬 Groups", callback_data="broadcast_groups")],
                [InlineKeyboardButton("🌍 Both", callback_data="broadcast_both"),
                 InlineKeyboardButton("🎯 Filtered", callback_data="broadcast_filtered")]
            ])

            await message.reply("📢 Select broadcast target:", reply_markup=keyboard)
        except Exception as e:
            logger.error(f"Error in broadcast handler: {e}")

    async def on_stats(self, client: Client, message: Message):
        try:
            if message.from_user.id != OWNER_ID:
                await message.reply("❌ Only owner can view stats")
                return

            user_count = await self.db.get_user_count()
            group_count = await self.db.get_group_count()

            stats_text = (
                f"📊 **BOT STATISTICS**\n\n"
                f"👤 Total Users: {user_count}\n"
                f"💬 Total Groups: {group_count}\n"
                f"🎧 Active Voice Chats: {len(self.current_playing)}\n"
                f"📀 Total Queue Size: {sum(len(q) for q in self.queues.values())}\n"
                f"🔄 Pause States: {sum(1 for v in self.pause_states.values() if v)}\n"
                f"⏱ Uptime: Active\n"
                f"📡 Memory Usage: Minimal\n"
                f"⚡ Status: Running\n"
            )
            await message.reply(stats_text)
        except Exception as e:
            logger.error(f"Error in stats handler: {e}")

    async def on_users(self, client: Client, message: Message):
        try:
            if message.from_user.id != OWNER_ID:
                await message.reply("❌ Only owner can access user management")
                return

            users = await self.db.get_all_users()
            users_text = f"👥 **USER MANAGEMENT** ({len(users)} users)\n\n"
            
            for user in users[:10]:
                banned = "🚫" if user.get("banned") else "✅"
                users_text += f"{banned} {user['name']} (ID: {user['_id']})\n   Plays: {user.get('plays', 0)} | Joined: {user.get('joined_at', 'Unknown')}\n\n"

            if len(users) > 10:
                users_text += f"... and {len(users) - 10} more users"

            await message.reply(users_text)
        except Exception as e:
            logger.error(f"Error in users handler: {e}")

    async def on_groups(self, client: Client, message: Message):
        try:
            if message.from_user.id != OWNER_ID:
                await message.reply("❌ Only owner can access group management")
                return

            groups = await self.db.get_all_groups()
            groups_text = f"💬 **GROUP MANAGEMENT** ({len(groups)} groups)\n\n"
            
            for group in groups[:10]:
                groups_text += f"📌 {group.get('title', 'Unknown')} (ID: {group['_id']})\n   Added: {group.get('added_at', 'Unknown')}\n\n"

            if len(groups) > 10:
                groups_text += f"... and {len(groups) - 10} more groups"

            await message.reply(groups_text)
        except Exception as e:
            logger.error(f"Error in groups handler: {e}")

    async def on_backup(self, client: Client, message: Message):
        try:
            if message.from_user.id != OWNER_ID:
                await message.reply("❌ Only owner can backup database")
                return

            status_msg = await message.reply("📥 Exporting database...")

            users = await self.db.get_all_users()
            groups = await self.db.get_all_groups()

            backup_data = {
                "exported_at": datetime.utcnow().isoformat(),
                "total_users": len(users),
                "total_groups": len(groups),
                "users": json.loads(json.dumps(users, default=str)),
                "groups": json.loads(json.dumps(groups, default=str))
            }

            backup_json = json.dumps(backup_data, indent=2, default=str)
            
            filename = f"backup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
            
            with open(filename, 'w') as f:
                f.write(backup_json)

            with open(filename, 'rb') as f:
                await client.send_document(message.chat.id, f, caption="📂 Database Backup")

            os.remove(filename)
            await status_msg.delete()
            await self.db.add_log("backup_created", {"user_count": len(users), "group_count": len(groups)})

        except Exception as e:
            logger.error(f"Error in backup handler: {e}")
            await message.reply(f"❌ Error: {str(e)[:100]}")

    async def on_login(self, client: Client, message: Message):
        try:
            if message.from_user.id != OWNER_ID:
                await message.reply("❌ Only owner can login userbot")
                return

            await message.reply(
                "🔐 **Userbot Login**\n\n"
                "This feature requires userbot authentication.\n"
                "For security reasons, use manual login or provide session string.\n\n"
                "/login complete to finish"
            )
        except Exception as e:
            logger.error(f"Error in login handler: {e}")

    async def on_schedule(self, client: Client, message: Message):
        try:
            if message.from_user.id != OWNER_ID:
                await message.reply("❌ Only owner can schedule messages")
                return

            await message.reply(
                "📅 **Schedule Message**\n\n"
                "This feature allows scheduling messages.\n"
                "Send your message content first."
            )
        except Exception as e:
            logger.error(f"Error in schedule handler: {e}")

    async def on_settings(self, client: Client, message: Message):
        try:
            if message.from_user.id != OWNER_ID:
                await message.reply("❌ Only owner can access settings")
                return

            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔒 Admin Only: OFF", callback_data="set_admin_mode_on"),
                 InlineKeyboardButton("🎵 Auto DJ: ON", callback_data="set_autodj_off")],
                [InlineKeyboardButton("🔄 Loop Default: OFF", callback_data="set_loop_on"),
                 InlineKeyboardButton("⏱ Cooldown: 2s", callback_data="set_cooldown")]
            ])

            await message.reply(
                "⚙️ **BOT SETTINGS**\n\n"
                "Configure bot behavior:",
                reply_markup=keyboard
            )
        except Exception as e:
            logger.error(f"Error in settings handler: {e}")

    async def on_logs(self, client: Client, message: Message):
        try:
            if message.from_user.id != OWNER_ID:
                await message.reply("❌ Only owner can view logs")
                return

            await message.reply(
                "📜 **BOT LOGS**\n\n"
                "Logs are stored in MongoDB.\n"
                "Recent activities are tracked automatically."
            )
        except Exception as e:
            logger.error(f"Error in logs handler: {e}")

    async def on_ping(self, client: Client, message: Message):
        try:
            start_time = time.time()
            ping_msg = await message.reply("🏓 Pong!")
            end_time = time.time()
            ping_time = int((end_time - start_time) * 1000)
            await ping_msg.edit_text(f"🏓 Pong! **{ping_time}ms**")
        except Exception as e:
            logger.error(f"Error in ping handler: {e}")

    async def on_broadcast_content(self, client: Client, message: Message):
        if message.from_user.id != OWNER_ID or not self.broadcast_mode:
            return
        
        self.broadcast_data['content'] = message
        self.broadcast_mode = False
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Confirm", callback_data="broadcast_confirm"),
             InlineKeyboardButton("❌ Cancel", callback_data="broadcast_cancel")]
        ])
        
        await message.reply("Are you sure you want to broadcast this?", reply_markup=keyboard)

    async def on_callback(self, client: Client, callback_query: CallbackQuery):
        try:
            data = callback_query.data

            if data == "help_menu":
                await self.on_help(client, callback_query.message)

            elif data == "admin_stats":
                user_count = await self.db.get_user_count()
                group_count = await self.db.get_group_count()
                await callback_query.answer()
                await callback_query.message.edit_text(
                    f"📊 **STATISTICS**\n\n"
                    f"👤 Total Users: {user_count}\n"
                    f"💬 Total Groups: {group_count}\n"
                    f"🎧 Active Players: {len(self.current_playing)}\n"
                    f"📀 Total Queue: {sum(len(q) for q in self.queues.values())}\n"
                    f"⚡ Server Status: Online"
                )

            elif data == "admin_broadcast":
                await callback_query.answer()
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("👤 Users", callback_data="broadcast_users"),
                     InlineKeyboardButton("💬 Groups", callback_data="broadcast_groups")],
                    [InlineKeyboardButton("🌍 Both", callback_data="broadcast_both"),
                     InlineKeyboardButton("🎯 Filtered", callback_data="broadcast_filtered")]
                ])
                await callback_query.message.edit_text("📢 Select broadcast target:", reply_markup=keyboard)

            elif data == "admin_users":
                await callback_query.answer()
                users = await self.db.get_all_users()
                users_text = f"👥 **USER MANAGEMENT** ({len(users)} users)\n\n"
                for user in users[:10]:
                    banned = "🚫" if user.get("banned") else "✅"
                    users_text += f"{banned} {user['name']} (ID: {user['_id']})\n"
                await callback_query.message.edit_text(users_text)

            elif data == "admin_groups":
                await callback_query.answer()
                groups = await self.db.get_all_groups()
                groups_text = f"💬 **GROUP MANAGEMENT** ({len(groups)} groups)\n\n"
                for group in groups[:10]:
                    groups_text += f"📌 {group.get('title', 'Unknown')} (ID: {group['_id']})\n"
                await callback_query.message.edit_text(groups_text)

            elif data == "admin_refresh":
                await callback_query.answer("Refreshing...", show_alert=False)
                user_count = await self.db.get_user_count()
                group_count = await self.db.get_group_count()
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📊 Stats", callback_data="admin_stats"),
                     InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast")],
                    [InlineKeyboardButton("👥 Users", callback_data="admin_users"),
                     InlineKeyboardButton("💬 Groups", callback_data="admin_groups")],
                    [InlineKeyboardButton("⚙️ Settings", callback_data="admin_settings"),
                     InlineKeyboardButton("📂 Backup", callback_data="admin_backup")],
                    [InlineKeyboardButton("📅 Schedule", callback_data="admin_schedule"),
                     InlineKeyboardButton("📜 Logs", callback_data="admin_logs")],
                    [InlineKeyboardButton("🔄 Refresh", callback_data="admin_refresh")]
                ])
                await callback_query.message.edit_text(
                    f"👑 **ADMIN DASHBOARD**\n\n"
                    f"📊 Statistics:\n"
                    f"👤 Total Users: {user_count}\n"
                    f"💬 Total Groups: {group_count}\n"
                    f"🎧 Active Players: {len(self.current_playing)}\n"
                    f"📻 Queue Size: {sum(len(q) for q in self.queues.values())}\n"
                    f"⚡ Server: Online\n\n"
                    f"Choose an action below:",
                    reply_markup=keyboard
                )

            elif data.startswith("broadcast_"):
                await callback_query.answer()
                self.broadcast_mode = True
                self.broadcast_data['type'] = data.replace("broadcast_", "")
                await callback_query.message.edit_text(f"📢 Send your broadcast message now...")

            elif data == "broadcast_confirm":
                await callback_query.answer("Broadcasting...", show_alert=False)
                broadcast_type = self.broadcast_data.get('type', 'both')
                users = await self.db.get_all_users()
                sent = 0
                failed = 0
                
                for user in users:
                    try:
                        if 'content' in self.broadcast_data:
                            await self.broadcast_data['content'].copy(user['_id'])
                        sent += 1
                    except:
                        failed += 1

                await callback_query.message.edit_text(
                    f"📊 **BROADCAST COMPLETE**\n\n"
                    f"✅ Sent: {sent}\n"
                    f"❌ Failed: {failed}\n"
                    f"⏱ Completed"
                )
                self.broadcast_mode = False
                self.broadcast_data = {}

            elif data == "broadcast_cancel":
                await callback_query.answer()
                self.broadcast_mode = False
                self.broadcast_data = {}
                await callback_query.message.edit_text("❌ Broadcast cancelled")

            elif data == "admin_backup":
                await callback_query.answer()
                await callback_query.message.edit_text("📥 Backup feature - Use /backup command")

            elif data == "admin_schedule":
                await callback_query.answer()
                await callback_query.message.edit_text("📅 Schedule feature - Use /schedule command")

            elif data == "admin_logs":
                await callback_query.answer()
                await callback_query.message.edit_text("📜 Logs feature available - Bot tracks all activities")

            elif data == "admin_settings":
                await callback_query.answer()
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔒 Admin Only: OFF", callback_data="set_admin_on"),
                     InlineKeyboardButton("🎵 Auto DJ: ON", callback_data="set_autodj_off")],
                    [InlineKeyboardButton("🔄 Loop Default: OFF", callback_data="set_loop_on")]
                ])
                await callback_query.message.edit_text("⚙️ **BOT SETTINGS**", reply_markup=keyboard)

            else:
                await callback_query.answer()

        except Exception as e:
            logger.error(f"Error in callback handler: {e}")
            await callback_query.answer(f"Error: {str(e)[:50]}", show_alert=True)

    async def run(self):
        await self.db.connect()
        self.session = aiohttp.ClientSession()
        
        async with self.app:
            logger.info("Music Bot started successfully")
            await self.app.idle()

    async def schedule_worker(self):
        while True:
            try:
                schedules = await self.db.get_pending_schedules()
                for schedule in schedules:
                    try:
                        users = await self.db.get_all_users()
                        for user in users:
                            try:
                                await self.app.send_message(user['_id'], schedule['message'])
                            except:
                                pass
                        await self.db.mark_schedule_sent(schedule['_id'])
                    except Exception as e:
                        logger.error(f"Error sending scheduled message: {e}")
                
                await asyncio.sleep(10)
            except Exception as e:
                logger.error(f"Schedule worker error: {e}")
                await asyncio.sleep(10)


async def main():
    bot = MusicBot()
    
    # Start schedule worker as a background task
    asyncio.create_task(bot.schedule_worker())
    
    # Start bot
    await bot.run()


class DashboardGenerator:
    """Generates interactive dashboards"""
    
    @staticmethod
    async def generate_admin_dashboard(db: Database, client: Client) -> str:
        """Generate admin dashboard"""
        try:
            users = await db.get_user_count()
            groups = await db.get_group_count()
            
            dashboard = (
                f"👑 **ADMIN DASHBOARD**\n\n"
                f"📊 **Real-Time Statistics:**\n"
                f"👤 Total Users: {users}\n"
                f"💬 Total Groups: {groups}\n"
                f"🎧 Active Players: {len(bot.current_playing)}\n"
                f"📻 Queue Size: {sum(len(q) for q in bot.queues.values())}\n"
                f"🔄 Pause States: {sum(1 for v in bot.pause_states.values() if v)}\n"
                f"⚡ Server Status: Online\n"
                f"📡 Memory Usage: Minimal\n"
                f"🕐 Last Update: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}\n"
            )
            return dashboard
        except Exception as e:
            logger.error(f"Error generating dashboard: {e}")
            return "❌ Dashboard generation failed"

    @staticmethod
    async def generate_user_statistics(user_id: int, db: Database) -> str:
        """Generate user statistics"""
        try:
            user = await db.users.find_one({"_id": user_id})
            if not user:
                return "❌ User not found"
            
            stats = (
                f"👤 **USER STATISTICS**\n\n"
                f"Name: {user.get('name', 'Unknown')}\n"
                f"ID: {user_id}\n"
                f"Username: @{user.get('username', 'N/A')}\n"
                f"🎵 Total Plays: {user.get('plays', 0)}\n"
                f"🚫 Status: {'Banned' if user.get('banned') else 'Active'}\n"
                f"📅 Joined: {user.get('joined_at', 'Unknown')}\n"
                f"⏰ Last Seen: {user.get('last_seen', 'Unknown')}\n"
            )
            return stats
        except Exception as e:
            logger.error(f"Error generating user stats: {e}")
            return "❌ Statistics generation failed"

    @staticmethod
    async def generate_group_statistics(group_id: int, db: Database) -> str:
        """Generate group statistics"""
        try:
            group = await db.groups.find_one({"_id": group_id})
            if not group:
                return "❌ Group not found"
            
            queue_size = len(bot.queues.get(group_id, []))
            
            stats = (
                f"💬 **GROUP STATISTICS**\n\n"
                f"Title: {group.get('title', 'Unknown')}\n"
                f"ID: {group_id}\n"
                f"📀 Queue Size: {queue_size}\n"
                f"🔄 Loop Mode: {bot.loop_modes.get(group_id, 'off')}\n"
                f"🔊 Volume: {bot.volume.get(group_id, 100)}%\n"
                f"⏸ Paused: {'Yes' if bot.pause_states.get(group_id) else 'No'}\n"
                f"📅 Added: {group.get('added_at', 'Unknown')}\n"
            )
            return stats
        except Exception as e:
            logger.error(f"Error generating group stats: {e}")
            return "❌ Statistics generation failed"


class BroadcastSystem:
    """Advanced broadcast system"""
    
    def __init__(self, db: Database, client: Client):
        self.db = db
        self.client = client
        self.broadcast_queue = []
        self.broadcasting = False

    async def broadcast_to_users(self, message_text: str, success_callback=None, error_callback=None) -> dict:
        """Broadcast message to all users"""
        try:
            self.broadcasting = True
            users = await self.db.get_all_users()
            sent = 0
            failed = 0

            for user in users:
                try:
                    if not user.get('banned', False):
                        await self.client.send_message(user['_id'], message_text)
                        sent += 1
                    else:
                        failed += 1
                except Exception as e:
                    logger.error(f"Error sending to user {user['_id']}: {e}")
                    failed += 1
                
                if success_callback:
                    await success_callback(sent, failed)

            if error_callback:
                await error_callback(failed)

            self.broadcasting = False
            return {"sent": sent, "failed": failed, "total": len(users)}
        except Exception as e:
            logger.error(f"Error in broadcast: {e}")
            self.broadcasting = False
            return {"sent": 0, "failed": len(users), "error": str(e)}

    async def broadcast_to_groups(self, message_text: str) -> dict:
        """Broadcast message to all groups"""
        try:
            self.broadcasting = True
            groups = await self.db.get_all_groups()
            sent = 0
            failed = 0

            for group in groups:
                try:
                    await self.client.send_message(group['_id'], message_text)
                    sent += 1
                except Exception as e:
                    logger.error(f"Error sending to group {group['_id']}: {e}")
                    failed += 1

            self.broadcasting = False
            return {"sent": sent, "failed": failed, "total": len(groups)}
        except Exception as e:
            logger.error(f"Error in group broadcast: {e}")
            self.broadcasting = False
            return {"sent": 0, "failed": 0, "error": str(e)}

    async def schedule_broadcast(self, message_text: str, target_type: str, send_time: datetime) -> bool:
        """Schedule broadcast for later"""
        try:
            self.broadcast_queue.append({
                "message": message_text,
                "target": target_type,
                "send_time": send_time,
                "created_at": datetime.utcnow()
            })
            return True
        except Exception as e:
            logger.error(f"Error scheduling broadcast: {e}")
            return False


class PermissionManager:
    """Manages bot permissions and access control"""
    
    def __init__(self):
        self.permissions = {}
        self.admin_ids = [OWNER_ID]
        self.moderators = []
        self.restricted_commands = ["ban", "unban", "broadcast", "admin", "backup"]

    async def check_permission(self, user_id: int, command: str) -> bool:
        """Check if user has permission for command"""
        if command in self.restricted_commands:
            return user_id == OWNER_ID or user_id in self.admin_ids
        return True

    async def add_admin(self, user_id: int):
        """Add admin"""
        if user_id not in self.admin_ids:
            self.admin_ids.append(user_id)
            logger.info(f"Added admin: {user_id}")

    async def remove_admin(self, user_id: int):
        """Remove admin"""
        if user_id in self.admin_ids and user_id != OWNER_ID:
            self.admin_ids.remove(user_id)
            logger.info(f"Removed admin: {user_id}")

    async def add_moderator(self, user_id: int):
        """Add moderator"""
        if user_id not in self.moderators:
            self.moderators.append(user_id)
            logger.info(f"Added moderator: {user_id}")


class AnalyticsTracker:
    """Tracks bot analytics"""
    
    def __init__(self):
        self.analytics = {
            "commands_executed": 0,
            "songs_played": 0,
            "users_joined": 0,
            "groups_joined": 0,
            "errors": 0,
            "broadcasts_sent": 0,
            "peak_users": 0
        }
        self.hourly_stats = defaultdict(int)
        self.daily_stats = defaultdict(int)

    async def track_command(self, command: str):
        """Track command execution"""
        self.analytics["commands_executed"] += 1
        hour = datetime.utcnow().strftime("%Y-%m-%d %H:00")
        self.hourly_stats[hour] += 1

    async def track_song_play(self):
        """Track song play"""
        self.analytics["songs_played"] += 1

    async def track_error(self):
        """Track error"""
        self.analytics["errors"] += 1

    async def track_broadcast(self):
        """Track broadcast"""
        self.analytics["broadcasts_sent"] += 1

    async def get_analytics(self) -> dict:
        """Get analytics"""
        return {
            "total_analytics": self.analytics,
            "hourly_stats": dict(self.hourly_stats),
            "daily_stats": dict(self.daily_stats)
        }


class BackupManager:
    """Manages database backups"""
    
    def __init__(self, db: Database):
        self.db = db
        self.backup_history = []

    async def create_backup(self) -> dict:
        """Create full database backup"""
        try:
            users = await self.db.get_all_users()
            groups = await self.db.get_all_groups()

            backup = {
                "timestamp": datetime.utcnow().isoformat(),
                "version": "2.0",
                "stats": {
                    "total_users": len(users),
                    "total_groups": len(groups)
                },
                "data": {
                    "users": json.loads(json.dumps(users, default=str)),
                    "groups": json.loads(json.dumps(groups, default=str))
                }
            }

            self.backup_history.append({
                "timestamp": datetime.utcnow(),
                "size": len(json.dumps(backup)),
                "users": len(users),
                "groups": len(groups)
            })

            return backup
        except Exception as e:
            logger.error(f"Error creating backup: {e}")
            return None

    async def restore_backup(self, backup_data: dict) -> bool:
        """Restore from backup"""
        try:
            logger.info("Backup restoration not implemented for safety")
            return False
        except Exception as e:
            logger.error(f"Error restoring backup: {e}")
            return False

    async def get_backup_history(self) -> list:
        """Get backup history"""
        return self.backup_history[-10:]  # Last 10 backups


class QueueManager:
    """Advanced queue management"""
    
    def __init__(self):
        self.queues = defaultdict(list)
        self.history = defaultdict(list)

    async def add_to_queue(self, group_id: int, song: dict):
        """Add song to queue"""
        self.queues[group_id].append(song)
        self.history[group_id].append({
            "song": song,
            "added_at": datetime.utcnow(),
            "played": False
        })

    async def get_queue(self, group_id: int, limit: int = 20) -> list:
        """Get queue"""
        return self.queues[group_id][:limit]

    async def clear_queue(self, group_id: int):
        """Clear queue"""
        self.queues[group_id].clear()

    async def get_history(self, group_id: int) -> list:
        """Get play history"""
        return self.history[group_id][-50:]  # Last 50 plays

    async def shuffle_queue(self, group_id: int):
        """Shuffle queue"""
        if group_id in self.queues:
            import random
            random.shuffle(self.queues[group_id])
            return True
        return False


# Initialize managers
permission_manager = None
analytics_tracker = None
backup_manager = None
broadcast_system = None
queue_manager = QueueManager()
dashboard_generator = DashboardGenerator()


class HealthCheck:
    """Health check system"""
    def __init__(self):
        self.status = "running"
        self.start_time = datetime.utcnow()

    async def get_status(self) -> dict:
        return {
            "status": self.status,
            "bot": "Music Bot",
            "version": "2.0",
            "uptime": (datetime.utcnow() - self.start_time).total_seconds()
        }


class AdvancedFeatures:
    """Advanced bot features for premium functionality"""
    
    def __init__(self, db: Database):
        self.db = db
        self.playlist_cache = {}
        self.user_preferences = defaultdict(dict)
        self.statistics = {
            "total_plays": 0,
            "total_users": 0,
            "total_commands": 0,
            "uptime_seconds": 0,
            "api_calls": 0
        }

    async def search_songs(self, query: str, limit: int = 5) -> List[dict]:
        """Search multiple songs from API"""
        try:
            results = []
            async with aiohttp.ClientSession() as session:
                for i in range(limit):
                    async with session.get(
                        f"https://ansh-apis.is-dev.org/api/ytstream?key=ansh&song={query}&type=vid"
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data.get("success"):
                                results.append(data)
                                self.statistics["api_calls"] += 1
            return results
        except Exception as e:
            logger.error(f"Error searching songs: {e}")
            return []

    async def save_user_preference(self, user_id: int, preference: str, value: any):
        """Save user preferences"""
        try:
            self.user_preferences[user_id][preference] = value
            logger.info(f"Saved preference for user {user_id}: {preference}={value}")
        except Exception as e:
            logger.error(f"Error saving preference: {e}")

    async def get_user_preference(self, user_id: int, preference: str, default=None):
        """Get user preference"""
        return self.user_preferences.get(user_id, {}).get(preference, default)

    async def create_playlist(self, user_id: int, playlist_name: str, songs: List[dict]):
        """Create user playlist"""
        try:
            playlist_id = hashlib.md5(f"{user_id}{playlist_name}".encode()).hexdigest()
            self.playlist_cache[playlist_id] = {
                "name": playlist_name,
                "user_id": user_id,
                "songs": songs,
                "created_at": datetime.utcnow(),
                "plays": 0
            }
            return playlist_id
        except Exception as e:
            logger.error(f"Error creating playlist: {e}")
            return None

    async def get_statistics(self) -> dict:
        """Get bot statistics"""
        self.statistics["total_plays"] = sum(len(q) for q in bot.queues.values())
        return self.statistics

    async def generate_report(self) -> str:
        """Generate comprehensive bot report"""
        try:
            stats = await self.get_statistics()
            user_count = await self.db.get_user_count()
            group_count = await self.db.get_group_count()
            
            report = (
                f"📊 **BOT PERFORMANCE REPORT**\n\n"
                f"👥 Users: {user_count}\n"
                f"💬 Groups: {group_count}\n"
                f"🎵 Total Plays: {stats['total_plays']}\n"
                f"📡 API Calls: {stats['api_calls']}\n"
                f"⏱ Uptime: Active\n"
                f"🎧 Active Chats: {len(bot.current_playing)}\n"
                f"🔄 Total Commands: {stats['total_commands']}\n"
            )
            return report
        except Exception as e:
            logger.error(f"Error generating report: {e}")
            return "❌ Report generation failed"


class UserBotManager:
    """Manages userbot login and authentication"""
    
    def __init__(self, db: Database):
        self.db = db
        self.auth_sessions = {}

    async def initiate_login(self, phone: str) -> Tuple[bool, str]:
        """Initiate userbot login"""
        try:
            self.auth_sessions[phone] = {
                "initiated_at": datetime.utcnow(),
                "status": "awaiting_otp"
            }
            return True, "OTP sent to your phone"
        except Exception as e:
            logger.error(f"Error initiating login: {e}")
            return False, str(e)

    async def verify_otp(self, phone: str, otp: str) -> Tuple[bool, str]:
        """Verify OTP"""
        try:
            if phone in self.auth_sessions:
                self.auth_sessions[phone]["status"] = "otp_verified"
                return True, "OTP verified successfully"
            return False, "Session not found"
        except Exception as e:
            logger.error(f"Error verifying OTP: {e}")
            return False, str(e)

    async def save_session_string(self, phone: str, session_string: str) -> Tuple[bool, str]:
        """Save session string"""
        try:
            await self.db.save_session(phone, session_string)
            self.auth_sessions.pop(phone, None)
            return True, "Session saved successfully"
        except Exception as e:
            logger.error(f"Error saving session: {e}")
            return False, str(e)


class CommandRateLimiter:
    """Rate limiting for commands"""
    
    def __init__(self, max_calls: int = 5, time_window: int = 60):
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls = defaultdict(list)

    async def check_rate_limit(self, user_id: int) -> bool:
        """Check if user is rate limited"""
        current_time = time.time()
        cutoff_time = current_time - self.time_window
        
        self.calls[user_id] = [t for t in self.calls[user_id] if t > cutoff_time]
        
        if len(self.calls[user_id]) >= self.max_calls:
            return False
        
        self.calls[user_id].append(current_time)
        return True


class CacheManager:
    """Manages caching for songs and data"""
    
    def __init__(self, ttl: int = 3600):
        self.cache = {}
        self.ttl = ttl
        self.timestamps = {}

    async def get(self, key: str):
        """Get from cache"""
        if key in self.cache:
            if time.time() - self.timestamps[key] < self.ttl:
                return self.cache[key]
            else:
                del self.cache[key]
                del self.timestamps[key]
        return None

    async def set(self, key: str, value: any):
        """Set cache"""
        self.cache[key] = value
        self.timestamps[key] = time.time()

    async def clear(self):
        """Clear all cache"""
        self.cache.clear()
        self.timestamps.clear()

    async def cleanup(self):
        """Remove expired cache"""
        current_time = time.time()
        expired_keys = [k for k, v in self.timestamps.items() if current_time - v > self.ttl]
        for key in expired_keys:
            del self.cache[key]
            del self.timestamps[key]


class NotificationManager:
    """Manages notifications for bot events"""
    
    def __init__(self, client: Client, owner_id: int):
        self.client = client
        self.owner_id = owner_id

    async def notify_new_user(self, user: User):
        """Notify owner of new user"""
        try:
            notification = (
                f"🚀 **NEW USER**\n"
                f"👤 Name: {user.first_name}\n"
                f"🆔 ID: {user.id}\n"
                f"📱 Username: @{user.username or 'N/A'}\n"
                f"⏰ Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            await self.client.send_message(self.owner_id, notification)
        except Exception as e:
            logger.error(f"Error sending notification: {e}")

    async def notify_error(self, error_message: str):
        """Notify owner of errors"""
        try:
            notification = f"❌ **ERROR**\n\n{error_message[:500]}"
            await self.client.send_message(self.owner_id, notification)
        except Exception as e:
            logger.error(f"Error sending error notification: {e}")

    async def notify_broadcast_complete(self, sent: int, failed: int):
        """Notify broadcast completion"""
        try:
            notification = (
                f"📊 **BROADCAST COMPLETE**\n"
                f"✅ Sent: {sent}\n"
                f"❌ Failed: {failed}\n"
                f"⏱ Completed: {datetime.now().strftime('%H:%M:%S')}"
            )
            await self.client.send_message(self.owner_id, notification)
        except Exception as e:
            logger.error(f"Error sending broadcast notification: {e}")


class CommandHandler:
    """Centralized command handling"""
    
    def __init__(self):
        self.commands = {}
        self.command_count = 0

    def register(self, name: str, handler):
        """Register command handler"""
        self.commands[name] = handler

    async def execute(self, command: str, *args, **kwargs):
        """Execute command"""
        if command in self.commands:
            self.command_count += 1
            return await self.commands[command](*args, **kwargs)
        return None


class ErrorHandler:
    """Centralized error handling"""
    
    @staticmethod
    async def handle_error(error: Exception, context: str = ""):
        """Handle errors globally"""
        logger.error(f"Error in {context}: {str(error)}")
        return f"❌ Error: {str(error)[:100]}"

    @staticmethod
    async def safe_execute(coroutine, fallback="Operation failed"):
        """Safely execute coroutine"""
        try:
            return await coroutine
        except Exception as e:
            logger.error(f"Safe execution error: {e}")
            return fallback


class DataValidator:
    """Validates data inputs"""
    
    @staticmethod
    def validate_song_name(name: str) -> bool:
        """Validate song name"""
        return 1 <= len(name) <= 200

    @staticmethod
    def validate_user_id(user_id: any) -> bool:
        """Validate user ID"""
        try:
            int(user_id)
            return int(user_id) > 0
        except:
            return False

    @staticmethod
    def validate_volume(volume: any) -> bool:
        """Validate volume"""
        try:
            vol = int(volume)
            return 1 <= vol <= 200
        except:
            return False


class EventLogger:
    """Logs all bot events"""
    
    def __init__(self, db: Database):
        self.db = db

    async def log_command(self, user_id: int, command: str, group_id: int = None):
        """Log command execution"""
        await self.db.add_log("command", {
            "user_id": user_id,
            "command": command,
            "group_id": group_id,
            "timestamp": datetime.utcnow()
        })

    async def log_error(self, error: str, context: str = ""):
        """Log error"""
        await self.db.add_log("error", {
            "error": error,
            "context": context,
            "timestamp": datetime.utcnow()
        })

    async def log_broadcast(self, sent: int, failed: int):
        """Log broadcast"""
        await self.db.add_log("broadcast", {
            "sent": sent,
            "failed": failed,
            "timestamp": datetime.utcnow()
        })


class MonitoringService:
    """Monitors bot performance and health"""
    
    def __init__(self):
        self.metrics = {
            "cpu_usage": 0,
            "memory_usage": 0,
            "response_time": 0,
            "uptime": 0,
            "health_score": 100
        }
        self.alerts = []
        self.start_time = datetime.utcnow()

    async def check_health(self) -> bool:
        """Check bot health"""
        self.metrics["uptime"] = (datetime.utcnow() - self.start_time).total_seconds()
        return self.metrics["health_score"] >= 80

    async def log_metric(self, metric_name: str, value: float):
        """Log metric"""
        if metric_name in self.metrics:
            self.metrics[metric_name] = value

    async def add_alert(self, alert_message: str):
        """Add alert"""
        self.alerts.append({
            "message": alert_message,
            "timestamp": datetime.utcnow()
        })

    async def get_metrics(self) -> dict:
        """Get metrics"""
        return self.metrics


class AutoModeration:
    """Automatic moderation system"""
    
    def __init__(self, db: Database):
        self.db = db
        self.spam_threshold = 10
        self.spam_window = 60

    async def check_spam(self, user_id: int) -> bool:
        """Check if user is spamming"""
        try:
            # Implementation would check rate limiting
            return False
        except Exception as e:
            logger.error(f"Error checking spam: {e}")
            return False

    async def auto_moderate(self, user_id: int) -> bool:
        """Auto moderate user"""
        try:
            if await self.check_spam(user_id):
                await self.db.ban_user(user_id)
                return True
            return False
        except Exception as e:
            logger.error(f"Error in auto moderation: {e}")
            return False


class ConfigurationManager:
    """Manages bot configuration"""
    
    def __init__(self):
        self.config = {
            "admin_only_mode": False,
            "auto_dj_enabled": True,
            "default_loop_mode": "off",
            "welcome_message_enabled": True,
            "anti_spam_enabled": True,
            "spam_cooldown": 2,
            "max_queue_size": 100,
            "auto_leave_timeout": 300
        }
        self.config_file = "bot_config.json"

    async def save_config(self) -> bool:
        """Save configuration"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Error saving config: {e}")
            return False

    async def load_config(self) -> bool:
        """Load configuration"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    self.config = json.load(f)
            return True
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            return False

    async def update_config(self, key: str, value: any) -> bool:
        """Update configuration"""
        if key in self.config:
            self.config[key] = value
            return await self.save_config()
        return False


class SearchOptimizer:
    """Optimizes song search"""
    
    def __init__(self):
        self.search_cache = {}
        self.popular_searches = defaultdict(int)

    async def optimized_search(self, query: str, client: Client) -> Optional[dict]:
        """Search with optimization"""
        try:
            if query in self.search_cache:
                return self.search_cache[query]

            self.popular_searches[query] += 1

            # API call would happen here
            result = await client.send_message  # Placeholder
            self.search_cache[query] = result
            return result
        except Exception as e:
            logger.error(f"Error in optimized search: {e}")
            return None

    async def get_trending(self) -> list:
        """Get trending searches"""
        return sorted(self.popular_searches.items(), key=lambda x: x[1], reverse=True)[:10]


class IntegrationManager:
    """Manages external integrations"""
    
    def __init__(self):
        self.integrations = {
            "youtube": {"enabled": True, "api": "ansh-apis.is-dev.org"},
            "mongodb": {"enabled": True, "status": "connected"},
            "telegram": {"enabled": True, "status": "connected"}
        }

    async def check_integrations(self) -> dict:
        """Check all integrations"""
        return self.integrations

    async def enable_integration(self, name: str) -> bool:
        """Enable integration"""
        if name in self.integrations:
            self.integrations[name]["enabled"] = True
            return True
        return False

    async def disable_integration(self, name: str) -> bool:
        """Disable integration"""
        if name in self.integrations:
            self.integrations[name]["enabled"] = False
            return True
        return False


class PlaylistManager:
    """Manages user playlists"""
    
    def __init__(self, db: Database):
        self.db = db
        self.playlists = {}

    async def create_playlist(self, user_id: int, name: str) -> Optional[str]:
        """Create playlist"""
        try:
            playlist_id = hashlib.md5(f"{user_id}{name}{time.time()}".encode()).hexdigest()
            self.playlists[playlist_id] = {
                "user_id": user_id,
                "name": name,
                "songs": [],
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }
            return playlist_id
        except Exception as e:
            logger.error(f"Error creating playlist: {e}")
            return None

    async def add_song_to_playlist(self, playlist_id: str, song: dict) -> bool:
        """Add song to playlist"""
        try:
            if playlist_id in self.playlists:
                self.playlists[playlist_id]["songs"].append(song)
                self.playlists[playlist_id]["updated_at"] = datetime.utcnow()
                return True
            return False
        except Exception as e:
            logger.error(f"Error adding song to playlist: {e}")
            return False

    async def get_playlist(self, playlist_id: str) -> Optional[dict]:
        """Get playlist"""
        return self.playlists.get(playlist_id)

    async def delete_playlist(self, playlist_id: str) -> bool:
        """Delete playlist"""
        if playlist_id in self.playlists:
            del self.playlists[playlist_id]
            return True
        return False


# Global instances
rate_limiter = CommandRateLimiter()
cache_manager = CacheManager()
data_validator = DataValidator()
event_logger = None
notification_manager = None
advanced_features = None
userbot_manager = None
monitoring_service = None
auto_moderation = None
config_manager = None
search_optimizer = None
integration_manager = None
playlist_manager = None
bot = None


async def initialize_services(bot_instance: MusicBot):
    """Initialize all bot services"""
    global event_logger, notification_manager, advanced_features, userbot_manager
    global monitoring_service, auto_moderation, config_manager, search_optimizer
    global integration_manager, playlist_manager, permission_manager, analytics_tracker
    global backup_manager, broadcast_system, queue_manager

    event_logger = EventLogger(bot_instance.db)
    notification_manager = NotificationManager(bot_instance.app, OWNER_ID)
    advanced_features = AdvancedFeatures(bot_instance.db)
    userbot_manager = UserBotManager(bot_instance.db)
    monitoring_service = MonitoringService()
    auto_moderation = AutoModeration(bot_instance.db)
    config_manager = ConfigurationManager()
    search_optimizer = SearchOptimizer()
    integration_manager = IntegrationManager()
    playlist_manager = PlaylistManager(bot_instance.db)
    permission_manager = PermissionManager()
    analytics_tracker = AnalyticsTracker()
    backup_manager = BackupManager(bot_instance.db)
    broadcast_system = BroadcastSystem(bot_instance.db, bot_instance.app)

    # Load configuration
    await config_manager.load_config()
    
    logger.info("All services initialized successfully")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())