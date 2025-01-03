import time
import heapq
from datetime import datetime, timedelta
import telebot
import os
import json
import redis

# Redis connection
redis_url_ev_db = os.getenv("REDIS_Live_EV_DB_URL")
redis_client_ev_db = redis.StrictRedis.from_url(redis_url_ev_db, decode_responses=True)

# Initialize the Telebot bot
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
telegram_bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

# Priority Queue for Notifications
notification_queue = []
ev_values = {}  # Cache to track arbitrage states
queue_keys = set()  # Track currently queued keys to prevent duplication

def calculate_notification_interval(ev_percent):
    """
    Calculate the time interval for the next notification based on arbitrage value.
    Higher arbitrage values get shorter intervals.
    """
    if ev_percent >= 7:
        return 10  # Every 10 seconds
    elif ev_percent >= 5:
        return 30  # Every 30 seconds
    elif ev_percent >= -3:
        return 60  # Every 60 seconds
    else:
        return None  # Do not notify

def format_notification_message(entry):
    """
    Formats the message for Telegram notification.
    """
    try:
        # Clean and format odds
        spool_odds = entry.get('spool_odds', '[]')
        normalised_gem_odds = entry.get('normalised_gem_odds', '[]')

        # Parse the odds and bets as lists if they're stored as JSON strings
        try:
            spool_odds = json.loads(spool_odds)
            normalised_gem_odds = json.loads(normalised_gem_odds)
        except (ValueError, TypeError):
            pass  # Keep as is if parsing fails

        # Ensure spool_odds and normalised_gem_odds are lists
        if not isinstance(spool_odds, list):
            spool_odds = [spool_odds]
        if not isinstance(normalised_gem_odds, list):
            normalised_gem_odds = [normalised_gem_odds]

        current_match_time = entry.get('current_match_time', 'N/A')
        if current_match_time == 'HT':
            current_match_time = 'Half-Time'
            formatted_time = current_match_time

        elif current_match_time != 'N/A':          
            formatted_time = str(current_match_time) + ' mins'
            current_match_time = ''
        else:
            current_match_time = ''
            formatted_time = current_match_time
        # Format bets by joining elements without brackets
        formatted_spool_odds = " ".join(map(str, spool_odds))
        formatted_normalised_gem_odds = " ".join(map(str, normalised_gem_odds))

        # Get the current time for when the message is sent (adjust for GMT+8)
        current_time_gmt8 = (datetime.now() + timedelta(hours=8)).strftime("%H:%M:%S")

        # Construct the message
        return (
            f"<b>{current_match_time}LIVE {float(entry.get('ev_percent', 0)):.2f}%</b> | {current_time_gmt8}\n"
            f"<b>{entry.get('spool_game')}</b>\n"
            f"<b>Live Match Time:</b> {formatted_time}\n"
            f"<b>Bet Market:</b> {entry.get('market')}\n"
            f"<b>Outcome:</b> {entry.get('outcome')}\n"
            f"<b>Spool Odds: {formatted_spool_odds}</b>\n"
            f"<b>Normalised Gem Odds: {formatted_normalised_gem_odds}</b>\n"
            f"{entry.get('spool_link')}\n"
            f"{entry.get('gem_game_url')}"
        )
    except Exception as e:
        print(f"Error formatting message: {e}")
        return "Error formatting message."
    
def refresh_notification_queue(redis_client_ev_db):
    """
    Refresh the notification queue with data from Redis, ensuring existing intervals are not affected.
    Remove bets that no longer exist in Redis.
    """
    redis_keys = set(redis_client_ev_db.keys("ev:*"))
    current_time = datetime.now()
    
    # Extract unique IDs from Redis keys
    keys = {key.split("ev:")[1] for key in redis_keys}
    
    # Debug statement to check the keys fetched from Redis
    print(f"Fetched keys from Redis: {keys}")
    print(f"Queue keys: {queue_keys}")

    # Remove keys in the queue that no longer exist in Redis
    removed_keys = queue_keys - keys
    if removed_keys:
        for removed_key in removed_keys:
            queue_keys.discard(removed_key)
            # Remove entries from the notification_queue
            notification_queue[:] = [item for item in notification_queue if item[1] != removed_key]
            print(f"Removed outdated key from queue: {removed_key}")

    for key in redis_keys:
        entry = redis_client_ev_db.hgetall(key)
        if not entry:
            continue

        ev_percent = float(entry.get("ev_percent", 0))
        match = entry.get("spool_game")
        market = entry.get("market")
        unique_id = f" {match} {market} {str(ev_percent)}"

        # Check if the arbitrage is new or updated
        if unique_id not in ev_values or ev_values[unique_id] != ev_percent:
            # Send immediate notification
            send_immediate_notification(entry)
            ev_values[unique_id] = ev_percent  # Update the stored value

            # Remove any existing entry with the same unique_id from the queue
            notification_queue[:] = [item for item in notification_queue if item[1] != unique_id]
            queue_keys.discard(unique_id)

            # Add to the queue with updated interval
            interval = calculate_notification_interval(ev_percent)
            if interval is not None:
                next_notification_time = current_time + timedelta(seconds=interval)
                heapq.heappush(notification_queue, (next_notification_time, unique_id, entry))
                queue_keys.add(unique_id)
        elif unique_id not in queue_keys:
            # Add to the queue only if it was not already queued
            interval = calculate_notification_interval(ev_percent)
            if interval is not None:
                next_notification_time = current_time + timedelta(seconds=interval)
                heapq.heappush(notification_queue, (next_notification_time, unique_id, entry))
                queue_keys.add(unique_id)


def process_notifications():
    """
    Process the notification queue and send messages when due.
    """
    while notification_queue:
        #print(f"Notification queue: {notification_queue}")
        next_notification_time, unique_id, entry = heapq.heappop(notification_queue)
        if datetime.now() >= next_notification_time:
            # Send the notification
            message = format_notification_message(entry)
            try:
                telegram_bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode='HTML')
                print(f"Notification sent for unique_id: {unique_id}")
                queue_keys.discard(unique_id)  # Remove from tracked keys
            except Exception as e:
                print(f"Failed to send notification for unique_id {unique_id}: {e}")

            # Recalculate next notification time and re-add to the queue
            ev_percent = float(entry.get("ev_percent", 0))
            interval = calculate_notification_interval(ev_percent)
            if interval is not None:
                next_notification_time = datetime.now() + timedelta(seconds=interval)
                heapq.heappush(notification_queue, (next_notification_time, unique_id, entry))
                queue_keys.add(unique_id)  # Add to tracked keys
        else:
            # Not yet time to notify, re-add to the queue
            heapq.heappush(notification_queue, (next_notification_time, unique_id, entry))
            queue_keys.add(unique_id)
            break  # Exit the loop to wait until the next due time


def send_immediate_notification(entry):
    """
    Send a notification immediately for new or updated arbitrages.
    """
    message = format_notification_message(entry)
    try:
        telegram_bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode='HTML')
        print("Immediate notification sent.")
    except Exception as e:
        print(f"Failed to send immediate notification: {e}")


if __name__ == "__main__":
    print("Starting notification system...")
    while True:
        refresh_notification_queue(redis_client_ev_db)
        process_notifications()
        time.sleep(2)
