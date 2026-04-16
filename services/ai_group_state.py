import random
import time

_group_last_reply = {}

COOLDOWN_SECONDS = 20 * 60
RANDOM_REPLY_CHANCE_PERCENT = 5

def can_reply(chat_id: int) -> bool:
    now = time.time()
    last = _group_last_reply.get(chat_id, 0)
    return (now - last) >= COOLDOWN_SECONDS

def mark_reply(chat_id: int) -> None:
    _group_last_reply[chat_id] = time.time()

def should_random_reply() -> bool:
    return random.randint(1, 100) <= RANDOM_REPLY_CHANCE_PERCENT
