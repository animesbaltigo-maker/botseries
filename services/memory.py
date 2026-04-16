"""Memória de conversas para a IA de grupo."""
import time
from collections import defaultdict

_MEMORY: dict = defaultdict(list)
_LAST_ACCESS: dict = {}
MAX_TURNS = 6
TTL = 60 * 30  # 30 min


def _clean_old():
    now = time.time()
    expired = [k for k, t in _LAST_ACCESS.items() if now - t > TTL]
    for k in expired:
        _MEMORY.pop(k, None)
        _LAST_ACCESS.pop(k, None)


class _ConversationMemory:
    def get(self, chat_id: int) -> list:
        _clean_old()
        _LAST_ACCESS[chat_id] = time.time()
        return list(_MEMORY[chat_id])

    def add(self, chat_id: int, role: str, content: str):
        _LAST_ACCESS[chat_id] = time.time()
        _MEMORY[chat_id].append({"role": role, "content": content})
        # Mantém só os últimos N turnos (cada turno = 2 mensagens)
        if len(_MEMORY[chat_id]) > MAX_TURNS * 2:
            _MEMORY[chat_id] = _MEMORY[chat_id][-(MAX_TURNS * 2):]

    def clear(self, chat_id: int):
        _MEMORY.pop(chat_id, None)
        _LAST_ACCESS.pop(chat_id, None)



    def get_history(self, chat_id: int) -> list:
        """Alias para get() — retorna histórico no formato messages list."""
        return self.get(chat_id)

    def add_turn(self, chat_id: int, user_text: str, assistant_reply: str):
        """Adiciona um turno completo (user + assistant)."""
        self.add(chat_id, "user", user_text)
        self.add(chat_id, "assistant", assistant_reply)

conversation_memory = _ConversationMemory()
