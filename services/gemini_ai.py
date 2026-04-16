"""Integracao simples com Groq para a IA dos grupos."""

import aiohttp

from config import GROQ_API_KEY, GROUP_AI_HTTP_TIMEOUT

NO_REPLY_TOKEN = "__NO_REPLY__"

SYSTEM_PROMPT = (
    "Voce e um assistente especializado em series e filmes chamado Akira.\n"
    "Responda sempre em portugues do Brasil de forma amigavel e objetiva.\n"
    "Quando mencionar titulos, use <b>Nome do Titulo</b>.\n"
    "Se nao souber algo, diga que nao sabe.\n"
    "Mantenha respostas curtas ou medias.\n"
    "Se a mensagem nao for sobre series, filmes, streaming, atores, diretores "
    "ou entretenimento e tambem nao for um cumprimento, responda apenas: "
    f"{NO_REPLY_TOKEN}"
)


def split_for_telegram(text: str, limit: int = 4000) -> list[str]:
    if len(text) <= limit:
        return [text]

    parts: list[str] = []
    current = text
    while current:
        parts.append(current[:limit])
        current = current[limit:]
    return parts


def _history_messages(messages: list | None = None, history: list | None = None) -> list[dict]:
    source = history if isinstance(history, list) else messages
    if not isinstance(source, list):
        return []

    normalized: list[dict] = []
    for item in source:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "").strip()
        content = str(item.get("content") or "").strip()
        if role and content:
            normalized.append({"role": role, "content": content[:1200]})
    return normalized[-10:]


async def generate_anime_reply(
    user_message: str,
    messages: list | None = None,
    history: list | None = None,
) -> str:
    if not GROQ_API_KEY:
        return NO_REPLY_TOKEN

    payload_messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    payload_messages.extend(_history_messages(messages=messages, history=history))
    payload_messages.append({"role": "user", "content": str(user_message or "").strip()[:900]})

    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=GROUP_AI_HTTP_TIMEOUT)
        ) as session:
            async with session.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {GROQ_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "llama-3.3-70b-versatile",
                    "messages": payload_messages,
                    "max_tokens": 350,
                    "temperature": 0.7,
                },
            ) as response:
                if response.status != 200:
                    detail = await response.text()
                    raise RuntimeError(f"Groq API retornou {response.status}: {detail[:200]}")
                data = await response.json()
                return str(data["choices"][0]["message"]["content"] or "").strip()
    except Exception as exc:
        print(f"[group_ai] erro: {exc}")
        return NO_REPLY_TOKEN


async def generate_group_reply(
    user_message: str,
    messages: list | None = None,
    history: list | None = None,
) -> str:
    return await generate_anime_reply(user_message, messages=messages, history=history)
