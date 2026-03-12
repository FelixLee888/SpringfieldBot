#!/usr/bin/env python3
"""Run Springfield Price Bot against Telegram using long polling."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests

ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = ROOT / ".env"
PIPELINE = ROOT / "scripts" / "springfield_price_pipeline.py"
OFFSET_PATH = ROOT / "data" / "telegram_offset.txt"
REQUEST_TIMEOUT = 35
DEFAULT_POLL_TIMEOUT = 25


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def get_token() -> str:
    load_env_file(ENV_PATH)
    return os.getenv("TELEGRAM_BOT_TOKEN", "").strip()


def get_allowed_chat_ids() -> List[str]:
    raw = os.getenv("SPRINGFIELD_PRICE_ALLOWED_CHAT_IDS", "").strip()
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def telegram_api(method: str, token: str, payload: Optional[Dict[str, object]] = None, timeout: int = REQUEST_TIMEOUT) -> Dict[str, object]:
    url = f"https://api.telegram.org/bot{token}/{method}"
    response = requests.post(url, json=payload or {}, timeout=timeout)
    response.raise_for_status()
    data = response.json()
    if not data.get("ok"):
        raise RuntimeError(str(data.get("description") or f"Telegram API error for {method}"))
    result = data.get("result")
    return result if isinstance(result, dict) else {"items": result}


def telegram_get_updates(token: str, offset: int, poll_timeout: int) -> List[Dict[str, object]]:
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    payload = {"offset": offset, "timeout": poll_timeout, "allowed_updates": ["message"]}
    response = requests.post(url, json=payload, timeout=poll_timeout + 10)
    response.raise_for_status()
    data = response.json()
    if not data.get("ok"):
        raise RuntimeError(str(data.get("description") or "Telegram getUpdates failed"))
    result = data.get("result")
    return result if isinstance(result, list) else []


def send_message(token: str, chat_id: str, text: str, reply_to_message_id: Optional[int] = None) -> None:
    payload: Dict[str, object] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_to_message_id is not None:
        payload["reply_to_message_id"] = reply_to_message_id
    telegram_api("sendMessage", token, payload)


def read_offset() -> int:
    try:
        return int(OFFSET_PATH.read_text(encoding="utf-8").strip())
    except Exception:
        return 0


def write_offset(offset: int) -> None:
    OFFSET_PATH.parent.mkdir(parents=True, exist_ok=True)
    OFFSET_PATH.write_text(str(offset), encoding="utf-8")


def extract_message(update: Dict[str, object]) -> Tuple[Optional[Dict[str, object]], Optional[int]]:
    message = update.get("message")
    update_id = update.get("update_id")
    if not isinstance(message, dict):
        return None, None
    if not isinstance(update_id, int):
        return message, None
    return message, update_id


def message_text(message: Dict[str, object]) -> str:
    for key in ("text", "caption"):
        value = message.get(key)
        if isinstance(value, str):
            return value.strip()
    return ""


def chat_id_for(message: Dict[str, object]) -> str:
    chat = message.get("chat")
    if isinstance(chat, dict):
        value = chat.get("id")
        if value is not None:
            return str(value)
    return ""


def run_pipeline(text: str) -> str:
    env = os.environ.copy()
    env["SPRINGFIELD_PRICE_ALLOW_LOCAL_FILES"] = "0"
    proc = subprocess.run(
        ["/usr/bin/python3", str(PIPELINE), "--stdin", "--json", "--json-brief"],
        input=text,
        text=True,
        capture_output=True,
        cwd=str(ROOT),
        check=False,
        env=env,
    )
    payload = (proc.stdout or "").strip()
    if not payload:
        err = (proc.stderr or "").strip() or "pipeline returned empty output"
        return f"Error: {err}"
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return payload
    reply = str(data.get("reply_message") or "").strip()
    if reply:
        return reply
    error = str(data.get("error_message") or "").strip()
    if error:
        return f"Error: {error}"
    return str(data.get("summary") or "Price lookup finished.").strip()


def check_token(token: str) -> Dict[str, object]:
    return telegram_api("getMe", token)


def handle_start() -> str:
    return (
        "Send me a UK food price question or a public product URL.\n"
        "I can point you to the best UK price datasets for basket costs, inflation, and retailer comparisons, or extract a live price from a product page."
    )


def process_updates(token: str, updates: Iterable[Dict[str, object]], allowed_chat_ids: List[str]) -> int:
    max_update_id = 0
    for update in updates:
        message, update_id = extract_message(update)
        if update_id is not None:
            max_update_id = max(max_update_id, update_id)
        if not message:
            continue
        chat_id = chat_id_for(message)
        if allowed_chat_ids and chat_id not in allowed_chat_ids:
            continue
        text = message_text(message)
        if not text:
            send_message(
                token,
                chat_id,
                "Send a UK food price question, a public product URL, or pasted HTML.",
                message.get("message_id") if isinstance(message.get("message_id"), int) else None,
            )
            continue
        if text.startswith("/start"):
            reply = handle_start()
        else:
            reply = run_pipeline(text)
        reply_to = message.get("message_id") if isinstance(message.get("message_id"), int) else None
        send_message(token, chat_id, reply, reply_to)
    return max_update_id


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Springfield Price Bot Telegram polling")
    parser.add_argument("--check", action="store_true", help="Validate the Telegram bot token via getMe")
    parser.add_argument("--once", action="store_true", help="Poll once and exit")
    parser.add_argument("--poll-timeout", type=int, default=DEFAULT_POLL_TIMEOUT, help="Telegram long-poll timeout in seconds")
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    token = get_token()
    if not token:
        print("error: TELEGRAM_BOT_TOKEN is not configured in .env", file=sys.stderr)
        return 2

    if args.check:
        me = check_token(token)
        username = me.get("username") if isinstance(me, dict) else None
        bot_id = me.get("id") if isinstance(me, dict) else None
        print(json.dumps({"ok": True, "id": bot_id, "username": username}))
        return 0

    allowed_chat_ids = get_allowed_chat_ids()
    offset = read_offset()

    while True:
        try:
            updates = telegram_get_updates(token, offset, args.poll_timeout)
            max_update_id = process_updates(token, updates, allowed_chat_ids)
            if max_update_id:
                offset = max_update_id + 1
                write_offset(offset)
            if args.once:
                break
        except KeyboardInterrupt:
            return 130
        except Exception as exc:
            print(f"warning: {exc}", file=sys.stderr)
            time.sleep(3)
            if args.once:
                return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
