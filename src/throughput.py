from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import time
from dataclasses import dataclass, field

from aiohttp import web

logger = logging.getLogger(__name__)

STATIC_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static")


@dataclass
class StreamSession:
    session_id: str
    token: str
    queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=256))
    created_at: float = field(default_factory=time.time)


class ThroughputMonitor:
    def __init__(self, token_pool: dict):
        self.token_pool = token_pool
        self.sessions: dict[str, StreamSession] = {}
        self.token_sessions: dict[str, set[str]] = {}

    def is_active(self, token: str | None) -> bool:
        if not token:
            return False
        return bool(self.token_sessions.get(token))

    def create_session(self, token: str) -> str | None:
        entry = self.token_pool.get(token)
        if not entry:
            return None
        session_id = secrets.token_hex(16)
        session = StreamSession(session_id=session_id, token=token)
        self.sessions[session_id] = session
        self.token_sessions.setdefault(token, set()).add(session_id)
        logger.info(
            "throughput session created: %s... for token %s...",
            session_id[:8],
            token[:8],
        )
        return session_id

    def remove_session(self, session_id: str) -> None:
        session = self.sessions.pop(session_id, None)
        if session:
            sids = self.token_sessions.get(session.token)
            if sids:
                sids.discard(session_id)
                if not sids:
                    del self.token_sessions[session.token]
            logger.info("throughput session removed: %s...", session_id[:8])

    def broadcast(self, token: str, event_type: str, data: dict) -> None:
        sids = self.token_sessions.get(token)
        if not sids:
            return
        payload = (
            f"event: {event_type}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
        )
        encoded = payload.encode("utf-8")
        dead = []
        for sid in list(sids):
            session = self.sessions.get(sid)
            if not session:
                dead.append(sid)
                continue
            try:
                session.queue.put_nowait(encoded)
            except asyncio.QueueFull:
                dead.append(sid)
        for sid in dead:
            self.remove_session(sid)


def create_throughput_handlers(monitor: ThroughputMonitor, token_pool: dict):
    async def serve_throughput_page(request: web.Request) -> web.Response:
        html_path = os.path.join(STATIC_DIR, "throughput.html")
        if not os.path.exists(html_path):
            return web.json_response({"error": "throughput.html not found"}, status=500)
        with open(html_path, "r", encoding="utf-8") as f:
            return web.Response(text=f.read(), content_type="text/html")

    async def create_session_api(request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"error": "invalid JSON"}, status=400)
        token = body.get("token", "").strip()
        if not token:
            return web.json_response({"error": "missing token"}, status=400)
        session_id = monitor.create_session(token)
        if not session_id:
            return web.json_response({"error": "token not found in pool"}, status=404)
        return web.json_response({"session_id": session_id})

    async def sse_handler(request: web.Request) -> web.StreamResponse:
        sid = request.query.get("sid", "")
        session = monitor.sessions.get(sid)
        if not session:
            return web.json_response({"error": "invalid session"}, status=403)

        resp = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )
        await resp.prepare(request)

        try:
            await resp.write(b"event: connected\ndata: {}\n\n")
        except ConnectionError:
            monitor.remove_session(sid)
            return resp

        async def keepalive():
            while True:
                await asyncio.sleep(15)
                try:
                    await resp.write(b": keepalive\n\n")
                except ConnectionError:
                    break

        task = asyncio.ensure_future(keepalive())

        try:
            while True:
                if task.done():
                    break
                try:
                    chunk = await asyncio.wait_for(session.queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                try:
                    await resp.write(chunk)
                except ConnectionError:
                    break
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            monitor.remove_session(sid)

        return resp

    return serve_throughput_page, create_session_api, sse_handler
