from __future__ import annotations

import os

from aiohttp import web

from src.config import AppConfig
from src.db import (
    add_token,
    delete_token,
    get_all_tokens,
    is_admin_token,
    update_token,
)
from src.models import TokenEntry
from src.pool import make_token

STATIC_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static")


def _get_admin_token(request: web.Request) -> str:
    return request.headers.get("X-Admin-Token", "")


def create_admin_handlers(
    cfg: AppConfig,
    db_conn,
    token_pool: dict[str, TokenEntry],
):
    async def serve_admin_page(request: web.Request) -> web.Response:
        html_path = os.path.join(STATIC_DIR, "admin.html")
        if not os.path.exists(html_path):
            return web.json_response({"error": "admin.html not found"}, status=500)

        providers = [p.name for p in cfg.providers]
        with open(html_path, "r", encoding="utf-8") as f:
            html = f.read()
        html = html.replace("/*__PROVIDERS__*/", repr(providers))
        return web.Response(text=html, content_type="text/html")

    async def api_list_tokens(request: web.Request) -> web.Response:
        admin_token = _get_admin_token(request)
        if not admin_token or not is_admin_token(db_conn, admin_token):
            return web.json_response({"error": "unauthorized"}, status=403)

        tokens = get_all_tokens(db_conn)
        return web.json_response({"tokens": [t.to_dict() for t in tokens]})

    async def api_create_token(request: web.Request) -> web.Response:
        admin_token = _get_admin_token(request)
        if not admin_token or not is_admin_token(db_conn, admin_token):
            return web.json_response({"error": "unauthorized"}, status=403)

        try:
            body = await request.json()
        except Exception:
            return web.json_response({"error": "invalid JSON"}, status=400)

        providers = body.get("providers", ["*"])
        is_admin = body.get("is_admin", False)
        custom_token = body.get("token")

        token = custom_token if custom_token else make_token()
        entry = add_token(db_conn, token, providers, is_admin)
        token_pool[entry.token] = entry
        return web.json_response({"token": entry.to_dict()})

    async def api_update_token(request: web.Request) -> web.Response:
        admin_token = _get_admin_token(request)
        if not admin_token or not is_admin_token(db_conn, admin_token):
            return web.json_response({"error": "unauthorized"}, status=403)

        try:
            body = await request.json()
        except Exception:
            return web.json_response({"error": "invalid JSON"}, status=400)

        target = body.get("token", "")
        if not target:
            return web.json_response({"error": "missing token"}, status=400)

        providers = body.get("providers")
        is_admin = body.get("is_admin")
        enabled = body.get("enabled")

        if is_admin is False and target == admin_token:
            return web.json_response(
                {"error": "cannot remove your own admin privilege"}, status=400
            )
        if enabled is False and target == admin_token:
            return web.json_response(
                {"error": "cannot disable your own token"}, status=400
            )

        entry = update_token(
            db_conn, target, providers=providers, is_admin=is_admin, enabled=enabled
        )
        if not entry:
            return web.json_response({"error": "token not found"}, status=404)

        token_pool[entry.token] = entry
        return web.json_response({"token": entry.to_dict()})

    async def api_delete_token(request: web.Request) -> web.Response:
        admin_token = _get_admin_token(request)
        if not admin_token or not is_admin_token(db_conn, admin_token):
            return web.json_response({"error": "unauthorized"}, status=403)

        try:
            body = await request.json()
        except Exception:
            return web.json_response({"error": "invalid JSON"}, status=400)

        target = body.get("token", "")
        if not target:
            return web.json_response({"error": "missing token"}, status=400)

        if target == admin_token:
            return web.json_response(
                {"error": "cannot delete your own token"}, status=400
            )

        if not delete_token(db_conn, target):
            return web.json_response({"error": "token not found"}, status=404)

        token_pool.pop(target, None)
        return web.json_response({"ok": True})

    return (
        serve_admin_page,
        api_list_tokens,
        api_create_token,
        api_update_token,
        api_delete_token,
    )
