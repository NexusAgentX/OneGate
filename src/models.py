from __future__ import annotations

import json
import time
from urllib.parse import urlparse


class Provider:
    def __init__(
        self,
        name: str,
        prefix: str,
        upstream: str,
        strip_prefix: bool,
        env_token: str = "",
    ):
        self.name = name
        self.prefix = prefix
        self.upstream = upstream.rstrip("/")
        self.strip_prefix = strip_prefix
        self.env_token = env_token or f"TOKEN_{name.upper()}"
        parsed = urlparse(self.upstream)
        self.host: str = parsed.hostname or ""
        self.origin: str = f"{parsed.scheme}://{parsed.netloc}"


class TokenEntry:
    def __init__(
        self,
        token: str,
        providers: list[str] | None = None,
        is_admin: bool = False,
        enabled: bool = True,
        created_at: int | None = None,
    ):
        self.token = token
        self.providers: list[str] = providers if providers is not None else ["*"]
        self.is_admin = is_admin
        self.enabled = enabled
        self.created_at = created_at or int(time.time())

    def has_permission(self, provider: Provider) -> bool:
        if not self.enabled:
            return False
        if "*" in self.providers:
            return True
        return provider.name in self.providers

    def to_db_row(self) -> tuple:
        return (
            self.token,
            json.dumps(self.providers),
            1 if self.is_admin else 0,
            1 if self.enabled else 0,
            self.created_at,
        )

    @staticmethod
    def from_db_row(row: tuple) -> TokenEntry:
        return TokenEntry(
            token=row[0],
            providers=json.loads(row[1]),
            is_admin=bool(row[2]),
            enabled=bool(row[3]),
            created_at=row[4],
        )

    def to_dict(self) -> dict:
        return {
            "token": self.token,
            "providers": self.providers,
            "is_admin": self.is_admin,
            "enabled": self.enabled,
            "created_at": self.created_at,
        }
