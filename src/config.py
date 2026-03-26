from __future__ import annotations

import os
from dataclasses import dataclass, field

import toml

from src.models import Provider

CONFIG_FILE = "config.toml"


@dataclass
class AppConfig:
    bind_port: int = 5678
    intercept_port: int = 0
    enable_log: bool = False
    pool_count: int = 5
    pool_file: str = "pool.json"
    usage_db: str = "data/onegate.db"
    admin_tokens: list[str] = field(default_factory=list)
    providers: list[Provider] = field(default_factory=list)
    real_tokens: dict[str, str] = field(default_factory=dict)


def load_config(config_dir: str | None = None) -> AppConfig:
    cfg = AppConfig()

    cfg_path = os.path.join(config_dir, CONFIG_FILE) if config_dir else CONFIG_FILE

    if not os.path.exists(cfg_path):
        cfg.bind_port = int(os.environ.get("BIND_PORT", "5678"))
        cfg.intercept_port = int(os.environ.get("INTERCEPT_PORT", "0"))
        cfg.enable_log = os.environ.get("ENABLE_LOG", "") == "1"
        env_pool_count = os.environ.get("POOL_COUNT")
        cfg.pool_count = int(env_pool_count) if env_pool_count else 5
        cfg.pool_file = os.environ.get("POOL_FILE", "pool.json")
        cfg.usage_db = os.environ.get("USAGE_DB", "data/onegate.db")
        real = os.environ.get("REAL_TOKEN", "")
        if real:
            cfg.real_tokens["default"] = real
        return cfg

    with open(cfg_path, "r", encoding="utf-8") as f:
        data = toml.load(f)

    cfg.bind_port = data.get("bind_port", 5678)
    cfg.intercept_port = data.get("intercept_port", 0)
    cfg.enable_log = data.get("enable_log", False)

    pool_cfg = data.get("pool", {})
    env_pool_count = os.environ.get("POOL_COUNT")
    cfg_pool_count = pool_cfg.get("count", 5)
    cfg.pool_count = int(env_pool_count) if env_pool_count else int(cfg_pool_count)
    cfg.pool_file = pool_cfg.get("file", "pool.json")
    cfg.usage_db = pool_cfg.get("usage_db", "data/onegate.db")
    cfg.admin_tokens = pool_cfg.get("admin_tokens", [])

    providers_cfg = data.get("providers", {})
    for name, pcfg in providers_cfg.items():
        p = Provider(
            name=name,
            prefix=pcfg.get("prefix", "/"),
            upstream=pcfg.get("upstream", ""),
            strip_prefix=pcfg.get("strip_prefix", False),
            env_token=pcfg.get("env_token", f"TOKEN_{name.upper()}"),
        )
        cfg.providers.append(p)
    cfg.providers.sort(key=lambda x: len(x.prefix), reverse=True)

    for p in cfg.providers:
        val = os.environ.get(p.env_token, "")
        if val:
            cfg.real_tokens[p.name] = val

    return cfg


def match_provider(providers: list[Provider], path: str) -> Provider | None:
    best: Provider | None = None
    for p in providers:
        if p.prefix == "/":
            if best is None:
                best = p
            continue
        prefix = p.prefix.rstrip("/")
        if path == prefix or path.startswith(prefix + "/"):
            if best is None or len(p.prefix) > len(best.prefix):
                best = p
    return best
