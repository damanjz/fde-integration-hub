"""Destination registry: settings -> gate configs and clients. Add a destination here (see runbook)."""

from __future__ import annotations

import httpx
from pydantic import BaseModel

from hub.auth import ApiKeyAuth, OAuthClientCredentials
from hub.client import DestinationClient, DestinationSpec
from hub.config import ApiKeyDestination, OAuthDestination, Settings
from hub.gate import Gate, GateConfig
from hub.mapping import MAPPERS


def destination_settings(settings: Settings) -> dict[str, BaseModel]:
    return {"warehouse": settings.warehouse, "helpdesk": settings.helpdesk, "accounting": settings.accounting}


def destination_names(settings: Settings) -> list[str]:
    return sorted(set(destination_settings(settings)) & set(MAPPERS))


def gate_configs(settings: Settings) -> dict[str, GateConfig]:
    return {
        name: GateConfig(
            rate=cfg.rate,
            burst=cfg.burst,
            breaker_threshold=settings.breaker_threshold,
            breaker_base_s=settings.breaker_base_s,
            breaker_cap_s=settings.breaker_cap_s,
        )
        for name, cfg in destination_settings(settings).items()
    }


def build_clients(settings: Settings, gate: Gate, http: httpx.AsyncClient) -> dict[str, DestinationClient]:
    clients = {}
    for name, cfg in destination_settings(settings).items():
        if isinstance(cfg, OAuthDestination):
            auth = OAuthClientCredentials(
                http,
                cfg.base_url + cfg.token_path,
                cfg.client_id,
                cfg.client_secret.get_secret_value(),
                timeout_s=cfg.timeout_s,
            )
        elif isinstance(cfg, ApiKeyDestination):
            auth = ApiKeyAuth(cfg.api_key_header, cfg.api_key.get_secret_value())
        else:
            raise TypeError(f"unsupported destination config for {name}")
        spec = DestinationSpec(
            name,
            cfg.base_url,
            cfg.send_path,
            cfg.lookup_path,
            cfg.timeout_s,
            auth,
            lookup_param=cfg.lookup_param,
            idempotency_header=cfg.idempotency_header,
            duplicate_statuses=tuple(cfg.duplicate_statuses),
            duplicate_error=cfg.duplicate_error,
            duplicate_number_field=cfg.duplicate_number_field,
        )
        clients[name] = DestinationClient(
            spec,
            http,
            gate,
            max_inline_wait_s=settings.max_inline_wait_s,
            max_retry_after_s=settings.max_retry_after_s,
        )
    return clients
