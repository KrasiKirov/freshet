"""Connector framework: the ingestion-edge seam. A Connector turns one external
source's webhook deliveries into canonical Events. The webhook receiver is generic
over the REGISTRY — adding a source means writing a Connector and registering it."""

from __future__ import annotations

from typing import Mapping, Protocol, runtime_checkable

from freshet.common.schemas import Event


@runtime_checkable
class Connector(Protocol):
    source: str  # URL segment, e.g. "github" -> POST /webhook/github

    def event_type(self, headers: Mapping[str, str]) -> str: ...

    def verify(self, headers: Mapping[str, str], body: bytes) -> bool: ...

    def parse(self, event_type: str, payload: dict) -> list[Event]: ...


REGISTRY: dict[str, Connector] = {}


def register(connector: Connector) -> None:
    REGISTRY[connector.source] = connector
