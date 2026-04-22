from __future__ import annotations

import json
import threading
from collections.abc import Iterator, MutableMapping
from pathlib import Path
from typing import Any


class JsonlKeyedStore(MutableMapping[str, dict[str, Any]]):
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._data: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()
        if self.path.exists():
            for line in self.path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                record = json.loads(line)
                publish_key = record["publish_key"]
                self._data[publish_key] = record

    def __getitem__(self, key: str) -> dict[str, Any]:
        return self._data[key]

    def __setitem__(self, key: str, value: dict[str, Any]) -> None:
        with self._lock:
            self._data[key] = {"publish_key": key, **value}
            self.flush()

    def __delitem__(self, key: str) -> None:
        with self._lock:
            del self._data[key]
            self.flush()

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def flush(self) -> None:
        payload = "\n".join(
            json.dumps(self._data[key], ensure_ascii=False, sort_keys=True)
            for key in sorted(self._data)
        )
        if payload:
            payload += "\n"
        self.path.write_text(payload, encoding="utf-8")
