import json
import os
import uuid
from dataclasses import dataclass, field

from .config import APP_DIR

SESSIONS_DIR = os.path.join(APP_DIR, "sessions")

@dataclass
class SessionState:
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    original_requests: list[str] = field(default_factory=list)
    failed_track_ids: set[str] = field(default_factory=set)

    @property
    def path(self):
        return os.path.join(SESSIONS_DIR, f"{self.id}.json")

    def add_request(self, source, media_type, item_id):
        req = f"{source} {media_type} {item_id}"
        if req not in self.original_requests:
            self.original_requests.append(req)

    def add_failed_track(self, track_id: str):
        self.failed_track_ids.add(track_id)

    def save(self):
        os.makedirs(SESSIONS_DIR, exist_ok=True)
        with open(self.path, "w") as f:
            json.dump({
                "id": self.id,
                "original_requests": self.original_requests,
                "failed_track_ids": list(self.failed_track_ids)
            }, f)

    def clear(self):
        if os.path.exists(self.path):
            try:
                os.remove(self.path)
            except OSError:
                pass

    @classmethod
    def load(cls, session_id: str) -> "SessionState":
        path = os.path.join(SESSIONS_DIR, f"{session_id}.json")
        if not os.path.exists(path):
            raise FileNotFoundError(f"Session {session_id} not found.")
        with open(path, "r") as f:
            data = json.load(f)
        return cls(
            id=data["id"],
            original_requests=data.get("original_requests", []),
            failed_track_ids=set(data.get("failed_track_ids", []))
        )
