import hashlib
from typing import Dict, List, Optional


def normalize_email(email: Optional[str]) -> Optional[str]:
    if not email:
        return None
    e = email.strip().lower()
    return e if "@" in e else None


def checksum_row(row: Dict, fields: List[str]) -> str:
    parts = [str(row.get(f) or "") for f in fields]
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

