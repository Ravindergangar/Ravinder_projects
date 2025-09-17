from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Literal

import yaml


Direction = Literal["hubspot_to_d365", "d365_to_hubspot", "bidirectional"]
MergePolicy = Literal["overwrite", "fill_if_missing"]
ConflictPolicy = Literal["hubspot", "d365", "most_recent_wins"]


@dataclass
class FieldMapping:
    source_field: str
    target_field: str
    direction: Direction
    merge_policy: MergePolicy
    conflict_policy: ConflictPolicy = "most_recent_wins"


@dataclass
class ObjectMapping:
    name: str
    business_key: List[str]
    watermark_field_hubspot: str
    watermark_field_d365: str
    fields: List[FieldMapping]


@dataclass
class Settings:
    database: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    audit_schema: str
    hubspot_base_url: str
    d365_resource_url: str
    objects: List[str]


def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_mappings(path: str) -> Dict[str, ObjectMapping]:
    raw = load_yaml(path)
    result: Dict[str, ObjectMapping] = {}
    for obj_name, cfg in raw.get("objects", {}).items():
        fields = [
            FieldMapping(
                source_field=f["source_field"],
                target_field=f["target_field"],
                direction=f.get("direction", "bidirectional"),
                merge_policy=f.get("merge_policy", "overwrite"),
                conflict_policy=f.get("conflict_policy", "most_recent_wins"),
            )
            for f in cfg.get("fields", [])
        ]
        result[obj_name] = ObjectMapping(
            name=obj_name,
            business_key=cfg.get("business_key", []),
            watermark_field_hubspot=cfg.get("watermark_field_hubspot", "updatedAt"),
            watermark_field_d365=cfg.get("watermark_field_d365", "modifiedon"),
            fields=fields,
        )
    return result


def load_settings(path: str) -> Settings:
    raw = load_yaml(path)
    return Settings(
        database=raw["database"],
        bronze_schema=raw["bronze_schema"],
        silver_schema=raw["silver_schema"],
        gold_schema=raw["gold_schema"],
        audit_schema=raw["audit_schema"],
        hubspot_base_url=raw["hubspot_base_url"],
        d365_resource_url=raw["d365_resource_url"],
        objects=raw.get("objects", []),
    )

