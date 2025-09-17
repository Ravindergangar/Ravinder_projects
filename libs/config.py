import os
import json
from typing import Any, Dict, List, Optional

import yaml


def _read_dbfs_text(path: str, max_bytes: int = 5_000_000) -> str:
    """Read a small text file from DBFS using dbutils.fs.head or DBFS mount path.

    Supports paths like dbfs:/configs/file.yaml or /dbfs/configs/file.yaml.
    """
    if path.startswith("dbfs:/"):
        try:
            # Databricks only: dbutils is injected in notebooks. In jobs, use /dbfs path.
            from pyspark.dbutils import DBUtils  # type: ignore
            try:
                # Will fail if not in a notebook context
                dbutils = DBUtils(getspark())  # type: ignore # noqa: F821
            except Exception:
                dbfs_path = "/dbfs/" + path.replace("dbfs:/", "")
                with open(dbfs_path, "r") as f:
                    return f.read()
            raw = dbutils.fs.head(path, max_bytes)
            return raw
        except Exception:
            # Fallback to /dbfs
            dbfs_path = "/dbfs/" + path.replace("dbfs:/", "")
            with open(dbfs_path, "r") as f:
                return f.read()
    else:
        with open(path, "r") as f:
            return f.read()


def load_yaml_config(config_dir: Optional[str] = None) -> Dict[str, Any]:
    """Load sources.yaml and field_mappings.yaml from a directory.

    Resolution order for config_dir:
    - CONFIG_DIR environment variable if provided
    - provided config_dir argument
    - ../configs (relative to notebook path if in Databricks)
    - /Workspace/.../configs (for repos)
    - /workspace/configs (local dev)
    - dbfs:/configs (if present)
    """
    # Potential locations
    candidate_dirs: List[str] = []
    if os.environ.get("CONFIG_DIR"):
        candidate_dirs.append(os.environ["CONFIG_DIR"]) 
    if config_dir:
        candidate_dirs.append(config_dir)

    # Try DBFS configs dir by default
    candidate_dirs.append("dbfs:/configs")

    # Try relative to working dir
    candidate_dirs.append(os.path.abspath(os.path.join(os.getcwd(), "configs")))
    candidate_dirs.append("/workspace/configs")

    # Build from notebook path if available
    try:
        # Databricks-only: derive repo root
        import json as _json
        import pyspark  # noqa: F401
        # Accessing context may throw outside DBX
        from pyspark.sql import SparkSession  # noqa: F401
        # Attempt to use driver local path mirror for repos
        workspace_configs = "/Workspace/Repos/configs"
        candidate_dirs.append(workspace_configs)
    except Exception:
        pass

    # Try candidates in order
    sources: Optional[Dict[str, Any]] = None
    mappings: Optional[Dict[str, Any]] = None
    chosen_dir: Optional[str] = None
    for base in candidate_dirs:
        try:
            sources_raw = _read_dbfs_text(os.path.join(base, "sources.yaml"))
            mappings_raw = _read_dbfs_text(os.path.join(base, "field_mappings.yaml"))
            sources = yaml.safe_load(sources_raw) or {}
            mappings = yaml.safe_load(mappings_raw) or {}
            chosen_dir = base
            break
        except Exception:
            continue

    if sources is None or mappings is None:
        raise FileNotFoundError("Could not load sources.yaml and field_mappings.yaml from any known location. Set CONFIG_DIR.")

    return {
        "dir": chosen_dir,
        "sources": sources,
        "mappings": mappings,
    }


def _get_dbutils():
    """Best-effort access to dbutils in Databricks (notebooks or jobs)."""
    try:  # notebook global
        return dbutils  # type: ignore  # noqa: F821
    except Exception:
        try:
            from pyspark.dbutils import DBUtils  # type: ignore
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark is not None:
                return DBUtils(spark)
        except Exception:
            return None


def get_secret(name: str, default: Optional[str] = None, scope: Optional[str] = None) -> Optional[str]:
    """Retrieve a secret from Databricks secret scope if available, otherwise env.

    Resolution order:
    - If scope provided and dbutils available: return dbutils.secrets.get(scope, name)
    - Else: return environment variable NAME if set
    - Else: return default
    """
    if scope:
        dbu = _get_dbutils()
        if dbu is not None:
            try:
                return dbu.secrets.get(scope=scope, key=name)  # type: ignore[attr-defined]
            except Exception:
                pass
    return os.environ.get(name, default)


def as_spark_conf(conf: Dict[str, Any]) -> Dict[str, str]:
    """Serialize nested config into flat string map suitable for Spark configs if needed."""
    flat: Dict[str, str] = {}
    def _flatten(prefix: str, obj: Any) -> None:
        if isinstance(obj, dict):
            for k, v in obj.items():
                _flatten(f"{prefix}.{k}" if prefix else k, v)
        elif isinstance(obj, list):
            flat[prefix] = json.dumps(obj)
        else:
            flat[prefix] = str(obj)
    _flatten("", conf)
    return flat

