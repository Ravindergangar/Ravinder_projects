from typing import Dict, Any

from libs.dynamics_client import DynamicsClient


def translate_display_to_backend(client: DynamicsClient, cfg: Dict[str, Any], properties: Dict[str, Any]) -> Dict[str, Any]:
    """Translate OptionSet labels and Lookup display values to backend values.

    - OptionSets: label -> numeric value using global option set name
    - Lookups: expected value is an OData bind like: fieldname@odata.bind = "/accounts(accountid)"

    This function assumes `cfg` contains `option_sets` and `lookups` mappings for contact fields.
    Unknown fields are passed through unchanged.
    """
    out: Dict[str, Any] = {}
    option_sets = (cfg.get("sources", {}).get("dynamics", {}).get("option_sets", {})) or {}
    lookups = (cfg.get("sources", {}).get("dynamics", {}).get("lookups", {})) or {}

    # Cache for option set name -> {label: value}
    cache: Dict[str, Dict[str, int]] = {}

    for field, value in properties.items():
        if value is None:
            out[field] = None
            continue
        if field in option_sets:
            os_name = option_sets[field]
            if os_name not in cache:
                cache[os_name] = client.get_global_option_set(os_name)
            mapping = cache[os_name]
            if isinstance(value, str):
                out[field] = mapping.get(value, value)
            else:
                out[field] = value
        elif field in lookups:
            # Expecting display to be exact entity id or a pre-resolved GUID
            entity = lookups[field]
            # If user provided a GUID, bind to it; else pass through (requires enrichment upstream)
            if isinstance(value, str) and len(value) >= 32:
                out[f"{field}@odata.bind"] = f"/{entity}({value})"
            else:
                out[field] = value
        else:
            out[field] = value

    return out

