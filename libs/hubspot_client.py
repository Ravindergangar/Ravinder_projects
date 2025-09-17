from typing import Dict, Iterable, List, Optional, Tuple
import time

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


class HubSpotError(Exception):
    pass


class HubSpotClient:
    def __init__(self, token: str, base_url: str = "https://api.hubapi.com") -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
        )

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type(HubSpotError),
    )
    def _post(self, path: str, json_body: Dict) -> Dict:
        url = f"{self.base_url}{path}"
        resp = self.session.post(url, json=json_body, timeout=60)
        if resp.status_code in (429, 500, 502, 503, 504):
            # Respect rate limit if present
            retry_after = int(resp.headers.get("Retry-After", "0"))
            if retry_after > 0:
                time.sleep(retry_after)
            raise HubSpotError(f"HTTP {resp.status_code}: {resp.text}")
        if not resp.ok:
            raise HubSpotError(f"HTTP {resp.status_code}: {resp.text}")
        return resp.json() if resp.text else {}

    def search_contacts(
        self,
        properties: List[str],
        last_modified_gte_iso: Optional[str] = None,
        page_size: int = 100,
    ) -> Tuple[List[Dict], Optional[str]]:
        """Search contacts incrementally by hs_lastmodifieddate >= watermark.

        Returns: (records, max_hs_lastmodifieddate_iso)
        """
        path = "/crm/v3/objects/contacts/search"
        has_more = True
        after: Optional[str] = None
        results: List[Dict] = []
        max_lastmod: Optional[str] = None

        filter_groups: List[Dict] = []
        if last_modified_gte_iso:
            filter_groups = [
                {
                    "filters": [
                        {
                            "propertyName": "hs_lastmodifieddate",
                            "operator": "GTE",
                            "value": last_modified_gte_iso,
                        }
                    ]
                }
            ]

        while has_more:
            body: Dict = {
                "properties": properties,
                "limit": page_size,
                "sorts": [
                    {
                        "propertyName": "hs_lastmodifieddate",
                        "direction": "ASC",
                    }
                ],
            }
            if filter_groups:
                body["filterGroups"] = filter_groups
            if after:
                body["after"] = after
            data = self._post(path, body)
            recs = data.get("results", [])
            for r in recs:
                props = r.get("properties", {})
                lm = props.get("hs_lastmodifieddate")
                if lm and (max_lastmod is None or lm > max_lastmod):
                    max_lastmod = lm
            results.extend(recs)
            paging = data.get("paging", {})
            next_info = paging.get("next") if paging else None
            after = next_info.get("after") if next_info else None
            has_more = after is not None and len(recs) > 0

        return results, max_lastmod

    def batch_update_contacts(self, inputs: List[Dict]) -> Dict:
        """Batch update contacts. inputs: [{"id": "123", "properties": {...}}]
        """
        path = "/crm/v3/objects/contacts/batch/update"
        body = {"inputs": inputs}
        return self._post(path, body)

