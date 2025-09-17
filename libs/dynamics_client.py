from typing import Dict, List, Optional, Tuple
import time
import urllib.parse

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from msal import ConfidentialClientApplication


class DynamicsError(Exception):
    pass


class DynamicsClient:
    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        org_uri: str,
        api_version: str = "v9.2",
    ) -> None:
        self.org_uri = org_uri
        self.base_url = f"https://{org_uri}/api/data/{api_version}"
        self.app = ConfidentialClientApplication(
            client_id=client_id,
            client_credential=client_secret,
            authority=f"https://login.microsoftonline.com/{tenant_id}",
        )
        self.session = requests.Session()

    def _token(self) -> str:
        scope = [f"https://{self.org_uri}/.default"]
        res = self.app.acquire_token_silent(scopes=scope, account=None)
        if not res:
            res = self.app.acquire_token_for_client(scopes=scope)
        if "access_token" not in res:
            raise DynamicsError(f"MSAL token error: {res}")
        return res["access_token"]

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token()}",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type(DynamicsError),
    )
    def _get(self, path: str, params: Optional[Dict[str, str]] = None) -> Dict:
        url = f"{self.base_url}{path}"
        resp = self.session.get(url, headers=self._headers(), params=params, timeout=60)
        if resp.status_code in (429, 500, 502, 503, 504):
            retry_after = int(resp.headers.get("Retry-After", "0"))
            if retry_after > 0:
                time.sleep(retry_after)
            raise DynamicsError(f"HTTP {resp.status_code}: {resp.text}")
        if not resp.ok:
            raise DynamicsError(f"HTTP {resp.status_code}: {resp.text}")
        return resp.json() if resp.text else {}

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type(DynamicsError),
    )
    def _patch(self, path: str, json_body: Dict) -> None:
        url = f"{self.base_url}{path}"
        resp = self.session.patch(url, headers=self._headers(), json=json_body, timeout=60)
        if resp.status_code in (429, 500, 502, 503, 504):
            retry_after = int(resp.headers.get("Retry-After", "0"))
            if retry_after > 0:
                time.sleep(retry_after)
            raise DynamicsError(f"HTTP {resp.status_code}: {resp.text}")
        if resp.status_code not in (200, 204):
            raise DynamicsError(f"HTTP {resp.status_code}: {resp.text}")

    def list_contacts(
        self,
        select: List[str],
        modifiedon_gte_iso: Optional[str] = None,
        page_size: int = 500,
    ) -> Tuple[List[Dict], Optional[str]]:
        """Incrementally list contacts by modifiedon ge watermark.

        Returns: (records, max_modifiedon_iso)
        """
        entity = "/contacts"
        filter_clause = None
        if modifiedon_gte_iso:
            # OData ge timestamp
            filter_clause = f"modifiedon ge {modifiedon_gte_iso}"
        params: Dict[str, str] = {
            "$select": ",".join(select),
            "$orderby": "modifiedon asc",
            "$top": str(page_size),
        }
        if filter_clause:
            params["$filter"] = filter_clause

        url = f"{self.base_url}{entity}"
        results: List[Dict] = []
        max_mod: Optional[str] = None
        next_link: Optional[str] = None

        while True:
            if next_link:
                resp = self.session.get(next_link, headers=self._headers(), timeout=60)
                if not resp.ok:
                    raise DynamicsError(f"HTTP {resp.status_code}: {resp.text}")
                data = resp.json()
            else:
                data = self._get(entity, params)

            values = data.get("value", [])
            for r in values:
                lm = r.get("modifiedon")
                if lm and (max_mod is None or lm > max_mod):
                    max_mod = lm
            results.extend(values)
            next_link = data.get("@odata.nextLink")
            if not next_link:
                break

        return results, max_mod

    def patch_contact(self, contact_id: str, payload: Dict) -> None:
        entity_path = f"/contacts({contact_id})"
        self._patch(entity_path, payload)

    # OptionSet metadata: resolve labels <-> values
    def get_global_option_set(self, name: str) -> Dict[str, int]:
        # This endpoint returns metadata; user must ensure correct permissions
        path = "/GlobalOptionSetDefinitions(Name='{name}')".format(name=name)
        params = {"$select": "Name,Options"}
        data = self._get(path, params)
        options = data.get("Options", [])
        return {opt.get("Label", {}).get("LocalizedLabels", [{}])[0].get("Label"): opt.get("Value") for opt in options}

