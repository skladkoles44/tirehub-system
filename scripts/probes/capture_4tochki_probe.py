from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lxml import etree
from zeep import Client
from zeep.helpers import serialize_object
from zeep.plugins import HistoryPlugin
from zeep.transports import Transport

from src.integrations.fourtochki.models import (
    DiskSearchFilter,
    GoodsPriceRestFilter,
    TyreSearchFilter,
)


DEFAULT_WSDL_URL = "https://api-b2b.4tochki.ru/WCF/ClientService.svc?wsdl"
DEFAULT_ADDRESS_ID = 64302
DEFAULT_CODE = "4405300"


def load_env_file(path: str) -> None:
    p = Path(path)
    if not p.exists():
        return

    for raw_line in p.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(
            key.strip(),
            value.strip().strip('"').strip("'"),
        )


def env_first(*names: str, default: str | None = None) -> str:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    if default is not None:
        return default
    raise RuntimeError("Missing env key, accepted aliases: " + ", ".join(names))


def jsonable(value: Any) -> Any:
    return serialize_object(value, target_cls=dict)


def sha256_json(value: Any) -> str:
    blob = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def xml_to_text(node: Any) -> str | None:
    if node is None:
        return None
    return etree.tostring(node, pretty_print=True, encoding="unicode")


def redact(text: str | None, login: str, password: str) -> str | None:
    if text is None:
        return None

    out = text
    if login:
        out = out.replace(login, "***REDACTED_LOGIN***")
    if password:
        out = out.replace(password, "***REDACTED_PASSWORD***")

    out = re.sub(
        r"<login>.*?</login>",
        "<login>***REDACTED_LOGIN***</login>",
        out,
        flags=re.I | re.S,
    )
    out = re.sub(
        r"<password>.*?</password>",
        "<password>***REDACTED_PASSWORD***</password>",
        out,
        flags=re.I | re.S,
    )
    return out


def find_first_list_by_key(value: Any, key: str) -> list[Any]:
    if isinstance(value, dict):
        direct = value.get(key)
        if isinstance(direct, list):
            return direct
        for nested in value.values():
            found = find_first_list_by_key(nested, key)
            if found:
                return found

    if isinstance(value, list):
        for item in value:
            found = find_first_list_by_key(item, key)
            if found:
                return found

    return []


def response_summary(response: Any) -> dict[str, Any]:
    data = jsonable(response)

    price_rest = find_first_list_by_key(data, "price_rest")
    wh_price_rest = find_first_list_by_key(data, "wh_price_rest")
    logistics = find_first_list_by_key(data, "WarehouseLogistic")

    top_level_keys = None
    if isinstance(data, dict):
        top_level_keys = sorted(data.keys())

    return {
        "response_type": type(response).__name__,
        "response_sha256": sha256_json(data),
        "top_level_keys": top_level_keys,
        "price_rest_count": len(price_rest),
        "wh_price_rest_count": len(wh_price_rest),
        "warehouse_logistics_count": len(logistics),
        "sample_price_rest": price_rest[:3],
        "sample_wh_price_rest": wh_price_rest[:5],
        "sample_warehouse_logistics": logistics[:5],
    }


def build_client(
    wsdl_url: str,
    timeout: int,
    soap_address_override: str | None,
) -> tuple[Client, HistoryPlugin]:
    history = HistoryPlugin()
    client = Client(
        wsdl=wsdl_url,
        transport=Transport(timeout=timeout),
        plugins=[history],
    )
    if soap_address_override:
        client.service._binding_options["address"] = soap_address_override
    return client, history


def call_probe(
    client: Client,
    mode: str,
    login: str,
    password: str,
    address_id: int,
) -> tuple[str, dict[str, Any], Any, str]:
    if mode == "ping":
        method = "Ping"
        payload = {}
        response = client.service.Ping(login, password)
        return method, payload, response, "EXACT"

    if mode == "goods-exact":
        method = "GetGoodsPriceRestByCode"
        payload = GoodsPriceRestFilter(
            codes=[DEFAULT_CODE],
            warehouse_ids=[],
            include_paid_delivery=False,
            search_by_occurrence=False,
        ).to_payload(address_id)
        response = client.service.GetGoodsPriceRestByCode(
            login,
            password,
            payload,
        )
        return method, payload, response, "EXACT"

    if mode == "goods-wrh-232":
        method = "GetGoodsPriceRestByCode"
        payload = GoodsPriceRestFilter(
            codes=[DEFAULT_CODE],
            warehouse_ids=[232],
            include_paid_delivery=False,
            search_by_occurrence=False,
        ).to_payload(address_id)
        response = client.service.GetGoodsPriceRestByCode(
            login,
            password,
            payload,
        )
        return method, payload, response, "EXACT"

    if mode == "goods-occurrence":
        method = "GetGoodsPriceRestByCode"
        payload = GoodsPriceRestFilter(
            codes=[DEFAULT_CODE],
            warehouse_ids=[],
            include_paid_delivery=False,
            search_by_occurrence=True,
        ).to_payload(address_id)
        response = client.service.GetGoodsPriceRestByCode(
            login,
            password,
            payload,
        )
        return method, payload, response, "EXACT"

    if mode == "tyre":
        method = "GetFindTyre"
        filt = TyreSearchFilter(
            width_min=185,
            width_max=185,
            height_min=60,
            height_max=60,
            diameter_min=15,
            diameter_max=15,
            season_list=["w"],
            warehouse_ids=[],
            include_paid_delivery=False,
            page=0,
            page_size=50,
        )
        payload = filt.to_payload(address_id)
        response = client.service.GetFindTyre(
            login,
            password,
            payload,
            filt.page,
            filt.page_size,
        )
        return method, payload, response, "EXACT"

    if mode == "disk-candidate":
        method = "GetFindDisk"
        filt = DiskSearchFilter(
            width_min=6.5,
            width_max=6.5,
            diameter_min=16,
            diameter_max=16,
            bolts_count_min=5,
            bolts_count_max=5,
            bolts_spacing_min=114.3,
            bolts_spacing_max=114.3,
            warehouse_ids=[],
            include_paid_delivery=False,
            page=0,
            page_size=50,
        )
        payload = filt.to_payload(address_id)
        response = client.service.GetFindDisk(
            login,
            password,
            payload,
            filt.page,
            filt.page_size,
        )
        return method, payload, response, "EXACT"

    raise ValueError(f"Unsupported mode: {mode}")


def write_artifact(
    output_dir: Path,
    mode: str,
    method: str,
    status_before: str,
    address_id: int,
    request_payload: dict[str, Any],
    response: Any,
    history: HistoryPlugin,
    login: str,
    password: str,
) -> Path:
    created_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir.mkdir(parents=True, exist_ok=True)

    sent = None
    received = None

    if history.last_sent:
        sent = history.last_sent.get("envelope")
    if history.last_received:
        received = history.last_received.get("envelope")

    artifact = {
        "artifact_schema_version": 1,
        "created_at_utc": created_at,
        "supplier": "4tochki",
        "mode": mode,
        "method": method,
        "status_before": status_before,
        "address_id": address_id,
        "request_payload": request_payload,
        "request_payload_sha256": sha256_json(request_payload),
        "response_summary": response_summary(response),
        "response_full": jsonable(response),
        "soap_last_sent_envelope_redacted": redact(
            xml_to_text(sent),
            login,
            password,
        ),
        "soap_last_received_envelope_redacted": redact(
            xml_to_text(received),
            login,
            password,
        ),
    }

    path = output_dir / f"{created_at}_{mode}.json"
    path.write_text(
        json.dumps(artifact, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return path


def main() -> int:
    load_env_file(".env.4tochki")
    load_env_file(".env")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=[
            "ping",
            "goods-exact",
            "goods-wrh-232",
            "goods-occurrence",
            "tyre",
            "disk-candidate",
            "all",
        ],
        required=True,
    )
    parser.add_argument("--address-id", type=int, default=DEFAULT_ADDRESS_ID)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--output-dir", default="var/probes/4tochki")

    args = parser.parse_args()

    login = env_first("FOURTOCHKI_LOGIN", "FTO_LOGIN")
    password = env_first("FOURTOCHKI_PASSWORD", "FTO_PASSWORD")
    wsdl_url = env_first(
        "FOURTOCHKI_WSDL_URL",
        "FTO_WSDL_URL",
        default=DEFAULT_WSDL_URL,
    )
    soap_address_override = (
        os.environ.get("FOURTOCHKI_SOAP_ADDRESS_OVERRIDE")
        or os.environ.get("FTO_SOAP_ADDRESS_OVERRIDE")
    )

    if args.mode == "all":
        modes = [
            "ping",
            "goods-exact",
            "goods-wrh-232",
            "goods-occurrence",
            "tyre",
            "disk-candidate",
        ]
    else:
        modes = [args.mode]

    created: list[str] = []

    for mode in modes:
        client, history = build_client(
            wsdl_url=wsdl_url,
            timeout=args.timeout,
            soap_address_override=soap_address_override,
        )
        method, payload, response, status_before = call_probe(
            client=client,
            mode=mode,
            login=login,
            password=password,
            address_id=args.address_id,
        )
        path = write_artifact(
            output_dir=Path(args.output_dir),
            mode=mode,
            method=method,
            status_before=status_before,
            address_id=args.address_id,
            request_payload=payload,
            response=response,
            history=history,
            login=login,
            password=password,
        )
        created.append(str(path))

    print(json.dumps({"created": created}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
