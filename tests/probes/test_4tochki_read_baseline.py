from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from src.integrations.fourtochki.client import FourTochkiReadClient
from src.integrations.fourtochki.models import (
    AddressListFilter,
    DiskSearchFilter,
    FourTochkiCredentials,
    FourTochkiSettings,
    TyreSearchFilter,
)


DEFAULT_WSDL_URL = "https://api-b2b.4tochki.ru/WCF/ClientService.svc?wsdl"
PROBE_INPUTS = json.loads(Path("tests/probes/probe_inputs.json").read_text(encoding="utf-8"))


def load_env_file(path: str) -> None:
    p = Path(path)
    if not p.exists():
        return

    for raw_line in p.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def env_first(*names: str, default: str | None = None) -> str:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    if default is not None:
        return default
    raise RuntimeError("Missing env key, accepted aliases: " + ", ".join(names))


def client() -> FourTochkiReadClient:
    load_env_file(".env.4tochki")
    load_env_file(".env")

    return FourTochkiReadClient(
        credentials=FourTochkiCredentials(
            login=env_first("FOURTOCHKI_LOGIN", "FTO_LOGIN"),
            password=env_first("FOURTOCHKI_PASSWORD", "FTO_PASSWORD"),
        ),
        settings=FourTochkiSettings(
            wsdl_url=env_first(
                "FOURTOCHKI_WSDL_URL",
                "FTO_WSDL_URL",
                default=DEFAULT_WSDL_URL,
            ),
            soap_address_override=(
                os.environ.get("FOURTOCHKI_SOAP_ADDRESS_OVERRIDE")
                or os.environ.get("FTO_SOAP_ADDRESS_OVERRIDE")
            ),
            baseline_address_id=PROBE_INPUTS["account_baseline"]["default_address_id"],
        ),
    )


@pytest.mark.probe
@pytest.mark.exact
def test_ping_returns_true() -> None:
    assert client().ping() is True


@pytest.mark.probe
@pytest.mark.exact
def test_get_default_address_id_matches_frozen_baseline() -> None:
    assert client().get_default_address_id() == 64302


@pytest.mark.probe
@pytest.mark.exact
def test_get_address_list_accepts_null_address_list() -> None:
    rows = client().get_address_list(AddressListFilter(address_id_list=None))
    assert rows


@pytest.mark.probe
@pytest.mark.exact
def test_get_address_list_accepts_empty_address_list() -> None:
    rows = client().get_address_list(AddressListFilter(address_id_list=[]))
    assert rows


@pytest.mark.probe
@pytest.mark.exact
def test_get_goods_price_rest_by_code_all_warehouses_exact() -> None:
    f = PROBE_INPUTS["goods_price_rest"]["all_warehouses_exact"]

    rows = client().get_goods_price_rest_by_code(
        address_id=f["user_address_id"],
        codes=f["code_list"],
        warehouse_ids=f["wrh_list"],
        include_paid_delivery=f["include_paid_delivery"],
        search_by_occurrence=f["searchCodeByOccurence"],
    )

    wh_rows = [wh for product in rows for wh in product.warehouse_rows]

    assert rows
    assert wh_rows
    assert all(wh.warehouse_id is not None for wh in wh_rows)
    assert all(wh.rest is not None for wh in wh_rows)
    assert all(wh.price_in is not None for wh in wh_rows)
    # wrh_list=[] returns a live supplier-dependent warehouse set.
    # Exact warehouse values are pinned by single_warehouse_exact.


@pytest.mark.probe
@pytest.mark.exact
def test_get_goods_price_rest_by_code_single_warehouse_exact() -> None:
    f = PROBE_INPUTS["goods_price_rest"]["single_warehouse_exact"]

    rows = client().get_goods_price_rest_by_code(
        address_id=f["user_address_id"],
        codes=f["code_list"],
        warehouse_ids=f["wrh_list"],
        include_paid_delivery=f["include_paid_delivery"],
        search_by_occurrence=f["searchCodeByOccurence"],
    )

    wh_rows = [wh for product in rows for wh in product.warehouse_rows]

    assert len(wh_rows) == 1
    assert wh_rows[0].warehouse_id == 232
    assert wh_rows[0].price_in == 4212
    assert wh_rows[0].price_retail_raw == 5190
    assert wh_rows[0].rest == 41


@pytest.mark.probe
@pytest.mark.exact
def test_get_goods_price_rest_by_code_occurrence_flag_exact_code() -> None:
    f = PROBE_INPUTS["goods_price_rest"]["all_warehouses_occurrence"]

    rows = client().get_goods_price_rest_by_code(
        address_id=f["user_address_id"],
        codes=f["code_list"],
        warehouse_ids=f["wrh_list"],
        include_paid_delivery=f["include_paid_delivery"],
        search_by_occurrence=f["searchCodeByOccurence"],
    )

    wh_rows = [wh for product in rows for wh in product.warehouse_rows]

    assert rows
    assert wh_rows
    assert all(wh.warehouse_id is not None for wh in wh_rows)
    assert all(wh.rest is not None for wh in wh_rows)
    assert all(wh.price_in is not None for wh in wh_rows)
    # wrh_list=[] returns a live supplier-dependent warehouse set.
    # Exact warehouse values are pinned by single_warehouse_exact.


@pytest.mark.probe
@pytest.mark.exact
def test_find_tyre_known_good_query_exact() -> None:
    q = PROBE_INPUTS["find_tyre"]["known_good_query"]

    rows = client().find_tyre(
        address_id=q["user_address_id"],
        filt=TyreSearchFilter(
            width_min=q["width_min"],
            width_max=q["width_max"],
            height_min=q["height_min"],
            height_max=q["height_max"],
            diameter_min=q["diameter_min"],
            diameter_max=q["diameter_max"],
            season_list=q["season_list"],
            warehouse_ids=q["wrh_list"],
            include_paid_delivery=q["include_paid_delivery"],
            page=PROBE_INPUTS["find_tyre"]["page"],
            page_size=PROBE_INPUTS["find_tyre"]["page_size"],
        ),
    )

    wh_rows = [wh for product in rows for wh in product.warehouse_rows]
    joined = [wh for wh in wh_rows if wh.logistic_days is not None]

    assert rows
    assert len(wh_rows) >= 1
    assert joined
    assert all(wh.warehouse_id is not None for wh in joined)
    assert all(wh.logistic_days is not None for wh in joined)


@pytest.mark.probe
@pytest.mark.exact
def test_find_disk_known_good_query_exact() -> None:
    q = PROBE_INPUTS["find_disk"]["known_good_query"]

    rows = client().find_disk(
        address_id=q["user_address_id"],
        filt=DiskSearchFilter(
            width_min=q["width_min"],
            width_max=q["width_max"],
            diameter_min=q["diameter_min"],
            diameter_max=q["diameter_max"],
            bolts_count_min=q["bolts_count_min"],
            bolts_count_max=q["bolts_count_max"],
            bolts_spacing_min=q["bolts_spacing_min"],
            bolts_spacing_max=q["bolts_spacing_max"],
            warehouse_ids=q["wrh_list"],
            include_paid_delivery=q["include_paid_delivery"],
            page=PROBE_INPUTS["find_disk"]["page"],
            page_size=PROBE_INPUTS["find_disk"]["page_size"],
        ),
    )

    wh_rows = [wh for product in rows for wh in product.warehouse_rows]
    joined = [wh for wh in wh_rows if wh.logistic_days is not None]

    assert rows
    assert len(wh_rows) >= 1
    assert joined
    assert all(wh.warehouse_id is not None for wh in joined)
    assert all(wh.logistic_days is not None for wh in joined)
