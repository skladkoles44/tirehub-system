from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


def _arr_str(values: list[str] | None) -> dict[str, list[str]] | None:
    if values is None:
        return None
    return {"string": values}


def _arr_int(values: list[int] | None) -> dict[str, list[int]] | None:
    if values is None:
        return None
    return {"int": values}


@dataclass(frozen=True)
class FourTochkiCredentials:
    login: str
    password: str


@dataclass(frozen=True)
class FourTochkiSettings:
    wsdl_url: str
    soap_address_override: str | None = None
    timeout_seconds: int = 30
    baseline_address_id: int | None = 64302


@dataclass(frozen=True)
class AddressListFilter:
    address_id_list: list[int] | None = None
    is_default_address: bool = False
    order_id: int = 0
    payment_type: int = 0

    def to_payload(self) -> dict[str, Any]:
        return {
            "addressIdList": _arr_int(self.address_id_list),
            "isDefaultAddress": self.is_default_address,
            "orderID": self.order_id,
            "paymentType": self.payment_type,
        }


@dataclass(frozen=True)
class GoodsPriceRestFilter:
    codes: list[str]
    warehouse_ids: list[int] | None = None
    include_paid_delivery: bool = False
    search_by_occurrence: bool = False

    def to_payload(self, address_id: int) -> dict[str, Any]:
        return {
            "code_list": _arr_str(self.codes),
            "wrh_list": _arr_int(self.warehouse_ids if self.warehouse_ids is not None else []),
            "include_paid_delivery": self.include_paid_delivery,
            "user_address_id": address_id,
            "searchCodeByOccurence": self.search_by_occurrence,
        }


@dataclass(frozen=True)
class TyreSearchFilter:
    width_min: float | None = None
    width_max: float | None = None
    height_min: float | None = None
    height_max: float | None = None
    diameter_min: float | None = None
    diameter_max: float | None = None
    season_list: list[str] | None = None
    brand_list: list[str] | None = None
    code_list: list[str] | None = None
    model_list: list[str] | None = None
    thorn: bool | None = None
    reinforced: bool | None = None
    runflat: bool | None = None
    retread: bool | None = None
    load_index: list[str] | None = None
    speed_index: list[str] | None = None
    warehouse_ids: list[int] | None = None
    include_paid_delivery: bool = False
    page: int = 0
    page_size: int = 50

    def to_payload(self, address_id: int) -> dict[str, Any]:
        return {
            "brand_list": _arr_str(self.brand_list),
            "code_list": _arr_str(self.code_list),
            "diameter_max": self.diameter_max,
            "diameter_min": self.diameter_min,
            "height_max": self.height_max,
            "height_min": self.height_min,
            "load_index": _arr_str(self.load_index),
            "model_list": _arr_str(self.model_list),
            "reinforced": self.reinforced,
            "runflat": self.runflat,
            "season_list": _arr_str(self.season_list),
            "speed_index": _arr_str(self.speed_index),
            "thorn": self.thorn,
            "width_max": self.width_max,
            "width_min": self.width_min,
            "wrh_list": _arr_int(self.warehouse_ids if self.warehouse_ids is not None else []),
            "include_paid_delivery": self.include_paid_delivery,
            "retread": self.retread,
            "address_id": address_id,
        }


@dataclass(frozen=True)
class DiskSearchFilter:
    width_min: float | None = None
    width_max: float | None = None
    diameter_min: int | None = None
    diameter_max: int | None = None
    bolts_count_min: int | None = None
    bolts_count_max: int | None = None
    bolts_spacing_min: float | None = None
    bolts_spacing_max: float | None = None
    et_min: float | None = None
    et_max: float | None = None
    dia_min: float | None = None
    dia_max: float | None = None
    brand_list: list[str] | None = None
    color_list: list[str] | None = None
    type_list: list[int] | None = None
    warehouse_ids: list[int] | None = None
    include_paid_delivery: bool = False
    page: int = 0
    page_size: int = 50

    def to_payload(self, address_id: int) -> dict[str, Any]:
        return {
            "bolts_count_max": self.bolts_count_max,
            "bolts_count_min": self.bolts_count_min,
            "bolts_spacing_max": self.bolts_spacing_max,
            "bolts_spacing_min": self.bolts_spacing_min,
            "brand_list": _arr_str(self.brand_list),
            "color_list": _arr_str(self.color_list),
            "dia_max": self.dia_max,
            "dia_min": self.dia_min,
            "diameter_max": self.diameter_max,
            "diameter_min": self.diameter_min,
            "et_max": self.et_max,
            "et_min": self.et_min,
            "type_list": _arr_int(self.type_list),
            "width_max": self.width_max,
            "width_min": self.width_min,
            "wrh_list": _arr_int(self.warehouse_ids if self.warehouse_ids is not None else []),
            "include_paid_delivery": self.include_paid_delivery,
            "address_id": address_id,
        }


@dataclass(frozen=True)
class AddressRef:
    address_id: int
    name: str | None
    is_default_address: bool | None
    payment_type: int | None
    raw: dict[str, Any]


@dataclass(frozen=True)
class WarehouseRef:
    warehouse_id: int | None
    warehouse_name: str | None
    raw: dict[str, Any]


@dataclass(frozen=True)
class LogisticsInfo:
    warehouse_id: int
    logistic_days: int | None
    raw: dict[str, Any]


@dataclass(frozen=True)
class WarehouseOfferRow:
    warehouse_id: int
    price_in: float | None
    price_retail_raw: float | None
    rest: float | None
    logistic_days: int | None
    raw_price_rest: dict[str, Any]
    raw_logistics: dict[str, Any] | None = None


@dataclass(frozen=True)
class ProductOffer:
    source_method: str
    source_address_id: int
    supplier_code: str | None
    brand_raw: str | None
    model_raw: str | None
    raw_name: str | None
    product_type_raw: str | int | None
    raw_product: dict[str, Any]
    warehouse_rows: tuple[WarehouseOfferRow, ...] = field(default_factory=tuple)
    logistics_rows: tuple[LogisticsInfo, ...] = field(default_factory=tuple)
