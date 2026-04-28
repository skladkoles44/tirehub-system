from __future__ import annotations

from typing import Any

from zeep import Client
from zeep.helpers import serialize_object
from zeep.transports import Transport

from .errors import (
    FourTochkiAuthError,
    FourTochkiDataError,
    FourTochkiSemanticError,
    FourTochkiTransportError,
)
from .models import (
    AddressListFilter,
    AddressRef,
    DiskSearchFilter,
    FourTochkiCredentials,
    FourTochkiSettings,
    GoodsPriceRestFilter,
    LogisticsInfo,
    ProductOffer,
    TyreSearchFilter,
    WarehouseOfferRow,
    WarehouseRef,
)


class FourTochkiReadClient:
    def __init__(self, credentials: FourTochkiCredentials, settings: FourTochkiSettings) -> None:
        self._credentials = credentials
        self._settings = settings
        self._client = self._build_client()

    def _build_client(self) -> Client:
        try:
            client = Client(
                wsdl=self._settings.wsdl_url,
                transport=Transport(timeout=self._settings.timeout_seconds),
            )
            if self._settings.soap_address_override:
                client.service._binding_options["address"] = self._settings.soap_address_override
            return client
        except Exception as exc:
            raise FourTochkiTransportError(str(exc)) from exc

    def ping(self) -> bool:
        try:
            result = self._client.service.Ping(
                self._credentials.login,
                self._credentials.password,
            )
        except Exception as exc:
            raise FourTochkiTransportError(str(exc)) from exc
        if result is True:
            return True
        raise FourTochkiAuthError(f"Ping returned non-true value: {result!r}")

    def get_address_list(self, filt: AddressListFilter) -> list[AddressRef]:
        try:
            resp = self._client.service.GetAddressList(
                self._credentials.login,
                self._credentials.password,
                filt.to_payload(),
            )
        except Exception as exc:
            raise FourTochkiTransportError(str(exc)) from exc

        root = self._to_dict(resp)
        address_rows = (
            self._find_first_list_by_key(root, "getAddressListItem")
            or self._extract_list(resp)
        )

        out: list[AddressRef] = []
        for row in address_rows:
            d = self._to_dict(row)
            address_id = self._to_int(d.get("addressId"))
            if address_id is None:
                raise FourTochkiDataError(f"Address row missing addressId: {d!r}")
            out.append(
                AddressRef(
                    address_id=address_id,
                    name=d.get("name") or d.get("addressName"),
                    is_default_address=self._to_bool(d.get("isDefaultAddress")),
                    payment_type=self._to_int(d.get("paymentType")),
                    raw=d,
                )
            )
        return out

    def get_default_address_id(self) -> int:
        rows = self.get_address_list(AddressListFilter(is_default_address=True))
        if not rows:
            raise FourTochkiSemanticError("Default address not found")
        return rows[0].address_id

    def resolve_effective_address_id(self) -> int:
        resolved = self.get_default_address_id()
        baseline = self._settings.baseline_address_id
        if baseline is not None and resolved != baseline:
            raise FourTochkiSemanticError(
                f"Resolved address_id={resolved} differs from frozen baseline={baseline}"
            )
        return resolved

    def get_warehouses(self, address_id: int) -> list[WarehouseRef]:
        try:
            resp = self._client.service.GetWarehouses(
                self._credentials.login,
                self._credentials.password,
                address_id,
            )
        except Exception as exc:
            raise FourTochkiTransportError(str(exc)) from exc

        out: list[WarehouseRef] = []
        for row in self._extract_list(resp):
            d = self._to_dict(row)
            out.append(
                WarehouseRef(
                    warehouse_id=self._to_int(d.get("id") or d.get("warehouseId")),
                    warehouse_name=d.get("name") or d.get("warehouseName"),
                    raw=d,
                )
            )
        return out

    def get_goods_price_rest_by_code(
        self,
        address_id: int,
        codes: list[str],
        warehouse_ids: list[int] | None = None,
        include_paid_delivery: bool = False,
        search_by_occurrence: bool = False,
    ) -> list[ProductOffer]:
        filt = GoodsPriceRestFilter(
            codes=codes,
            warehouse_ids=warehouse_ids,
            include_paid_delivery=include_paid_delivery,
            search_by_occurrence=search_by_occurrence,
        )
        try:
            resp = self._client.service.GetGoodsPriceRestByCode(
                self._credentials.login,
                self._credentials.password,
                filt.to_payload(address_id),
            )
        except Exception as exc:
            raise FourTochkiTransportError(str(exc)) from exc
        return self._parse_goods_price_rest_products(resp, "GetGoodsPriceRestByCode", address_id)

    def find_tyre(self, address_id: int, filt: TyreSearchFilter) -> list[ProductOffer]:
        try:
            resp = self._client.service.GetFindTyre(
                self._credentials.login,
                self._credentials.password,
                filt.to_payload(address_id),
                filt.page,
                filt.page_size,
            )
        except Exception as exc:
            raise FourTochkiTransportError(str(exc)) from exc
        return self._parse_product_offers(resp, "GetFindTyre", address_id)

    def find_disk(self, address_id: int, filt: DiskSearchFilter) -> list[ProductOffer]:
        try:
            resp = self._client.service.GetFindDisk(
                self._credentials.login,
                self._credentials.password,
                filt.to_payload(address_id),
                filt.page,
                filt.page_size,
            )
        except Exception as exc:
            raise FourTochkiTransportError(str(exc)) from exc
        return self._parse_product_offers(resp, "GetFindDisk", address_id)

    def _parse_goods_price_rest_products(
        self,
        resp: Any,
        source_method: str,
        source_address_id: int,
    ) -> list[ProductOffer]:
        root = self._to_dict(resp)
        candidates = self._find_first_list_by_key(root, "price_rest") or self._extract_list(resp)
        out: list[ProductOffer] = []

        for row in candidates:
            product = self._to_dict(row)
            warehouse_rows = tuple(
                WarehouseOfferRow(
                    warehouse_id=self._require_int(pr.get("wrh"), "wrh"),
                    price_in=self._to_float(pr.get("price")),
                    price_retail_raw=self._to_float(pr.get("price_rozn")),
                    rest=self._to_float(pr.get("rest")),
                    logistic_days=None,
                    raw_price_rest=pr,
                    raw_logistics=None,
                )
                for pr in self._extract_wh_price_rest_rows(product)
            )
            out.append(
                ProductOffer(
                    source_method=source_method,
                    source_address_id=source_address_id,
                    supplier_code=product.get("code"),
                    brand_raw=product.get("marka") or product.get("brand"),
                    model_raw=product.get("model"),
                    raw_name=product.get("name"),
                    product_type_raw=product.get("type"),
                    raw_product=product,
                    warehouse_rows=warehouse_rows,
                    logistics_rows=tuple(),
                )
            )
        return out

    def _parse_product_offers(
        self,
        resp: Any,
        source_method: str,
        source_address_id: int,
    ) -> list[ProductOffer]:
        root = self._to_dict(resp)
        root_logistics = self._extract_logistics_rows(root)
        out: list[ProductOffer] = []

        for row in self._find_product_rows(root):
            product = self._to_dict(row)
            logistics_raw = self._extract_logistics_rows(product) or root_logistics
            logistics_rows = tuple(
                LogisticsInfo(
                    warehouse_id=self._require_int(lr.get("whId") or lr.get("wh_id"), "whId"),
                    logistic_days=self._to_int(lr.get("logistDays")),
                    raw=lr,
                )
                for lr in logistics_raw
            )
            logistics_by_wh = {x.warehouse_id: x for x in logistics_rows}

            warehouse_rows: list[WarehouseOfferRow] = []
            for pr in self._extract_wh_price_rest_rows(product):
                warehouse_id = self._require_int(pr.get("wrh"), "wrh")
                matched = logistics_by_wh.get(warehouse_id)
                warehouse_rows.append(
                    WarehouseOfferRow(
                        warehouse_id=warehouse_id,
                        price_in=self._to_float(pr.get("price")),
                        price_retail_raw=self._to_float(pr.get("price_rozn")),
                        rest=self._to_float(pr.get("rest")),
                        logistic_days=matched.logistic_days if matched else None,
                        raw_price_rest=pr,
                        raw_logistics=matched.raw if matched else None,
                    )
                )

            out.append(
                ProductOffer(
                    source_method=source_method,
                    source_address_id=source_address_id,
                    supplier_code=product.get("code"),
                    brand_raw=product.get("marka") or product.get("brand"),
                    model_raw=product.get("model"),
                    raw_name=product.get("name"),
                    product_type_raw=product.get("type") or product.get("rim_vid_name"),
                    raw_product=product,
                    warehouse_rows=tuple(warehouse_rows),
                    logistics_rows=logistics_rows,
                )
            )
        return out

    def _find_product_rows(self, root: dict[str, Any]) -> list[dict[str, Any]]:
        for key in ("TyrePriceRest", "DiskPriceRest", "price_rest"):
            rows = self._find_first_list_by_key(root, key)
            if rows:
                return [self._to_dict(x) for x in rows]

        for key in ("goods", "good", "result", "items", "item", "searchResult", "tyre", "disk"):
            rows = self._find_first_list_by_key(root, key)
            if rows:
                return [self._to_dict(x) for x in rows]

        return [self._to_dict(x) for x in self._extract_list(root)]

    def _extract_wh_price_rest_rows(self, data: dict[str, Any]) -> list[dict[str, Any]]:
        whpr = data.get("whpr")
        if whpr is not None:
            rows = self._find_first_list_by_key(self._to_dict(whpr), "wh_price_rest")
            if rows:
                return [self._to_dict(x) for x in rows]

        rows = self._find_first_list_by_key(data, "wh_price_rest")
        return [self._to_dict(x) for x in rows] if rows else []

    def _extract_logistics_rows(self, data: dict[str, Any]) -> list[dict[str, Any]]:
        wl = data.get("warehouseLogistics")
        if wl is not None:
            rows = self._find_first_list_by_key(self._to_dict(wl), "WarehouseLogistic")
            if rows:
                return [self._to_dict(x) for x in rows]

        rows = self._find_first_list_by_key(data, "WarehouseLogistic")
        return [self._to_dict(x) for x in rows] if rows else []

    def _find_first_list_by_key(self, value: Any, target_key: str) -> list[Any] | None:
        if isinstance(value, dict):
            if target_key in value and isinstance(value[target_key], list):
                return value[target_key]
            for nested in value.values():
                found = self._find_first_list_by_key(nested, target_key)
                if found:
                    return found
        elif isinstance(value, list):
            for item in value:
                found = self._find_first_list_by_key(item, target_key)
                if found:
                    return found
        return None

    @staticmethod
    def _extract_list(value: Any) -> list[Any]:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            for v in value.values():
                if isinstance(v, list):
                    return v
        return []

    @staticmethod
    def _to_dict(value: Any) -> dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return value
        serialized = serialize_object(value, target_cls=dict)
        if isinstance(serialized, dict):
            return serialized
        return {"value": serialized}

    @staticmethod
    def _to_int(value: Any) -> int | None:
        if value in (None, ""):
            return None
        return int(value)

    @staticmethod
    def _require_int(value: Any, field_name: str) -> int:
        parsed = FourTochkiReadClient._to_int(value)
        if parsed is None:
            raise FourTochkiDataError(f"Missing required int field {field_name}")
        return parsed

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value in (None, ""):
            return None
        return float(value)

    @staticmethod
    def _to_bool(value: Any) -> bool | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        s = str(value).strip().lower()
        if s in {"true", "1"}:
            return True
        if s in {"false", "0"}:
            return False
        return None
