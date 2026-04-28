# 4tochki API integration baseline

Status: baseline v1
Scope: supplier read-side integration for stock, price, warehouse, logistics, and later deterministic supplier-offer extraction
Audience: anyone touching `/docs/integrations/4tochki/` integration logic, probes, adapter, mapping, or future order flow
This document is the single baseline for this integration. It deliberately separates contract knowledge, probe-verified runtime semantics, inferred join rules, unknowns, and forbidden assumptions.

---

## 1. Purpose

This baseline fixes what is currently known about the 4tochki B2B API from two different evidence classes:

1. **Contract/WSDL/XSD facts** — method signatures and filter schemas visible in WSDL/XSD.
2. **Probe-verified runtime semantics** — behavior confirmed by live calls and XRAY output.

Critical rule:

> WSDL describes contract shape, but not full runtime semantics on concrete values.
>
> Contract != runtime behavior.

Therefore this baseline must be used in three layers of certainty:

- **CONTRACT** — present in WSDL/XSD or vendor docs.
- **PROBE_VERIFIED** — observed in successful live calls.
- **UNKNOWN** — present in contract/docs but not yet runtime-confirmed for this account/contour.

---

## 2. Integration objective

The current objective is **read-side supplier integration**, not write automation.

Target use:

- obtain supplier-visible products,
- obtain per-warehouse stock and price,
- obtain logistics days to reachable warehouses,
- preserve supplier reality as-is,
- later map this into normalized supplier offers inside the intake/current-state platform.

This integration is intended to become a component of the VPS-based supplier stock/price collection contour.

Not the current objective:

- direct auto-ordering,
- direct write flows into supplier API,
- business decisioning,
- storefront/catalog shaping,
- final identity resolution.

---

## 3. Runtime baseline fixed so far

### 3.1 Credentials / transport

- Transport works.
- Authentication works.
- SOAP request/response path works.
- SOAP endpoint may require forcing `soap:address` to HTTPS in the client contour.
- `Ping(login, password)` returned `true` in the probe contour.

### 3.2 Baseline address

Current probe baseline address for this account:

- `address_id = 64302`
- address name: `Зелёная 1/1`
- `isDefaultAddress = true`
- payment type observed on returned address: `3`

Important:

- `64302` is a **runtime baseline for the current account**, not a universal constant of the API.
- It is safe to treat it as the current integration baseline only after `GetAddressList` resolution for this account.

### 3.3 Read chain already proven

The following read chain is already proven end-to-end:

1. `Ping` confirms transport/auth.
2. `GetAddressList` resolves usable delivery address IDs.
3. `GetWarehouses(addressId)` resolves reachable warehouse catalog for that address.
4. Product search methods (`GetFindTyre`, `GetFindDisk`) return offers with per-warehouse stock/price plus separate warehouse logistics mapping.
5. `GetGoodsPriceRestByCode` returns exact-code stock/price by warehouse when supplied with valid `user_address_id`.

This is the current minimal proven read contour.

---

## 4. Method status model

Each method used in this baseline must be treated in one of these statuses:

- **CONTRACT_ONLY** — visible in WSDL/docs, runtime not yet confirmed here.
- **PROBE_VERIFIED** — runtime behavior confirmed by live call.
- **FORBIDDEN_FOR_NOW** — exists in API, but not allowed in current phase because current phase is read-side only.

---

## 5. Probe-verified methods and runtime semantics

## 5.1 `Ping(login, password)`

Status: **PROBE_VERIFIED**

Observed runtime behavior:

- Request reached server successfully.
- Response returned `true`.
- This proves transport/auth path is alive.

What it proves:

- endpoint reachable,
- credentials accepted,
- SOAP envelope/headers acceptable in current contour.

What it does **not** prove:

- product methods work,
- address-dependent methods are correctly parameterized,
- any business data access beyond auth/transport.

---

## 5.2 `GetAddressList(login, password, filter)`

Status: **PROBE_VERIFIED**

WSDL/XSD contract shape for filter:

`getAddressListFilter(addressIdList: ArrayOfint, isDefaultAddress: boolean, orderID: int, paymentType: int)`

### Probe-confirmed cases

#### Case: `zero_scalars`

Request semantics:

```json
{
  "addressIdList": null,
  "isDefaultAddress": false,
  "orderID": 0,
  "paymentType": 0
}
```

Observed behavior:

- call succeeded,
- `success = true`,
- returned full address list for this account,
- one returned address included `addressId = 12363`.

#### Case: `default_only`

Request semantics:

```json
{
  "addressIdList": null,
  "isDefaultAddress": true,
  "orderID": 0,
  "paymentType": 0
}
```

Observed behavior:

- call succeeded,
- `success = true`,
- returned the default address,
- returned address had `addressId = 64302` and `isDefaultAddress = true`.

#### Case: `with_empty_list`

Request semantics:

```json
{
  "addressIdList": {"int": []},
  "isDefaultAddress": false,
  "orderID": 0,
  "paymentType": 0
}
```

Observed behavior:

- call succeeded,
- `success = true`,
- empty `addressIdList` is accepted,
- behavior matched “all addresses” rather than “no addresses”.

### Probe-verified semantics summary

- `addressIdList = null` is accepted.
- `addressIdList = []` is accepted.
- `isDefaultAddress = true` restricts output to the default address.
- scalar zeros (`orderID = 0`, `paymentType = 0`) are accepted.
- This method is the authoritative source for valid address IDs for the current account.

### Direct integration rule

Never hardcode an address ID without first knowing how it was obtained.

Allowed baseline rule:

- current account baseline may use `address_id = 64302` **only because it was resolved by `GetAddressList(... isDefaultAddress=true ...)` in the probe contour**.

---

## 5.3 `GetWarehouses(login, password, addressId)`

Status: **PROBE_VERIFIED**

WSDL/XSD contract shape:

`GetWarehouses(login: string, password: string, addressId: int)`

### Negative runtime fact

Using invalid `addressId` produced:

- `success = false`
- `error.code = 600`
- `error.comment = "Адрес доставки с таким ИД не найден."`

This proves the method is business-parameter-sensitive and does not accept arbitrary IDs.

### Positive runtime fact

Using valid `addressId = 64302` produced warehouse catalog output.

Observed warehouse item structure includes fields like:

- `id`
- `key`
- `name`
- `shortName`
- `haveDelivery`
- `havePickup`
- `isPaidDelivery`
- `logisticDays`
- `stoID`
- `background-color`

Observed examples in output:

- warehouse `232`
- warehouse `2184`
- warehouse `3`
- warehouse `846`
- warehouse `1608`
- warehouse `1431`
- warehouse `2043`
- and many others

### Probe-verified semantics summary

- `GetWarehouses` requires a real address ID.
- It returns the warehouse catalog reachable/usable for that address.
- Returned warehouse items already contain `logisticDays` at catalog level.
- This warehouse catalog is joinable with product-search outputs by warehouse ID.

### Important non-assumption

`GetWarehouses` is **not** itself a stock endpoint.

It provides warehouse catalog / routing / delivery metadata, not product inventory rows.

---

## 5.4 `GetGoodsPriceRestByCode(login, password, filter)`

Status: **PROBE_VERIFIED**

WSDL/XSD contract shape for filter:

`GoodsPriceRestFilter(code_list: ArrayOfstring, wrh_list: ArrayOfint, include_paid_delivery: boolean, user_address_id: int, searchCodeByOccurence: boolean)`

### Proven contract clarification

A failed client-side attempt using keyword `code` instead of `code_list` raised a type error in the SOAP client:

- `GoodsPriceRestFilter() got an unexpected keyword argument 'code'`
- signature confirmed: `code_list`, `wrh_list`, `include_paid_delivery`, `user_address_id`, `searchCodeByOccurence`

This is a confirmed contract fact for client implementation.

### Probe-confirmed cases

#### Case: `all_wrh_exact`

Request semantics:

```json
{
  "code_list": {"string": ["4405300"]},
  "wrh_list": {"int": []},
  "include_paid_delivery": false,
  "user_address_id": 64302,
  "searchCodeByOccurence": false
}
```

Observed behavior:

- call succeeded,
- returned `price_rest_list.price_rest[]`,
- for code `4405300` returned `wh_price_rest[]` entries for multiple warehouses,
- observed warehouses included `1046`, `2234`, `232`, `2184`, `1222`.

Observed sample rows:

- `wrh = 1046`, `price = 4212`, `price_rozn = 5190`, `rest = 41`
- `wrh = 2234`, `price = 4212`, `price_rozn = 5190`, `rest = 2`
- `wrh = 232`, `price = 4212`, `price_rozn = 5190`, `rest = 41`
- `wrh = 2184`, `price = 4212`, `price_rozn = 5190`, `rest = 41`
- `wrh = 1222`, `price = 4212`, `price_rozn = 5190`, `rest = 2`

#### Case: `one_wrh_exact`

Request semantics:

```json
{
  "code_list": {"string": ["4405300"]},
  "wrh_list": {"int": [232]},
  "include_paid_delivery": false,
  "user_address_id": 64302,
  "searchCodeByOccurence": false
}
```

Observed behavior:

- call succeeded,
- result was restricted to warehouse `232`,
- returned `price = 4212`, `price_rozn = 5190`, `rest = 41`, `wrh = 232`.

#### Case: `all_wrh_occurrence`

Request semantics:

```json
{
  "code_list": {"string": ["4405300"]},
  "wrh_list": {"int": []},
  "include_paid_delivery": false,
  "user_address_id": 64302,
  "searchCodeByOccurence": true
}
```

Observed behavior:

- call succeeded,
- on exact code `4405300`, output matched the exact-search case for observed rows,
- no visible effect from `searchCodeByOccurence = true` on this exact-code probe.

### Probe-verified semantics summary

- `code_list` is the correct field name.
- `wrh_list = []` is accepted and behaves like “all relevant warehouses for this address”.
- `wrh_list = [warehouse_id]` restricts results to that warehouse.
- `user_address_id` participates in routing/context and must be valid.
- `searchCodeByOccurence` had no observable effect on the exact-code probe `4405300`.
- The method returns stock/price rows by warehouse and is a valid source for supplier-offer extraction.

### Direct integration rule

This method is the best exact-code read method currently proven for stock and price by warehouse.

---

## 5.5 `GetFindTyre(login, password, filter, page, pageSize)`

Status: **PROBE_VERIFIED**

Contract facts known from docs and filter catalog:

- paginated output,
- default page size in docs = 50,
- max page size in docs = 2000,
- filter type = `FindTyreFilter`.

WSDL/XSD filter shape observed:

`FindTyreFilter(brand_list, code_list, diameter_max, diameter_min, diameter_out_max, diameter_out_min, height_max, height_min, load_index, model_list, quality, reinforced, runflat, season_list, sort, speed_index, thorn, type_list, width_max, width_min, wrh_list, include_paid_delivery, axis_apply, sloy_list, retread, address_id)`

### Probe-confirmed runtime behavior

A live search for tyre size `185/60R15` returned product rows and warehouse logistics.

Observed product row structure includes fields like:

- `code`
- `marka`
- `model`
- `name`
- `season`
- `thorn`
- `type`
- `quality`
- `saleId`
- image URLs
- `whpr.wh_price_rest[]`

Observed example row fragments:

- code `2349200`, marka `Pirelli Formula`, model `Ice`, name `185/60R15 88T XL Ice TL (шип.)`
- code `TS77957`, marka `Ikon`, model `Character Ice 7 (Nordman 7)`, name `185/60R15 88T XL Character Ice 7 (Nordman 7) TL (шип.)`

Observed `wh_price_rest[]` shape:

- `price`
- `price_rozn`
- `rest`
- `wrh`

Observed logistics block:

- `warehouseLogistics.WarehouseLogistic[]`
- each entry contains `whId` and `logistDays`

Observed logistics examples:

- `whId = 232`, `logistDays = 7`
- `whId = 2184`, `logistDays = 7`
- `whId = 3`, `logistDays = 8`
- `whId = 1`, `logistDays = 3`
- `whId = 1046`, `logistDays = 0`

### Probe-verified semantics summary

- This method returns searchable tyre offers.
- It returns stock/price per warehouse inside `whpr.wh_price_rest[]`.
- It returns logistics separately in `warehouseLogistics`.
- Warehouse stock rows and logistics rows must be joined by warehouse ID.
- Product naming contains structured commercial text useful for later parsing, but raw name must be preserved as supplier truth.

### Direct integration rule

This is a valid discovery/search method for tyre catalog exploration and extraction of raw supplier offers.

---

## 5.6 `GetFindDisk(login, password, filter, page, pageSize)`

Status: **PROBE_VERIFIED**

WSDL/XSD filter shape observed:

`FindDiskFilter(bolts_count_max, bolts_count_min, bolts_spacing_max, bolts_spacing_min, brand_list, code_list, color_list, dia_max, dia_min, diameter_max, diameter_min, et_max, et_min, model_list, rim_vid_name_list, sort, type_list, width_max, width_min, wrh_list, include_paid_delivery, address_id)`

### Probe-confirmed runtime behavior

Live smoke returned disk product rows and warehouse logistics.

Observed product row structure includes fields like:

- `code`
- `color`
- `marka`
- `model`
- `name`
- `rim_vid_name`
- `type`
- `saleId`
- image URLs
- `whpr.wh_price_rest[]`

Observed example row fragments:

- code `WHS520183`, marka `iFree`, model `Грид (КС1082)`, color `Нео-классик`
- code `WHS098429`, marka `СКАД`, model `Мальта (КЛ189)`, color `Алмаз`
- code `WHS521995`, marka `K&K`, model `Джемини-оригинал (КС617)`, color `Кварц`

Observed `wh_price_rest[]` shape:

- `price`
- `price_rozn`
- `rest`
- `wrh`

Observed logistics block:

- `warehouseLogistics.WarehouseLogistic[]`
- each entry contains `whId` and `logistDays`

### Probe-verified semantics summary

- This method behaves analogously to `GetFindTyre` from extraction perspective.
- It returns warehouse-scoped price/rest rows and separate logistics mapping.
- Join key again is warehouse ID.

---

## 6. Relevant filter catalog fixed from WSDL/XSD

This section records the relevant filter signatures currently visible in the contract and important for read-side supplier integration.

## 6.1 Search/product filters

### `FindTyreFilter`

```text
brand_list: ArrayOfstring
code_list: ArrayOfstring
diameter_max: decimal
diameter_min: decimal
diameter_out_max: decimal
diameter_out_min: decimal
height_max: decimal
height_min: decimal
load_index: ArrayOfstring
model_list: ArrayOfstring
quality: int
reinforced: boolean
runflat: boolean
season_list: ArrayOfstring
sort: int
speed_index: ArrayOfstring
thorn: boolean
type_list: ArrayOfstring
width_max: decimal
width_min: decimal
wrh_list: ArrayOfint
include_paid_delivery: boolean
axis_apply: ArrayOfstring
sloy_list: ArrayOfint
retread: boolean
address_id: int
```

### `FindDiskFilter`

```text
bolts_count_max: int
bolts_count_min: int
bolts_spacing_max: decimal
bolts_spacing_min: decimal
brand_list: ArrayOfstring
code_list: ArrayOfstring
color_list: ArrayOfstring
dia_max: decimal
dia_min: decimal
diameter_max: int
diameter_min: int
et_max: decimal
et_min: decimal
model_list: ArrayOfstring
rim_vid_name_list: ArrayOfstring
sort: int
type_list: ArrayOfint
width_max: decimal
width_min: decimal
wrh_list: ArrayOfint
include_paid_delivery: boolean
address_id: int
```

### `GoodsPriceRestFilter`

```text
code_list: ArrayOfstring
wrh_list: ArrayOfint
include_paid_delivery: boolean
user_address_id: int
searchCodeByOccurence: boolean
```

### `GetGoodsByCarFilter`

```text
marka: string
model: string
modification: string
podbor_type: ArrayOfint
season_list: ArrayOfstring
thorn: boolean
type: ArrayOfstring
wrh_list: ArrayOfint
year_beg: string
year_end: string
address_id: int
```

### `getPressureSensorFilter`

```text
brand_list: ArrayOfstring
code_list: ArrayOfstring
sort: int
include_paid_delivery: boolean
address_id: int
wrh_list: ArrayOfint
```

### `getFastenerFilter`

```text
brand_list: ArrayOfstring
code_list: ArrayOfstring
sort: int
subtype_id_list: ArrayOfint
wrh_list: ArrayOfint
include_paid_delivery: boolean
address_id: int
```

### `GetConsumableFilter`

```text
applicability_list: ArrayOfint
brand_list: ArrayOfstring
code_list: ArrayOfstring
color_list: ArrayOfstring
material_list: ArrayOfstring
sort: int
type: int
weight_max: int
weight_min: int
wrh_list: ArrayOfint
include_paid_delivery: boolean
address_id: int
```

### `GetOilFilter`

```text
applicability_list: ArrayOfint
brand_list: ArrayOfstring
code_list: ArrayOfstring
manufacturer_code_list: ArrayOfstring
model_list: ArrayOfstring
viscosity_list: ArrayOfstring
type_list: ArrayOfint
sort: int
wrh_list: ArrayOfint
include_paid_delivery: boolean
address_id: int
```

### `GetFindCameraFilter`

```text
address_id: int
brand_list: ArrayOfstring
code_list: ArrayOfstring
diameter_list: ArrayOfdecimal
sort: int
subtype_id_list: ArrayOfunsignedByte
wrh_list: ArrayOfint
include_paid_delivery: boolean
```

### `GetFindWheelFilter`

```text
address_id: int
code_list: ArrayOfstring
disk_filter: FindWheelFilterDisk
include_paid_delivery: boolean
sort: int
tire_filter: FindWheelFilterTireContainer
wrh_list: ArrayOfint
```

## 6.2 Address and basic stock/price filters

### `getAddressListFilter`

```text
addressIdList: ArrayOfint
isDefaultAddress: boolean
orderID: int
paymentType: int
```

### `getPriceFilter`

```text
priceId: int
code: string
page: int
```

### `getRestFilter`

```text
wrh: int
code: string
page: int
types: ArrayOfstring
```

---

## 7. Relevant method catalog status map

This is not a full behavioral catalog. It is a status map for methods relevant to this integration at the current stage.

## 7.1 Read-side methods relevant now

| Method | Status | Use in integration |
|---|---|---|
| `Ping` | PROBE_VERIFIED | transport/auth smoke |
| `GetAddressList` | PROBE_VERIFIED | resolve valid address IDs |
| `GetWarehouses` | PROBE_VERIFIED | warehouse catalog for address |
| `GetGoodsPriceRestByCode` | PROBE_VERIFIED | exact-code stock/price by warehouse |
| `GetFindTyre` | PROBE_VERIFIED | tyre search + warehouse stock/price + logistics |
| `GetFindDisk` | PROBE_VERIFIED | disk search + warehouse stock/price + logistics |
| `GetGoodsInfo` | CONTRACT_ONLY | enrich product metadata later |
| `GetGoodsByCar` | CONTRACT_ONLY | car-fitment discovery later |
| `GetFastener` | CONTRACT_ONLY | accessory search later |
| `GetPressureSensor` | CONTRACT_ONLY | TPMS search later |
| `GetConsumable` | CONTRACT_ONLY | consumables later |
| `GetOil` | CONTRACT_ONLY | oils later |
| `GetFindCamera` | CONTRACT_ONLY | camera/tube search later |
| `GetFindWheel` | CONTRACT_ONLY | wheel assemblies later |
| `GetPrice` | CONTRACT_ONLY | alternative price lookup; not baseline |
| `GetRest` | CONTRACT_ONLY | alternative rest lookup; not baseline |

## 7.2 Methods present in docs but outside current phase

Methods for orders, delivery mutation, movement, remarking, inventory, status updates, and other write/business actions are **not baseline methods for current implementation**.

Even if contract exists, they are outside current phase until a separate write adapter exists.

---

## 8. Canonical join model

This is the current operational join model for warehouse-scoped supplier offers.

```text
offer warehouse id from product output = wh_price_rest.wrh
                                   = warehouseLogistics.whId
                                   = warehouse catalog id from GetWarehouses.id
```

Meaning:

- `wh_price_rest.wrh` identifies the warehouse for stock/price row.
- `warehouseLogistics.whId` identifies the same warehouse in logistics mapping.
- `GetWarehouses.id` identifies the same warehouse in warehouse catalog.

Therefore the canonical warehouse join key is the numeric warehouse ID.

### Consequence

To build one supplier-offer row correctly, the adapter must:

1. read raw product row,
2. expand `wh_price_rest[]`,
3. join each expanded row to `warehouseLogistics` by warehouse ID,
4. optionally enrich from warehouse catalog returned by `GetWarehouses` for the active address.

No other join key is currently approved.

---

## 9. Extraction target for supplier-offer layer

The target below is not the final marketplace/read-model shape. It is the intended supplier-offer extraction target preserving supplier truth with warehouse granularity.

```json
{
  "supplier": "4tochki",
  "source_method": "GetGoodsPriceRestByCode",
  "source_address_id": 64302,
  "fetched_at": "<utc_timestamp>",
  "product_family": "tyre",
  "supplier_code": "4405300",
  "raw_name": "185/60R15 88T XL Ice TL (шип.)",
  "brand_raw": "Pirelli Formula",
  "model_raw": "Ice",
  "attributes_raw": {
    "width": 185,
    "height": 60,
    "diameter": 15,
    "load_index": "88",
    "speed_index": "T",
    "extra_tokens": ["XL", "TL", "шип."]
  },
  "offers": [
    {
      "warehouse_id": 232,
      "warehouse_name_raw": null,
      "rest": 41,
      "price_in": 4212,
      "price_retail_raw": 5190,
      "logistic_days": 7,
      "is_paid_delivery": null,
      "pickup_available": null,
      "delivery_available": null
    }
  ]
}
```

### Rules for this extraction target

- `raw_name` must be preserved exactly as supplier gave it.
- `brand_raw` and `model_raw` are supplier fields, not final canonical identity.
- one product row may expand into multiple warehouse-scoped offer rows.
- `price_in` and `price_retail_raw` must remain semantically separate.
- warehouse logistics must be joined explicitly, never guessed.
- the extraction target must preserve enough source detail for replay and later normalization.

---

## 10. What is explicitly **not normalized** yet

The following must **not** be treated as final normalized truth at this stage:

### 10.1 Product naming

Do **not** treat these as already normalized attributes:

- `name`
- `marka`
- `model`

Reason:

They are supplier-facing commercial fields. They may contain structured information, but they are still raw supplier fields until a separate deterministic normalization layer proves how to parse them.

### 10.2 Warehouse/logistics composition

Do **not** flatten `warehouseLogistics` into offer rows without explicit join on warehouse ID.

Reason:

Logistics is delivered as a separate structure. Joining by position/order is unapproved and unsafe.

### 10.3 Warehouse catalog semantics

Do **not** treat `GetWarehouses` as stock truth.

Reason:

It is warehouse catalog/routing metadata, not product inventory output.

### 10.4 Price semantics

Do **not** treat `price_rozn` as final sell-side price in your own system.

Reason:

At current stage it is only a supplier-returned retail-like field. Its downstream business meaning in your system is not fixed by this baseline.

---

## 11. Tyre name parsing target for later normalization

This section does **not** claim normalization is already done. It only states what later parsing is expected to aim at for passenger tyres.

Given raw examples such as:

- `185/60R15 88T XL Ice TL (шип.)`
- `185/60R15 88T XL Character Ice 7 (Nordman 7) TL (шип.)`

A later tyre normalization layer will likely need to parse fields such as:

- width
- height
- diameter
- load index
- speed index
- reinforced / XL marker
- tube/tubeless marker if relevant (`TL` etc.)
- thorn/studded marker (`шип.` / boolean)
- season
- commercial model remainder
- possibly type / application

But current baseline rule remains:

- preserve full `raw_name`,
- preserve supplier `marka` and `model` as raw source fields,
- never discard tokens not yet fully understood.

---

## 12. Unknowns / to verify

The following are **not** fixed yet and must not be presented as established truth.

### 12.1 `include_paid_delivery`

Unknowns:

- exact runtime semantics,
- whether it changes warehouse availability,
- whether it changes logistics selection only,
- whether it changes price rows.

### 12.2 `searchCodeByOccurence`

Known:

- on exact code `4405300`, it showed no visible difference.

Unknown:

- behavior on partial code,
- behavior on fuzzy occurrence matches,
- whether result ordering changes.

### 12.3 Pagination semantics

Known:

- docs say `GetFindTyre` is paginated,
- docs mention default page size 50 and max 2000.

Unknown:

- pagination stability under repeated reads,
- deterministic ordering guarantees,
- whether page boundaries shift during live catalog changes.

### 12.4 Rate limiting / throttling / quotas

Unknown:

- formal rate limits,
- burst tolerance,
- backoff requirements,
- account-level quota behavior.

### 12.5 Filter interaction semantics

Unknown:

- exact meaning of `quality` filter values,
- exact `type_list` semantics across product families,
- how `wrh_list` interacts with address-based routing for all methods,
- whether some filters are silently ignored in some combinations.

### 12.6 Cross-method consistency

Unknown:

- whether `GetFindTyre` and `GetGoodsPriceRestByCode` always agree on the same code and address at the same moment,
- whether warehouse subsets differ by method,
- whether logistics output and warehouse catalog `logisticDays` can diverge.

---

## 13. Direct prohibitions

These are hard rules for the current phase.

### 13.1 No write-side automation yet

Forbidden now:

- creating orders,
- changing order status,
- delivery mutation,
- warehouse movement writes,
- inventory writes,
- any other supplier write flow.

Reason:

Current phase is read-adapter only. Write-side requires its own contract, idempotency model, safety gates, and decision layer.

### 13.2 No business decisions directly on probe scripts

Forbidden now:

- building auto-order logic on top of ad-hoc probe scripts,
- mixing live probes with production adapter behavior,
- treating experimental scripts as production integration layer.

### 13.3 No premature normalization claims

Forbidden now:

- claiming tyre/disk names are already normalized,
- treating `marka/model/name` as final identity,
- collapsing raw supplier text into canonical attributes without a separate normalization contract.

### 13.4 No warehouse join by position/order

Forbidden now:

- joining `wh_price_rest` with `warehouseLogistics` by array order,
- joining warehouse rows by name/key instead of numeric ID unless explicitly proven.

### 13.5 No silent semantic rewrites

Forbidden now:

- renaming supplier semantics into internal business semantics without explicit mapping,
- treating `price_rozn` as your own final sell price,
- treating `GetWarehouses` as inventory endpoint.

---

## 14. Approved implementation direction

The approved next implementation layer is:

1. **baseline address resolution**
   - resolve current usable `address_id` via `GetAddressList`
   - currently fixed baseline value: `64302`
2. **read adapter**
   - implement deterministic read-side client for proven methods
   - no writes
3. **probe tests**
   - each asserted fact in this document should be backed by probe tests
4. **supplier-offer extraction**
   - warehouse-scoped raw supplier offers with explicit joins
5. **later normalization layer**
   - separate deterministic parsing and canonicalization
6. **much later decision/write layer**
   - separate from read adapter

This preserves correct layer boundaries:

- supplier API read layer != normalization layer != decision layer != write layer

---

## 15. Minimal baseline read contour

A minimal productionizable read contour derived from this baseline should look like this:

### Step A. Resolve address

- call `GetAddressList`
- prefer default address resolution
- persist selected address ID as runtime baseline for this supplier account

### Step B. Resolve warehouse catalog

- call `GetWarehouses(address_id)`
- persist warehouse metadata catalog for that address

### Step C. Read product/search results

Depending on use case:

- exact code lookup → `GetGoodsPriceRestByCode`
- tyre search/discovery → `GetFindTyre`
- disk search/discovery → `GetFindDisk`

### Step D. Expand warehouse rows

- expand `whpr.wh_price_rest[]`
- one product row may yield multiple warehouse-scoped offers

### Step E. Join logistics

- join on warehouse ID with `warehouseLogistics.whId`
- optionally enrich from `GetWarehouses.id`

### Step F. Preserve raw supplier truth

Persist at least:

- source method
- source address id
- supplier code
- raw name
- raw brand/model
- warehouse id
- stock/rest
- supplier prices
- logistics days
- fetch timestamp

---

## 16. Probe obligations derived from this baseline

Every statement below should eventually be covered by automated or semi-automated probes:

1. `Ping` returns success with current credentials.
2. `GetAddressList` accepts null/empty `addressIdList`.
3. `GetAddressList` with `isDefaultAddress=true` returns current default address.
4. `GetWarehouses` rejects invalid `addressId` with business error.
5. `GetWarehouses` succeeds with baseline address `64302`.
6. `GetGoodsPriceRestByCode` requires `code_list`, not `code`.
7. `GetGoodsPriceRestByCode` with empty `wrh_list` returns multi-warehouse result.
8. `GetGoodsPriceRestByCode` with `[232]` restricts to warehouse `232`.
9. `GetFindTyre` returns both `wh_price_rest[]` and `warehouseLogistics[]`.
10. `GetFindDisk` returns both `wh_price_rest[]` and `warehouseLogistics[]`.
11. warehouse join key remains numeric warehouse ID across outputs.

---

## 17. What this baseline authorizes today

This document authorizes:

- fixing `address_id = 64302` as current baseline for this account,
- implementing a read adapter based on the proven methods above,
- writing probe tests for every approved claim,
- designing supplier-offer extraction around warehouse-scoped rows and explicit joins.

This document does **not** authorize:

- write-side order automation,
- direct production business decisions on raw probe code,
- collapsing raw supplier fields into final normalized identity,
- bypassing address resolution and warehouse join rules.

---

## 18. Canonical summary

The 4tochki integration is currently understood at two distinct levels: contract shape from WSDL/XSD and runtime semantics proven by live probes. The live read contour already works for transport/auth, address resolution, warehouse catalog resolution, exact-code stock/price lookup, tyre search, and disk search. The warehouse join key is the numeric warehouse ID shared across `wh_price_rest.wrh`, `warehouseLogistics.whId`, and `GetWarehouses.id`. The current approved implementation direction is a read-only supplier adapter that preserves supplier truth, expands warehouse-scoped stock/price rows, joins logistics explicitly, and postpones normalization and all write flows to separate layers.
