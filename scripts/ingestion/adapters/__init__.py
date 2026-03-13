from .base_adapter import Plan, SupplierAdapter
from .kolobox_adapter import KoloboxAdapter
from .centrshin_adapter import CentrshinAdapter

ADAPTERS = [KoloboxAdapter(), CentrshinAdapter()]
