"""Camada neutra para o cliente do catalogo."""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

_CURRENT_FILE = Path(__file__).resolve()
_TARGET_FILE = None
for path in _CURRENT_FILE.parent.glob("*client.py"):
    if path.resolve() == _CURRENT_FILE:
        continue

    source = path.read_text(encoding="utf-8", errors="ignore")
    if "async def search_content" in source and "async def get_content_details" in source:
        _TARGET_FILE = path
        break

if _TARGET_FILE is None:
    raise ImportError("Nao foi possivel localizar o backend do catalogo.")

_SPEC = spec_from_file_location("services._catalog_backend", _TARGET_FILE)
if _SPEC is None or _SPEC.loader is None:
    raise ImportError("Nao foi possivel carregar o backend do catalogo.")

_MODULE = module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)

__all__ = [name for name in dir(_MODULE) if not name.startswith("_")]
globals().update({name: getattr(_MODULE, name) for name in __all__})
