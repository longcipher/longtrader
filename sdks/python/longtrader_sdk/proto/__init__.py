"""Generated protobuf stubs; do not edit by hand.

`protoc`/`grpcio-tools` emit cross-package imports using the absolute proto
package path (e.g. ``from longtrader.account.v1 import account_pb2``) because
that is what the ``package longtrader.account.v1`` declaration in the ``.proto``
files mandates. The stubs, however, are installed *inside* this package as
``longtrader_sdk/proto/longtrader/...``, so ``longtrader`` is not importable as
a top-level module and every multi-package message fails to import with
``ModuleNotFoundError: No module named 'longtrader'``.

The generated layout is canonical and must not be hand-edited, so the mismatch
is repaired here instead: a meta-path finder is installed that resolves the
``longtrader`` package prefix against this directory. That makes ``longtrader``
importable exactly as the generated code expects without permanently adding the
stub tree to ``sys.path`` (where it could shadow unrelated modules) and without
an explicit user-installed ``longtrader`` package losing precedence.

Import this module (directly or via :mod:`longtrader_sdk.session`) before any
``*_pb2`` module.
"""

from __future__ import annotations

import sys
from importlib.abc import MetaPathFinder
from importlib.machinery import PathFinder
from importlib.util import spec_from_loader
from pathlib import Path

_PROTO_ROOT = Path(__file__).resolve().parent
_PROTO_PACKAGE = "longtrader"
_FINDER_NAME = f"__longtrader_sdk_stub_finder_{id(_PROTO_ROOT):x}__"


class _StubPackageFinder(MetaPathFinder):
    """Resolve ``longtrader`` and its submodules from the vendored stub tree.

    Only the ``longtrader`` prefix is claimed, and only while no real
    ``longtrader`` package is importable elsewhere, so a user-installed
    distribution of the same stubs still takes precedence.
    """

    def __init__(self, root: Path) -> None:
        self._root = root
        self._stolen = False

    def _real_spec_exists(self, fullname: str) -> bool:
        """True when ``fullname`` is importable from somewhere other than here."""
        if fullname in sys.modules:
            return True
        parts = fullname.split(".")
        # A top-level `longtrader` that is genuinely installed elsewhere.
        if PathFinder.find_spec(parts[0]) is not None:
            return True
        return False

    def find_spec(self, fullname: str, path: object = None, target: object = None) -> object:
        if self._stolen or self._real_spec_exists(fullname):
            return None
        # Only handle the package itself and its descendants.
        if fullname != _PROTO_PACKAGE and not fullname.startswith(f"{_PROTO_PACKAGE}."):
            return None
        return PathFinder.find_spec(fullname, [str(self._root)])


if not any(getattr(f, "__name__", "") == _FINDER_NAME for f in sys.meta_path):
    _finder = _StubPackageFinder(_PROTO_ROOT)
    setattr(_finder, "__name__", _FINDER_NAME)
    # Ahead of the default finders so the vendored tree is found first, but
    # behind anything already registered by the host application.
    sys.meta_path.append(_finder)
    __all__ = ["_StubPackageFinder", "spec_from_loader"]
