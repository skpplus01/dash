"""Realtime Micro TAIEX Futures quote dashboard for SinoPac Shioaji.

The module is intentionally notebook friendly: import ``run_colab_dashboard`` in
Google Colab or Jupyter to start an ipywidgets dashboard, or execute this file
as a CLI to print incoming quotes in a terminal.
"""

from __future__ import annotations

import argparse
import os
import queue
import signal
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable, Mapping, Sequence

DEFAULT_CONTRACT_CODE = "TMFR1"
DEFAULT_QUOTE_TYPES = ("tick", "bid_ask")
QUOTE_TYPE_ALIASES = {
    "tick": "Tick",
    "ticks": "Tick",
    "bidask": "BidAsk",
    "bid_ask": "BidAsk",
    "bid-ask": "BidAsk",
    "quote": "Quote",
}


@dataclass
class QuoteRecord:
    """Normalized quote payload used by the UI and CLI."""

    received_at: str
    quote_type: str
    code: str = ""
    date: str = ""
    time: str = ""
    close: Any = ""
    volume: Any = ""
    bid_price: Any = ""
    bid_volume: Any = ""
    ask_price: Any = ""
    ask_volume: Any = ""
    raw: dict[str, Any] = field(default_factory=dict)

    def as_row(self) -> dict[str, Any]:
        return {
            "received_at": self.received_at,
            "type": self.quote_type,
            "code": self.code,
            "date": self.date,
            "time": self.time,
            "close": self.close,
            "volume": self.volume,
            "bid": self.bid_price,
            "bid_vol": self.bid_volume,
            "ask": self.ask_price,
            "ask_vol": self.ask_volume,
        }


class MicroTaiexDashboard:
    """Subscribe to TMF quotes and render a Colab/Jupyter friendly dashboard."""

    def __init__(
        self,
        api: Any,
        contract: Any,
        quote_types: Sequence[str] = DEFAULT_QUOTE_TYPES,
        max_rows: int = 30,
        refresh_interval: float = 0.5,
    ) -> None:
        self.api = api
        self.contract = contract
        self.quote_types = tuple(normalize_quote_type(q) for q in quote_types)
        self.max_rows = max_rows
        self.refresh_interval = refresh_interval
        self.records: deque[QuoteRecord] = deque(maxlen=max_rows)
        self.events: queue.Queue[QuoteRecord] = queue.Queue()
        self._stop_event = threading.Event()
        self._ui_thread: threading.Thread | None = None
        self._previous_close: float | None = None
        self._widgets: dict[str, Any] = {}

    def start(self, display_ui: bool = True) -> "MicroTaiexDashboard":
        """Register callbacks, subscribe quotes, and optionally display widgets."""

        self._register_callbacks()
        for quote_type in self.quote_types:
            self.api.subscribe(self.contract, quote_type=subscribe_quote_arg(quote_type))

        if display_ui:
            self._display_widgets()
            self._ui_thread = threading.Thread(target=self._refresh_loop, daemon=True)
            self._ui_thread.start()
        return self

    def stop(self) -> None:
        """Unsubscribe quotes and close the Shioaji connection."""

        self._stop_event.set()
        for quote_type in self.quote_types:
            try:
                self.api.unsubscribe(self.contract, quote_type=subscribe_quote_arg(quote_type))
            except Exception as exc:  # noqa: BLE001 - best effort cleanup
                print(f"unsubscribe {quote_type} failed: {exc}", file=sys.stderr)
        try:
            self.api.logout()
        except Exception as exc:  # noqa: BLE001 - best effort cleanup
            print(f"logout failed: {exc}", file=sys.stderr)

    def print_forever(self) -> None:
        """Print quote events until interrupted. Useful outside notebooks."""

        print("Subscribed. Press Ctrl+C to stop.")
        try:
            while not self._stop_event.is_set():
                record = self.events.get(timeout=0.5)
                print(record.as_row())
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            self.stop()

    def _register_callbacks(self) -> None:
        if "Tick" in self.quote_types:
            decorator = getattr(self.api, "on_tick_fop_v1", None)
            if decorator is not None:
                @decorator()
                def on_tick(_exchange: Any, tick: Any) -> None:
                    self._handle_quote("tick", tick)

        if "BidAsk" in self.quote_types:
            decorator = getattr(self.api, "on_bidask_fop_v1", None)
            if decorator is not None:
                @decorator()
                def on_bidask(_exchange: Any, bidask: Any) -> None:
                    self._handle_quote("bid_ask", bidask)

        if "Quote" in self.quote_types:
            decorator = getattr(self.api, "on_quote_fop_v1", None)
            if decorator is not None:
                @decorator()
                def on_quote(_exchange: Any, quote: Any) -> None:
                    self._handle_quote("quote", quote)

    def _handle_quote(self, quote_type: str, payload: Any) -> None:
        record = normalize_quote_payload(quote_type, payload)
        self.records.appendleft(record)
        self.events.put(record)

    def _display_widgets(self) -> None:
        try:
            import pandas as pd
            import ipywidgets as widgets
            from IPython.display import display
        except ImportError as exc:  # pragma: no cover - depends on notebook env
            raise RuntimeError(
                "Dashboard UI requires ipywidgets, pandas, and IPython. "
                "Install with: pip install ipywidgets pandas"
            ) from exc

        self._widgets["pd"] = pd
        title = widgets.HTML("<h2>微型臺指期貨（TMF）即時報價</h2>")
        status = widgets.HTML("<b>狀態：</b>已訂閱，等待報價...")
        last_price = widgets.HTML("<h1 style='margin:0'>--</h1>")
        meta = widgets.HTML("契約：--　時間：--　量：--")
        bidask = widgets.HTML("買：-- / --　賣：-- / --")
        table = widgets.Output()
        box = widgets.VBox([title, status, last_price, meta, bidask, table])
        self._widgets.update(
            {
                "status": status,
                "last_price": last_price,
                "meta": meta,
                "bidask": bidask,
                "table": table,
            }
        )
        display(box)

    def _refresh_loop(self) -> None:
        while not self._stop_event.is_set():
            self._render_once()
            time.sleep(self.refresh_interval)

    def _render_once(self) -> None:
        if not self.records or not self._widgets:
            return

        pd = self._widgets["pd"]
        latest = self.records[0]
        price = coerce_float(latest.close)
        color = "black"
        arrow = ""
        if price is not None and self._previous_close is not None:
            if price > self._previous_close:
                color = "#d62728"
                arrow = "▲"
            elif price < self._previous_close:
                color = "#2ca02c"
                arrow = "▼"
        if price is not None:
            self._previous_close = price

        self._widgets["status"].value = (
            f"<b>狀態：</b>接收中　最後更新：{latest.received_at}　"
            f"訂閱：{', '.join(self.quote_types)}"
        )
        self._widgets["last_price"].value = (
            f"<h1 style='margin:0;color:{color}'>{latest.close or '--'} {arrow}</h1>"
        )
        self._widgets["meta"].value = (
            f"契約：{latest.code or '--'}　日期：{latest.date or '--'}　"
            f"時間：{latest.time or '--'}　量：{latest.volume or '--'}"
        )
        self._widgets["bidask"].value = (
            f"買：{latest.bid_price or '--'} / {latest.bid_volume or '--'}　"
            f"賣：{latest.ask_price or '--'} / {latest.ask_volume or '--'}"
        )
        dataframe = pd.DataFrame([record.as_row() for record in self.records])
        output = self._widgets["table"]
        output.clear_output(wait=True)
        with output:
            from IPython.display import display

            display(dataframe)


def login_api(api_key: str, secret_key: str, simulation: bool = True, fetch_contract: bool = True) -> Any:
    """Create and login a Shioaji API instance."""

    import shioaji as sj

    api = sj.Shioaji(simulation=simulation)
    api.login(api_key=api_key, secret_key=secret_key, fetch_contract=fetch_contract)
    return api


def run_colab_dashboard(
    api_key: str,
    secret_key: str,
    contract_code: str = DEFAULT_CONTRACT_CODE,
    quote_types: Sequence[str] = DEFAULT_QUOTE_TYPES,
    simulation: bool = True,
    max_rows: int = 30,
) -> MicroTaiexDashboard:
    """Login, resolve a TMF contract, and start the notebook dashboard."""

    patch_notebook_event_loop()
    api = login_api(api_key, secret_key, simulation=simulation, fetch_contract=True)
    contract = resolve_futures_contract(api, contract_code)
    dashboard = MicroTaiexDashboard(api, contract, quote_types=quote_types, max_rows=max_rows)
    return dashboard.start(display_ui=True)


def patch_notebook_event_loop() -> None:
    """Apply nest_asyncio when available; harmless outside Colab/Jupyter."""

    try:
        import nest_asyncio
    except ImportError:
        return
    nest_asyncio.apply()


def resolve_futures_contract(api: Any, contract_code: str = DEFAULT_CONTRACT_CODE) -> Any:
    """Resolve TMF continuous or monthly futures contracts from Shioaji's tree."""

    code = contract_code.upper()
    futures = api.Contracts.Futures
    product = code[:3]
    candidates = [
        lambda: getattr(getattr(futures, product), code),
        lambda: getattr(futures, product)[code],
        lambda: futures[product][code],
        lambda: futures[code],
    ]
    for getter in candidates:
        try:
            contract = getter()
        except Exception:  # noqa: BLE001 - try next contract-tree shape
            continue
        if contract is not None:
            return contract

    available = list_product_contracts(api, product, print_result=False)
    preview = ", ".join(available[:20]) or "no contracts found"
    raise ValueError(
        f"Cannot resolve futures contract {code!r}. Available {product} contracts: {preview}"
    )


def list_product_contracts(api: Any, product: str = "TMF", print_result: bool = True) -> list[str]:
    """Return available contract codes under a futures product node."""

    product = product.upper()
    try:
        node = getattr(api.Contracts.Futures, product)
    except Exception:  # noqa: BLE001
        try:
            node = api.Contracts.Futures[product]
        except Exception:  # noqa: BLE001
            node = None

    codes: list[str] = []
    if node is not None:
        for name in dir(node):
            if name.startswith(product):
                codes.append(name)
        if hasattr(node, "keys"):
            try:
                codes.extend(str(key) for key in node.keys())
            except Exception:  # noqa: BLE001
                pass
    codes = sorted(set(codes))
    if print_result:
        print("\n".join(codes))
    return codes


def subscribe_quote_arg(canonical_quote_type: str) -> str:
    """Return the string value accepted by Shioaji's subscribe API."""

    return {"Tick": "tick", "BidAsk": "bid_ask", "Quote": "quote"}[canonical_quote_type]


def normalize_quote_type(quote_type: str) -> str:
    key = quote_type.strip().lower()
    if key not in QUOTE_TYPE_ALIASES:
        raise ValueError(f"Unsupported quote type: {quote_type}")
    return QUOTE_TYPE_ALIASES[key]


def normalize_quote_payload(quote_type: str, payload: Any) -> QuoteRecord:
    data = payload_to_dict(payload)
    bid_prices = first_present(data, "bid_price", "bid_prices", "bid_price1")
    bid_volumes = first_present(data, "bid_volume", "bid_volumes", "bid_volume1")
    ask_prices = first_present(data, "ask_price", "ask_prices", "ask_price1")
    ask_volumes = first_present(data, "ask_volume", "ask_volumes", "ask_volume1")
    return QuoteRecord(
        received_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        quote_type=quote_type,
        code=str(first_present(data, "code", default="")),
        date=str(first_present(data, "date", default="")),
        time=str(first_present(data, "time", "datetime", "ts", default="")),
        close=first_present(data, "close", "price", "last_price", default=""),
        volume=first_present(data, "volume", "total_volume", "tick_volume", default=""),
        bid_price=first_item(bid_prices),
        bid_volume=first_item(bid_volumes),
        ask_price=first_item(ask_prices),
        ask_volume=first_item(ask_volumes),
        raw=data,
    )


def payload_to_dict(payload: Any) -> dict[str, Any]:
    if isinstance(payload, Mapping):
        return dict(payload)
    if hasattr(payload, "dict"):
        try:
            return dict(payload.dict())
        except Exception:  # noqa: BLE001
            pass
    if hasattr(payload, "model_dump"):
        try:
            return dict(payload.model_dump())
        except Exception:  # noqa: BLE001
            pass
    data: dict[str, Any] = {}
    for name in dir(payload):
        if name.startswith("_"):
            continue
        try:
            value = getattr(payload, name)
        except Exception:  # noqa: BLE001
            continue
        if callable(value):
            continue
        data[name] = value
    return data


def first_present(data: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        value = data.get(key)
        if value not in (None, ""):
            return value
    return default


def first_item(value: Any) -> Any:
    if value in (None, ""):
        return ""
    if isinstance(value, (str, bytes)):
        return value
    if isinstance(value, Iterable):
        try:
            return next(iter(value))
        except StopIteration:
            return ""
    return value


def coerce_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Realtime Micro TAIEX Futures quote dashboard")
    parser.add_argument("--contract", default=DEFAULT_CONTRACT_CODE, help="TMF contract code, e.g. TMFR1 or TMF202606")
    parser.add_argument(
        "--quote-type",
        action="append",
        default=[],
        help="Quote type: tick, bid_ask, or quote. Can be provided multiple times.",
    )
    parser.add_argument("--live", action="store_true", help="Use live trading environment instead of simulation")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    api_key = os.environ.get("SINOPAC_API_KEY")
    secret_key = os.environ.get("SINOPAC_SECRET_KEY")
    if not api_key or not secret_key:
        print("Please set SINOPAC_API_KEY and SINOPAC_SECRET_KEY environment variables.", file=sys.stderr)
        return 2

    api = login_api(api_key, secret_key, simulation=not args.live, fetch_contract=True)
    contract = resolve_futures_contract(api, args.contract)
    quote_types = args.quote_type or list(DEFAULT_QUOTE_TYPES)
    dashboard = MicroTaiexDashboard(api, contract, quote_types=quote_types).start(display_ui=False)
    signal.signal(signal.SIGTERM, lambda _signum, _frame: dashboard.stop())
    dashboard.print_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
