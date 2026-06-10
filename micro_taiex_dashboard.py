"""Realtime Micro TAIEX Futures quote dashboard for SinoPac Shioaji.

The module is intentionally notebook friendly: import ``run_colab_dashboard`` in
Google Colab or Jupyter to start an ipywidgets dashboard, or execute this file
as a CLI to print incoming quotes in a terminal.
"""

from __future__ import annotations

import argparse
import html
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
        quote_version: str = "v1",
    ) -> None:
        self.api = api
        self.contract = contract
        self.quote_types = tuple(normalize_quote_type(q) for q in quote_types)
        self.max_rows = max_rows
        self.refresh_interval = refresh_interval
        self.quote_version = quote_version
        self.records: deque[QuoteRecord] = deque(maxlen=max_rows)
        self.events: queue.Queue[QuoteRecord] = queue.Queue()
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._ui_thread: threading.Thread | None = None
        self._previous_close: float | None = None
        self._latest_close: Any = ""
        self._latest_volume: Any = ""
        self._latest_trade_time: Any = ""
        self._latest_trade_date: Any = ""
        self._latest_code: Any = ""
        self._latest_bid_price: Any = ""
        self._latest_bid_volume: Any = ""
        self._latest_ask_price: Any = ""
        self._latest_ask_volume: Any = ""
        self._latest_reference_price: float | None = None
        self._latest_reference_source = ""
        self._previous_reference_price: float | None = None
        self._price_history: deque[tuple[str, float, str]] = deque(maxlen=max_rows)
        self._callback_refs: dict[str, Any] = {}
        self._subscription_errors: list[str] = []
        self._widgets: dict[str, Any] = {}

    def start(self, display_ui: bool = True) -> "MicroTaiexDashboard":
        """Register callbacks, subscribe quotes, and optionally display widgets."""

        if display_ui:
            self._display_widgets()

        self._register_callbacks()
        for quote_type in self.quote_types:
            try:
                self.api.subscribe(self.contract, **subscribe_kwargs(quote_type, self.quote_version))
            except Exception as exc:  # noqa: BLE001 - surface all subscription failures in the UI
                self._subscription_errors.append(f"{quote_type}: {exc}")

        if display_ui:
            self._render_subscription_status()
            self._ui_thread = threading.Thread(target=self._refresh_loop, daemon=True)
            self._ui_thread.start()
        return self

    def stop(self) -> None:
        """Unsubscribe quotes and close the Shioaji connection."""

        self._stop_event.set()
        for quote_type in self.quote_types:
            try:
                self.api.unsubscribe(self.contract, **subscribe_kwargs(quote_type, self.quote_version))
            except Exception as exc:  # noqa: BLE001 - best effort cleanup
                print(f"unsubscribe {quote_type} failed: {exc}", file=sys.stderr)
        for quote_type in self.quote_types:
            clear_name = callback_clear_method_name(quote_type)
            clear_callback = getattr(self.api, clear_name, None)
            if clear_callback is not None:
                try:
                    clear_callback()
                except Exception as exc:  # noqa: BLE001 - best effort cleanup
                    print(f"clear {quote_type} callback failed: {exc}", file=sys.stderr)
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
        """Register persistent Shioaji callbacks for the selected FOP quote streams."""

        callback_specs = {
            "Tick": ("set_on_tick_fop_v1_callback", "on_tick_fop_v1", "tick"),
            "BidAsk": ("set_on_bidask_fop_v1_callback", "on_bidask_fop_v1", "bid_ask"),
            "Quote": ("set_on_quote_fop_v1_callback", "on_quote_fop_v1", "quote"),
        }
        for canonical_quote_type in self.quote_types:
            setter_name, decorator_name, event_type = callback_specs[canonical_quote_type]

            def callback(_exchange: Any, payload: Any, event_type: str = event_type) -> None:
                self._handle_quote(event_type, payload)

            self._callback_refs[canonical_quote_type] = callback
            setter = getattr(self.api, setter_name, None)
            if setter is not None:
                setter(callback)
                continue

            decorator = getattr(self.api, decorator_name, None)
            if decorator is not None:
                decorator()(callback)

    def _handle_quote(self, quote_type: str, payload: Any) -> None:
        record = normalize_quote_payload(quote_type, payload)
        with self._lock:
            self.records.appendleft(record)
            self._remember_quote_state(record)
        self.events.put(record)
        self._render_once()

    def _remember_quote_state(self, record: QuoteRecord) -> None:
        """Keep the latest trade and bid/ask values across mixed quote events."""

        if record.code:
            self._latest_code = record.code
        if record.close not in (None, ""):
            self._latest_close = record.close
            self._latest_trade_time = record.time
            self._latest_trade_date = record.date
            self._latest_volume = record.volume
        if record.bid_price not in (None, ""):
            self._latest_bid_price = record.bid_price
            self._latest_bid_volume = record.bid_volume
        if record.ask_price not in (None, ""):
            self._latest_ask_price = record.ask_price
            self._latest_ask_volume = record.ask_volume

        reference_price, reference_source = quote_reference_price(record)
        if reference_price is not None:
            self._latest_reference_price = reference_price
            self._latest_reference_source = reference_source
            self._price_history.append((record.received_at, reference_price, reference_source))

    def _display_widgets(self) -> None:
        try:
            import ipywidgets as widgets
            from IPython.display import display
        except ImportError as exc:  # pragma: no cover - depends on notebook env
            raise RuntimeError(
                "Dashboard UI requires ipywidgets and IPython. "
                "Install with: pip install ipywidgets"
            ) from exc

        title = widgets.HTML("<h2>微型臺指期貨（TMF）即時報價</h2>")
        status = widgets.HTML("<b>狀態：</b>已訂閱，等待報價...")
        last_price = widgets.HTML("<h1 style='margin:0'>--</h1>")
        meta = widgets.HTML("契約：--　時間：--　量：--")
        bidask = widgets.HTML("買：-- / --　賣：-- / --")
        change_panel = widgets.HTML("<b>即時價格變化：</b>等待報價...")
        table = widgets.HTML("")
        box = widgets.VBox([title, status, last_price, meta, bidask, change_panel, table])
        self._widgets.update(
            {
                "status": status,
                "last_price": last_price,
                "meta": meta,
                "bidask": bidask,
                "change_panel": change_panel,
                "table": table,
            }
        )
        display(box)

    def _render_subscription_status(self) -> None:
        if not self._widgets:
            return
        contract_code = getattr(self.contract, "code", "") or str(self.contract)
        if self._subscription_errors:
            self._widgets["status"].value = (
                "<b>狀態：</b>訂閱失敗　"
                f"契約：{html.escape(str(contract_code))}　"
                f"錯誤：{html.escape('; '.join(self._subscription_errors))}"
            )
            return
        self._widgets["status"].value = (
            "<b>狀態：</b>已訂閱，等待第一筆報價...　"
            f"契約：{html.escape(str(contract_code))}　"
            f"訂閱：{html.escape(', '.join(self.quote_types))}　版本：{html.escape(str(self.quote_version))}"
        )

    def _refresh_loop(self) -> None:
        while not self._stop_event.is_set():
            self._render_once()
            time.sleep(self.refresh_interval)

    def _render_once(self) -> None:
        if not self._widgets:
            return

        with self._lock:
            if not self.records:
                return
            latest = self.records[0]
            rows = [record.as_row() for record in self.records]
            display_close = self._latest_close
            display_volume = self._latest_volume
            display_code = self._latest_code or latest.code
            display_date = self._latest_trade_date or latest.date
            display_time = self._latest_trade_time or latest.time
            bid_price = self._latest_bid_price
            bid_volume = self._latest_bid_volume
            ask_price = self._latest_ask_price
            ask_volume = self._latest_ask_volume
            reference_price = self._latest_reference_price
            reference_source = self._latest_reference_source
            price_history = list(self._price_history)

        price = coerce_float(display_close)
        movement_price = reference_price if reference_price is not None else price
        color = "black"
        arrow = ""
        delta = None
        if movement_price is not None and self._previous_reference_price is not None:
            delta = movement_price - self._previous_reference_price
            if delta > 0:
                color = "#d62728"
                arrow = "▲"
            elif delta < 0:
                color = "#2ca02c"
                arrow = "▼"
        if movement_price is not None:
            self._previous_reference_price = movement_price
        if price is not None:
            self._previous_close = price

        self._widgets["status"].value = (
            f"<b>狀態：</b>接收中　最後更新：{html.escape(str(latest.received_at))}　"
            f"訂閱：{html.escape(', '.join(self.quote_types))}　版本：{html.escape(str(self.quote_version))}"
        )
        primary_price = display_close if display_close not in (None, "") else format_price(reference_price)
        self._widgets["last_price"].value = (
            f"<h1 style='margin:0;color:{color}'>{html.escape(str(primary_price or '--'))} {arrow}</h1>"
        )
        self._widgets["meta"].value = (
            f"契約：{html.escape(str(display_code or '--'))}　日期：{html.escape(str(display_date or '--'))}　"
            f"時間：{html.escape(str(display_time or '--'))}　量：{html.escape(str(display_volume or '--'))}"
        )
        self._widgets["bidask"].value = (
            f"買：{html.escape(str(bid_price or '--'))} / {html.escape(str(bid_volume or '--'))}　"
            f"賣：{html.escape(str(ask_price or '--'))} / {html.escape(str(ask_volume or '--'))}"
        )
        self._widgets["change_panel"].value = render_price_change_panel(
            trade_price=display_close,
            reference_price=reference_price,
            reference_source=reference_source,
            delta=delta,
            arrow=arrow,
            color=color,
            price_history=price_history,
        )
        self._widgets["table"].value = render_quote_table(rows)


def quote_reference_price(record: QuoteRecord) -> tuple[float | None, str]:
    """Return a realtime reference price from trade price, bid/ask midpoint, bid, or ask."""

    close = coerce_float(record.close)
    if close is not None:
        return close, "成交"

    bid = coerce_float(record.bid_price)
    ask = coerce_float(record.ask_price)
    if bid is not None and ask is not None:
        return (bid + ask) / 2, "買賣中價"
    if bid is not None:
        return bid, "最佳買價"
    if ask is not None:
        return ask, "最佳賣價"
    return None, ""


def format_price(value: Any) -> str:
    price = coerce_float(value)
    if price is None:
        return ""
    if price.is_integer():
        return str(int(price))
    return f"{price:.2f}"


def render_price_change_panel(
    trade_price: Any,
    reference_price: float | None,
    reference_source: str,
    delta: float | None,
    arrow: str,
    color: str,
    price_history: Sequence[tuple[str, float, str]],
) -> str:
    """Render a compact realtime price-change panel with a small sparkline."""

    delta_text = "--" if delta is None else f"{delta:+.2f}"
    latest_reference = format_price(reference_price) or "--"
    latest_trade = html.escape(str(trade_price or "--"))
    source = html.escape(reference_source or "等待報價")
    sparkline = render_sparkline([point[1] for point in price_history])
    return (
        "<div style='border:1px solid #ddd;border-radius:8px;padding:8px;margin:6px 0'>"
        "<div style='font-weight:700;margin-bottom:4px'>即時價格變化</div>"
        "<div style='display:flex;gap:18px;align-items:flex-end;flex-wrap:wrap'>"
        f"<div>參考價<br><span style='font-size:28px;color:{color};font-weight:700'>{html.escape(latest_reference)} {arrow}</span></div>"
        f"<div>變動<br><span style='font-size:20px;color:{color};font-weight:700'>{html.escape(delta_text)}</span></div>"
        f"<div>來源<br><span>{source}</span></div>"
        f"<div>最新成交<br><span>{latest_trade}</span></div>"
        f"<div style='min-width:220px'>{sparkline}</div>"
        "</div></div>"
    )


def render_sparkline(values: Sequence[float], width: int = 220, height: int = 54) -> str:
    if not values:
        return "<span style='color:#777'>等待價格序列...</span>"
    if len(values) == 1:
        y = height / 2
        points = f"0,{y:.1f} {width},{y:.1f}"
    else:
        low = min(values)
        high = max(values)
        span = high - low or 1
        step = width / (len(values) - 1)
        points = " ".join(
            f"{idx * step:.1f},{height - ((value - low) / span * (height - 8) + 4):.1f}"
            for idx, value in enumerate(values)
        )
    last = values[-1]
    first = values[0]
    stroke = "#d62728" if last > first else "#2ca02c" if last < first else "#555"
    return (
        f"<svg width='{width}' height='{height}' viewBox='0 0 {width} {height}' "
        "style='background:#fafafa;border:1px solid #eee'>"
        f"<polyline fill='none' stroke='{stroke}' stroke-width='2' points='{points}'/>"
        "</svg>"
    )


def render_quote_table(rows: Sequence[Mapping[str, Any]]) -> str:
    """Render quote rows as a single HTML value so Colab keeps refreshing it."""

    columns = ["received_at", "type", "code", "date", "time", "close", "volume", "bid", "bid_vol", "ask", "ask_vol"]
    header = "".join(f"<th>{html.escape(column)}</th>" for column in columns)
    body_rows = []
    for row in rows:
        cells = "".join(f"<td>{html.escape(str(row.get(column, '') or ''))}</td>" for column in columns)
        body_rows.append(f"<tr>{cells}</tr>")
    body = "".join(body_rows)
    return (
        "<div style='max-height:420px;overflow:auto'>"
        "<table style='border-collapse:collapse;font-family:monospace;font-size:12px'>"
        "<thead><tr>"
        f"{header}"
        "</tr></thead>"
        f"<tbody>{body}</tbody>"
        "</table></div>"
        "<style>td,th{border:1px solid #ddd;padding:3px 6px;text-align:right}"
        "th{background:#f6f6f6;position:sticky;top:0}</style>"
    )


def login_api(api_key: str, secret_key: str, simulation: bool = True, fetch_contract: bool = True) -> Any:
    """Create and login a Shioaji API instance with API Key and Secret Key."""

    if not api_key or not secret_key:
        raise ValueError("Shioaji login requires both api_key and secret_key.")

    import shioaji as sj

    api = sj.Shioaji(simulation=simulation)
    api.login(api_key=api_key, secret_key=secret_key, fetch_contract=fetch_contract)
    return api


def prompt_for_shioaji_credentials(api_key: str | None = None, secret_key: str | None = None) -> tuple[str, str]:
    """Read Shioaji credentials from arguments, environment variables, or hidden prompts."""

    from getpass import getpass

    resolved_api_key = api_key or os.environ.get("SINOPAC_API_KEY") or os.environ.get("SJ_API_KEY")
    resolved_secret_key = secret_key or os.environ.get("SINOPAC_SECRET_KEY") or os.environ.get("SJ_SEC_KEY")

    if not resolved_api_key:
        resolved_api_key = getpass("永豐 API Key: ")
    if not resolved_secret_key:
        resolved_secret_key = getpass("永豐 Secret Key: ")
    if not resolved_api_key or not resolved_secret_key:
        raise ValueError("請提供永豐 API Key 與 Secret Key；只有其中一個無法登入 Shioaji。")
    return resolved_api_key, resolved_secret_key


def run_colab_dashboard(
    api_key: str | None = None,
    secret_key: str | None = None,
    contract_code: str = DEFAULT_CONTRACT_CODE,
    quote_types: Sequence[str] = DEFAULT_QUOTE_TYPES,
    simulation: bool = True,
    max_rows: int = 30,
    quote_version: str = "v1",
) -> MicroTaiexDashboard:
    """Prompt when needed, login, resolve a TMF contract, and start the notebook dashboard."""

    patch_notebook_event_loop()
    resolved_api_key, resolved_secret_key = prompt_for_shioaji_credentials(api_key, secret_key)
    api = login_api(resolved_api_key, resolved_secret_key, simulation=simulation, fetch_contract=True)
    contract = resolve_futures_contract(api, contract_code)
    dashboard = MicroTaiexDashboard(api, contract, quote_types=quote_types, max_rows=max_rows, quote_version=quote_version)
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
            return resolve_streaming_target_contract(api, contract)

    available = list_product_contracts(api, product, print_result=False)
    preview = ", ".join(available[:20]) or "no contracts found"
    raise ValueError(
        f"Cannot resolve futures contract {code!r}. Available {product} contracts: {preview}"
    )


def resolve_streaming_target_contract(api: Any, contract: Any) -> Any:
    """Use the resolved monthly target when a continuous contract exposes target_code."""

    target_code = str(getattr(contract, "target_code", "") or "").upper()
    source_code = str(getattr(contract, "code", "") or "").upper()
    if not target_code or target_code == source_code:
        return contract

    product = target_code[:3]
    futures = api.Contracts.Futures
    candidates = [
        lambda: getattr(getattr(futures, product), target_code),
        lambda: getattr(futures, product)[target_code],
        lambda: futures[product][target_code],
        lambda: futures[target_code],
    ]
    for getter in candidates:
        try:
            target_contract = getter()
        except Exception:  # noqa: BLE001 - try next contract-tree shape
            continue
        if target_contract is not None:
            return target_contract
    return contract


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


def callback_clear_method_name(canonical_quote_type: str) -> str:
    """Return the Shioaji callback clear method for a canonical FOP quote type."""

    return {
        "Tick": "clear_on_tick_fop_v1_callback",
        "BidAsk": "clear_on_bidask_fop_v1_callback",
        "Quote": "clear_on_quote_fop_v1_callback",
    }[canonical_quote_type]


def subscribe_quote_arg(canonical_quote_type: str) -> Any:
    """Return the quote type accepted by Shioaji's subscribe API."""

    try:
        import shioaji as sj
    except ImportError:
        return {"Tick": "tick", "BidAsk": "bid_ask", "Quote": "quote"}[canonical_quote_type]
    return {"Tick": sj.QuoteType.Tick, "BidAsk": sj.QuoteType.BidAsk, "Quote": sj.QuoteType.Quote}[canonical_quote_type]


def quote_version_arg(version: str = "v1") -> Any:
    """Return the quote version accepted by Shioaji; v1 is required for FOP v1 callbacks."""

    if version.lower() != "v1":
        return version
    try:
        import shioaji as sj
    except ImportError:
        return version
    return sj.QuoteVersion.v1


def subscribe_kwargs(canonical_quote_type: str, version: str = "v1") -> dict[str, Any]:
    """Build subscribe/unsubscribe kwargs with quote_type and callback-compatible version."""

    return {"quote_type": subscribe_quote_arg(canonical_quote_type), "version": quote_version_arg(version)}


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
    api_key = os.environ.get("SINOPAC_API_KEY") or os.environ.get("SJ_API_KEY")
    secret_key = os.environ.get("SINOPAC_SECRET_KEY") or os.environ.get("SJ_SEC_KEY")
    if not api_key or not secret_key:
        print(
            "Please set SINOPAC_API_KEY/SJ_API_KEY and SINOPAC_SECRET_KEY/SJ_SEC_KEY. "
            "Shioaji login requires both API Key and Secret Key.",
            file=sys.stderr,
        )
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
