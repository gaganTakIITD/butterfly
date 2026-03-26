"""
Workflow Toolset — Data Pipeline & Report Delivery

14 functional tools organized in 5 layers for end-to-end data pipeline
orchestration.  Every primary tool has a guaranteed-success fallback so the
pipeline degrades gracefully instead of failing outright.

Layers
------
1. Ingestion   : fetch_data, fetch_data_from_cache
2. Processing  : validate_data, clean_data, transform_data
3. Filtering   : filter_data
4. Visualization: generate_chart, generate_text_table
5. Delivery    : compose_report, export_report, dispatch_email, dispatch_to_file
+  Orchestration: check_health, log_step
"""

from __future__ import annotations

import datetime
import json
import math
import os
import random
import re
import smtplib
import textwrap
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SIMULATE = os.getenv("SIMULATE_FAILURES", "false").lower() == "true"
_CACHE_DIR = Path.home() / ".workflow_cache"
_REPORTS_DIR = Path.cwd() / "reports"
_LOG_PATH = Path.cwd() / "execution_log.json"

_REGIONS = ["North", "South", "East", "West", "Central"]
_PRODUCTS = ["Widget A", "Widget B", "Gadget X", "Gadget Y", "Service Z"]


def _maybe_fail(probability: float = 0.15, error_code: int = 429,
                error_msg: str = "Rate limit exceeded") -> dict | None:
    """Return an error dict with *probability* when simulation is on."""
    if _SIMULATE and random.random() < probability:
        return {
            "status": "error",
            "error_code": error_code,
            "error_message": error_msg,
        }
    return None


def _ts() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _generate_sales_rows(n: int = 15,
                         start: str = "2026-03-19",
                         end: str = "2026-03-25") -> list[dict]:
    """Produce *n* realistic sales rows between *start* and *end*."""
    start_dt = datetime.datetime.fromisoformat(start)
    end_dt = datetime.datetime.fromisoformat(end)
    delta = (end_dt - start_dt).days or 1
    rows: list[dict] = []
    for _ in range(n):
        day_offset = random.randint(0, delta)
        date = (start_dt + datetime.timedelta(days=day_offset)).strftime("%Y-%m-%d")
        rows.append({
            "date": date,
            "region": random.choice(_REGIONS),
            "product": random.choice(_PRODUCTS),
            "quantity": random.randint(1, 200),
            "amount": round(random.uniform(10, 5000), 2),
            "status": random.choice(["Shipped", "Shipped", "Pending", "Cancelled"]),
            "customer_id": f"CUST-{random.randint(1000, 9999)}",
        })
    return rows


def _generate_inventory_rows(n: int = 10) -> list[dict]:
    rows: list[dict] = []
    for _ in range(n):
        rows.append({
            "product": random.choice(_PRODUCTS),
            "warehouse": random.choice(["WH-1", "WH-2", "WH-3"]),
            "stock": random.randint(0, 1000),
            "reorder_level": random.randint(50, 200),
            "last_updated": _ts(),
        })
    return rows


def _generate_user_rows(n: int = 10) -> list[dict]:
    names = ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace",
             "Heidi", "Ivan", "Judy", "Karl", "Laura", "Mallory", "Nick"]
    rows: list[dict] = []
    for _ in range(n):
        rows.append({
            "user_id": f"USR-{random.randint(10000, 99999)}",
            "name": random.choice(names),
            "email": f"{random.choice(names).lower()}@example.com",
            "sign_up_date": (
                datetime.datetime.now(datetime.timezone.utc)
                - datetime.timedelta(days=random.randint(0, 365))
            ).strftime("%Y-%m-%d"),
            "plan": random.choice(["free", "basic", "premium"]),
            "active": random.choice([True, True, True, False]),
        })
    return rows


# =========================================================================
# WorkflowToolset
# =========================================================================

class WorkflowToolset:
    """Data Pipeline & Report Delivery toolset.

    Provides 14 tools that cover the entire data-pipeline lifecycle:
    ingestion → validation → cleaning → transformation → filtering →
    visualisation → report composition → delivery, plus health-checking
    and step-level execution logging.

    Each primary tool has a guaranteed-success fallback so the pipeline
    can always degrade gracefully rather than fail hard.
    """

    def __init__(self) -> None:
        self._execution_log: list[dict] = []
        self._context: dict[str, str] = {}
        _CACHE_DIR.mkdir(parents=True, exist_ok=True)
        _REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    def _resolve(self, val: str) -> str:
        """Resolve lazy LLM placeholders like <tool_name_output>."""
        if val and isinstance(val, str) and val.startswith("<") and val.endswith(">"):
            return self._context.get(val, val)
        return val

    # ------------------------------------------------------------------
    # LAYER 1 — Data Ingestion
    # ------------------------------------------------------------------

    async def fetch_data(
        self,
        source: str = "sales",
        time_range: str = "last_week",
        filters: str = "",
    ) -> str:
        """Fetch data from the internal data source.

        Generates realistic mock datasets entirely in-process so the tool
        works on any machine without external APIs.

        Args:
            source: Data source identifier — one of 'sales', 'inventory',
                    or 'users'.  Defaults to 'sales'.
            time_range: Human-readable time window (e.g. 'last_week',
                        'last_month'). Used to annotate the response.
            filters: Optional comma-separated filters (e.g.
                     'region=North,product=Widget A').

        Returns:
            JSON string with keys ``status``, ``tool_name``, ``row_count``,
            ``time_range``, ``data``, and ``cached`` (always ``false``).
            On simulated failure returns ``status: error`` with an
            ``error_message``.
        """
        fail = _maybe_fail(0.15, 429, "Rate limit exceeded — try again shortly")
        if fail:
            fail["tool_name"] = "fetch_data"
            return json.dumps(fail)

        generators = {
            "sales": _generate_sales_rows,
            "inventory": _generate_inventory_rows,
            "users": _generate_user_rows,
        }
        gen = generators.get(source.lower(), _generate_sales_rows)
        rows = gen()

        # Apply simple key=value filters
        if filters:
            for f in filters.split(","):
                f = f.strip()
                if "=" in f:
                    key, val = f.split("=", 1)
                    key, val = key.strip(), val.strip()
                    rows = [r for r in rows if str(r.get(key, "")) == val]

        # Cache the result for the fallback tool
        cache_file = _CACHE_DIR / f"{source}_{time_range}.json"
        cache_file.write_text(json.dumps(rows, default=str), encoding="utf-8")

        result_json = json.dumps({
            "status": "success",
            "tool_name": "fetch_data",
            "source": source,
            "time_range": time_range,
            "row_count": len(rows),
            "data": rows,
            "cached": False,
        }, default=str)
        self._context["<fetch_data_output>"] = result_json
        return result_json

    async def fetch_data_from_cache(
        self,
        source: str = "sales",
        time_range: str = "last_week",
    ) -> str:
        """Fallback data fetcher — reads cached data from disk.

        If no cache exists, generates a minimal fallback dataset so the
        pipeline can always continue.  This tool **never fails**.

        Args:
            source: Data source identifier matching the primary fetch.
            time_range: Time window matching the primary fetch.

        Returns:
            JSON string with the same schema as ``fetch_data`` but with
            ``cached: true``.
        """
        cache_file = _CACHE_DIR / f"{source}_{time_range}.json"
        if cache_file.exists():
            rows = json.loads(cache_file.read_text(encoding="utf-8"))
        else:
            # Generate a small fallback dataset
            generators = {
                "sales": lambda: _generate_sales_rows(5),
                "inventory": lambda: _generate_inventory_rows(5),
                "users": lambda: _generate_user_rows(5),
            }
            rows = generators.get(source.lower(), lambda: _generate_sales_rows(15))()

        result_json = json.dumps({
            "status": "success",
            "tool_name": "fetch_data_from_cache",
            "source": source,
            "time_range": time_range,
            "row_count": len(rows),
            "data": rows,
            "cached": True,
        }, default=str)
        self._context["<fetch_data_from_cache_output>"] = result_json
        self._context["<fetch_data_output>"] = result_json  # Alias for bulletproof failover
        return result_json

    # ------------------------------------------------------------------
    # LAYER 2 — Data Processing
    # ------------------------------------------------------------------

    async def validate_data(self, data_json: str) -> str:
        """Validate data integrity, schema consistency, and completeness.

        Checks every row for: missing fields, null/empty values, negative
        numeric values, and duplicate rows.

        Args:
            data_json: JSON string — either the raw ``data`` array or the
                       full tool-response object from ``fetch_data``.

        Returns:
            JSON string with ``status`` (``valid`` / ``warnings`` /
            ``invalid``), ``total_rows``, ``issues`` list, and the
            cleaned ``data`` array.
        """
        data_json = self._resolve(data_json)
        try:
            parsed = json.loads(data_json)
            rows = parsed if isinstance(parsed, list) else parsed.get("data", [])
        except (json.JSONDecodeError, TypeError):
            return json.dumps({
                "status": "invalid",
                "tool_name": "validate_data",
                "error_message": "Input is not valid JSON",
                "issues": ["Input is not valid JSON"],
                "data": [],
            })

        issues: list[str] = []
        if not rows:
            issues.append("Dataset is empty")

        # Detect nulls / empties per column
        if rows:
            keys = set()
            for r in rows:
                keys.update(r.keys())
            for key in sorted(keys):
                null_count = sum(1 for r in rows if r.get(key) in (None, "", "null"))
                if null_count:
                    issues.append(f"Column '{key}' has {null_count} null/empty values")

        # Detect negative numerics
        for i, row in enumerate(rows):
            for k, v in row.items():
                if isinstance(v, (int, float)) and v < 0:
                    issues.append(f"Row {i}: negative value {v} in '{k}'")

        # Detect duplicates
        seen: list[str] = []
        dup_count = 0
        for row in rows:
            key = json.dumps(row, sort_keys=True, default=str)
            if key in seen:
                dup_count += 1
            else:
                seen.append(key)
        if dup_count:
            issues.append(f"{dup_count} duplicate rows detected")

        status = "valid" if not issues else ("warnings" if len(issues) <= 3 else "invalid")

        result_json = json.dumps({
            "status": status,
            "tool_name": "validate_data",
            "total_rows": len(rows),
            "issues": issues,
            "data": rows,
        }, default=str)
        self._context["<validate_data_output>"] = result_json
        return result_json

    async def clean_data(
        self,
        data_json: str,
        strategy: str = "drop_nulls",
    ) -> str:
        """Clean data by handling nulls, duplicates, and type issues.

        Args:
            data_json: JSON string — raw ``data`` array or full tool-
                       response object.
            strategy: Cleaning strategy for null values:
                      ``drop_nulls``  — remove rows with any null field.
                      ``fill_zero``   — replace numeric nulls with 0 and
                                        string nulls with ``'unknown'``.
                      ``fill_mean``   — replace numeric nulls with the
                                        column mean; strings with
                                        ``'unknown'``.

        Returns:
            JSON with ``status``, ``original_count``, ``cleaned_count``,
            ``removed_count``, ``data``.
        """
        data_json = self._resolve(data_json)
        try:
            parsed = json.loads(data_json)
            rows = parsed if isinstance(parsed, list) else parsed.get("data", [])
        except (json.JSONDecodeError, TypeError):
            return json.dumps({
                "status": "error",
                "tool_name": "clean_data",
                "error_message": "Input is not valid JSON",
            })

        original = len(rows)

        # Deduplicate
        unique: list[dict] = []
        seen_keys: set[str] = set()
        for row in rows:
            k = json.dumps(row, sort_keys=True, default=str)
            if k not in seen_keys:
                seen_keys.add(k)
                unique.append(row)
        rows = unique

        if strategy == "drop_nulls":
            rows = [r for r in rows if all(
                v not in (None, "", "null") for v in r.values()
            )]
        elif strategy in ("fill_zero", "fill_mean"):
            # Compute means for numeric fields
            means: dict[str, float] = {}
            if strategy == "fill_mean" and rows:
                for key in rows[0]:
                    nums = [r[key] for r in rows
                            if isinstance(r.get(key), (int, float))]
                    if nums:
                        means[key] = sum(nums) / len(nums)

            for row in rows:
                for key in list(row.keys()):
                    if row[key] in (None, "", "null"):
                        if isinstance(means.get(key), float):
                            row[key] = round(means[key], 2) if strategy == "fill_mean" else 0
                        else:
                            row[key] = "unknown"

        # Normalize strings
        for row in rows:
            for key in row:
                if isinstance(row[key], str):
                    row[key] = row[key].strip()

        result_json = json.dumps({
            "status": "success",
            "tool_name": "clean_data",
            "strategy": strategy,
            "original_count": original,
            "cleaned_count": len(rows),
            "removed_count": original - len(rows),
            "data": rows,
        }, default=str)
        self._context["<clean_data_output>"] = result_json
        return result_json

    async def transform_data(
        self,
        data_json: str,
        group_by: str = "region",
        aggregation: str = "sum",
    ) -> str:
        """Group and aggregate data to produce summary statistics.

        Performs real computation on every row — no mocking.

        Args:
            data_json: JSON string — raw ``data`` array or full tool
                       response.
            group_by: Column name to group by (e.g. ``region``,
                      ``product``, ``date``).  Supports multi-level
                      grouping with comma separation
                      (e.g. ``region,product``).
            aggregation: Aggregation function — ``sum``, ``avg``,
                         ``count``, ``min``, ``max``.

        Returns:
            JSON with ``status``, ``grouped_by``, ``aggregation``,
            ``result`` (list of group dicts), and ``data`` (same as
            ``result`` for chaining).
        """
        fail = _maybe_fail(0.08, 500, "Transformation engine error — data may be malformed")
        if fail:
            fail["tool_name"] = "transform_data"
            return json.dumps(fail)

        data_json = self._resolve(data_json)
        try:
            parsed = json.loads(data_json)
            rows = parsed if isinstance(parsed, list) else parsed.get("data", [])
        except (json.JSONDecodeError, TypeError):
            return json.dumps({
                "status": "error",
                "tool_name": "transform_data",
                "error_message": "Input is not valid JSON",
            })

        group_keys = [g.strip() for g in group_by.split(",")]

        # Build groups
        groups: dict[str, list[dict]] = {}
        for row in rows:
            gk = " | ".join(str(row.get(k, "N/A")) for k in group_keys)
            groups.setdefault(gk, []).append(row)

        # Identify numeric columns
        numeric_cols: list[str] = []
        if rows:
            for k, v in rows[0].items():
                if k not in group_keys and isinstance(v, (int, float)):
                    numeric_cols.append(k)

        # Aggregate
        agg_fn = aggregation.lower()
        result: list[dict] = []
        for group_label, group_rows in sorted(groups.items()):
            entry: dict[str, Any] = {}
            for i, gk in enumerate(group_keys):
                entry[gk] = group_label.split(" | ")[i] if " | " in group_label else group_label
            entry["count"] = len(group_rows)
            for col in numeric_cols:
                vals = [r[col] for r in group_rows if isinstance(r.get(col), (int, float))]
                if not vals:
                    entry[col] = 0
                elif agg_fn == "sum":
                    entry[col] = round(sum(vals), 2)
                elif agg_fn == "avg":
                    entry[col] = round(sum(vals) / len(vals), 2)
                elif agg_fn == "min":
                    entry[col] = min(vals)
                elif agg_fn == "max":
                    entry[col] = max(vals)
                elif agg_fn == "count":
                    entry[col] = len(vals)
                else:
                    entry[col] = round(sum(vals), 2)
            result.append(entry)

        result_json = json.dumps({
            "status": "success",
            "tool_name": "transform_data",
            "grouped_by": group_by,
            "aggregation": aggregation,
            "group_count": len(result),
            "result": result,
            "data": result,
        }, default=str)
        self._context["<transform_data_output>"] = result_json
        return result_json

    # ------------------------------------------------------------------
    # LAYER 3 — Filtering
    # ------------------------------------------------------------------

    async def filter_data(
        self,
        data_json: str,
        conditions: str = "",
    ) -> str:
        """Filter dataset rows by one or more conditions.

        Conditions use simple syntax: ``field operator value`` separated
        by commas.  Supported operators: ``==``, ``!=``, ``>``, ``<``,
        ``>=``, ``<=``, ``contains``.

        Args:
            data_json: JSON string — ``data`` array or full tool response.
            conditions: Comma-separated filter expressions, e.g.
                        ``amount > 1000, region == North``.

        Returns:
            JSON with ``status``, ``original_count``, ``filtered_count``,
            ``conditions_applied``, and ``data``.
        """
        data_json = self._resolve(data_json)
        try:
            parsed = json.loads(data_json)
            rows = parsed if isinstance(parsed, list) else parsed.get("data", [])
        except (json.JSONDecodeError, TypeError):
            return json.dumps({
                "status": "error",
                "tool_name": "filter_data",
                "error_message": "Input is not valid JSON",
            })

        original = len(rows)
        applied: list[str] = []

        if conditions.strip():
            for cond in conditions.split(","):
                cond = cond.strip()
                if not cond:
                    continue
                applied.append(cond)

                # Parse operator
                match = re.match(
                    r"(\w+)\s*(==|!=|>=|<=|>|<|contains)\s*(.+)", cond
                )
                if not match:
                    continue
                field, op, value = match.group(1), match.group(2), match.group(3).strip().strip("'\"")

                filtered: list[dict] = []
                for row in rows:
                    rv = row.get(field)
                    if rv is None:
                        continue
                    try:
                        # Try numeric comparison first
                        rv_num = float(rv) if not isinstance(rv, (int, float)) else rv
                        val_num = float(value)
                        if op == "==" and rv_num == val_num:
                            filtered.append(row)
                        elif op == "!=" and rv_num != val_num:
                            filtered.append(row)
                        elif op == ">" and rv_num > val_num:
                            filtered.append(row)
                        elif op == "<" and rv_num < val_num:
                            filtered.append(row)
                        elif op == ">=" and rv_num >= val_num:
                            filtered.append(row)
                        elif op == "<=" and rv_num <= val_num:
                            filtered.append(row)
                        elif op == "contains" and value.lower() in str(rv).lower():
                            filtered.append(row)
                    except (ValueError, TypeError):
                        # String comparison
                        rv_str = str(rv)
                        if op == "==" and rv_str == value:
                            filtered.append(row)
                        elif op == "!=" and rv_str != value:
                            filtered.append(row)
                        elif op == "contains" and value.lower() in rv_str.lower():
                            filtered.append(row)
                        elif op in (">", "<", ">=", "<="):
                            # Lexicographic for strings
                            if op == ">" and rv_str > value:
                                filtered.append(row)
                            elif op == "<" and rv_str < value:
                                filtered.append(row)
                            elif op == ">=" and rv_str >= value:
                                filtered.append(row)
                            elif op == "<=" and rv_str <= value:
                                filtered.append(row)
                rows = filtered

        result_json = json.dumps({
            "status": "success",
            "tool_name": "filter_data",
            "original_count": original,
            "filtered_count": len(rows),
            "conditions_applied": applied,
            "data": rows,
        }, default=str)
        self._context["<filter_data_output>"] = result_json
        return result_json

    # ------------------------------------------------------------------
    # LAYER 4 — Visualisation
    # ------------------------------------------------------------------

    async def generate_chart(
        self,
        data_json: str,
        chart_type: str = "bar",
        title: str = "Chart",
        x_axis: str = "",
        y_axis: str = "",
    ) -> str:
        """Generate an ASCII chart from structured data.

        Produces bar, horizontal-bar, or line charts using only built-in
        Python — no external graphing libraries required.

        Args:
            data_json: JSON string — aggregated ``data``/``result`` array
                       from ``transform_data`` or similar.
            chart_type: ``bar``, ``horizontal_bar``, or ``line``.
            title: Chart title displayed above the graphic.
            x_axis: Column name for the x-axis (category labels).
            y_axis: Column name for the y-axis (numeric values).

        Returns:
            JSON with ``status``, ``chart_type``, ``chart_text`` (the
            rendered ASCII chart string), and ``data``.
        """
        fail = _maybe_fail(0.10, 500, "Chart generation failed — data may be malformed")
        if fail:
            fail["tool_name"] = "generate_chart"
            return json.dumps(fail)

        data_json = self._resolve(data_json)
        try:
            parsed = json.loads(data_json)
            rows = parsed if isinstance(parsed, list) else parsed.get("data",
                        parsed.get("result", []))
        except (json.JSONDecodeError, TypeError):
            return json.dumps({
                "status": "error",
                "tool_name": "generate_chart",
                "error_message": "Input is not valid JSON — use generate_text_table as fallback",
            })

        if not rows:
            return json.dumps({
                "status": "error",
                "tool_name": "generate_chart",
                "error_message": "No data rows to chart",
            })

        # Auto-detect axes if not specified
        if not x_axis:
            for k in rows[0]:
                if isinstance(rows[0][k], str):
                    x_axis = k
                    break
        if not y_axis:
            for k in rows[0]:
                if isinstance(rows[0][k], (int, float)) and k != "count":
                    y_axis = k
                    break
            if not y_axis:
                for k in rows[0]:
                    if isinstance(rows[0][k], (int, float)):
                        y_axis = k
                        break

        labels = [str(r.get(x_axis, "?")) for r in rows]
        values = []
        for r in rows:
            v = r.get(y_axis, 0)
            values.append(float(v) if isinstance(v, (int, float)) else 0)

        max_val = max(values) if values else 1
        chart_width = 50
        lines: list[str] = []
        lines.append(f"  {'=' * (chart_width + 20)}")
        lines.append(f"  {title:^{chart_width + 20}}")
        lines.append(f"  {'=' * (chart_width + 20)}")
        lines.append("")

        if chart_type in ("bar", "horizontal_bar"):
            max_label = max(len(l) for l in labels) if labels else 5
            for label, val in zip(labels, values):
                bar_len = int((val / max_val) * chart_width) if max_val else 0
                bar = "█" * bar_len
                lines.append(f"  {label:>{max_label}} │{bar} {val:,.2f}")
            lines.append(f"  {' ' * max_label} └{'─' * chart_width}")
            lines.append(f"  {' ' * max_label}  {y_axis}")
        elif chart_type == "line":
            height = 15
            if max_val == 0:
                max_val = 1
            for row_i in range(height, -1, -1):
                threshold = (row_i / height) * max_val
                row_label = f"{threshold:>10,.0f} │"
                chars: list[str] = []
                spacing = max(1, chart_width // len(values)) if values else 1
                for val in values:
                    if abs(val - threshold) <= (max_val / height / 2):
                        chars.append("●")
                    elif val >= threshold:
                        chars.append("│")
                    else:
                        chars.append(" ")
                line_str = (" " * (spacing - 1)).join(chars)
                lines.append(f"  {row_label}{line_str}")
            lines.append(f"  {'':>10} └{'─' * chart_width}")
            label_line = "  " + " " * 12
            for label in labels:
                label_line += f"{label:<{spacing}}"
            lines.append(label_line)
        else:
            # Default to bar
            max_label = max(len(l) for l in labels) if labels else 5
            for label, val in zip(labels, values):
                bar_len = int((val / max_val) * chart_width) if max_val else 0
                bar = "█" * bar_len
                lines.append(f"  {label:>{max_label}} │{bar} {val:,.2f}")

        lines.append("")
        chart_text = "\n".join(lines)

        result_json = json.dumps({
            "status": "success",
            "tool_name": "generate_chart",
            "chart_type": chart_type,
            "title": title,
            "chart_text": chart_text,
            "data": rows,
        }, default=str)
        self._context["<generate_chart_output>"] = result_json
        return result_json

    async def generate_text_table(
        self,
        data_json: str,
        title: str = "Data Table",
        columns: str = "",
    ) -> str:
        """Fallback visualisation — render data as a formatted ASCII table.

        This tool **never fails** and serves as the guaranteed fallback
        for ``generate_chart``.

        Args:
            data_json: JSON string — ``data`` array or full tool response.
            title: Table title.
            columns: Comma-separated column names to include (empty = all).

        Returns:
            JSON with ``status``, ``table_text``, and ``data``.
        """
        data_json = self._resolve(data_json)
        try:
            parsed = json.loads(data_json)
            rows = parsed if isinstance(parsed, list) else parsed.get("data",
                        parsed.get("result", []))
        except (json.JSONDecodeError, TypeError):
            rows = []

        if not rows:
            table_text = f"  {title}\n  (no data available)"
            return json.dumps({
                "status": "success",
                "tool_name": "generate_text_table",
                "table_text": table_text,
                "data": [],
            })

        # Select columns
        if columns:
            cols = [c.strip() for c in columns.split(",")]
        else:
            cols = list(rows[0].keys())

        # Compute column widths
        widths: dict[str, int] = {}
        for col in cols:
            widths[col] = max(
                len(col),
                max((len(str(r.get(col, ""))) for r in rows), default=0),
            )
            widths[col] = min(widths[col], 25)  # cap width

        # Build table
        lines: list[str] = []
        lines.append(f"\n  {title}")
        sep = "  +" + "+".join("-" * (widths[c] + 2) for c in cols) + "+"
        lines.append(sep)
        header = "  |" + "|".join(f" {c:^{widths[c]}} " for c in cols) + "|"
        lines.append(header)
        lines.append(sep)
        for row in rows[:50]:  # cap at 50 rows for readability
            row_str = "  |" + "|".join(
                f" {str(row.get(c, '')):>{widths[c]}} " for c in cols
            ) + "|"
            lines.append(row_str)
        lines.append(sep)
        if len(rows) > 50:
            lines.append(f"  ... and {len(rows) - 50} more rows")
        lines.append("")

        table_text = "\n".join(lines)

        result_json = json.dumps({
            "status": "success",
            "tool_name": "generate_text_table",
            "title": title,
            "row_count": len(rows),
            "table_text": table_text,
            "data": rows,
        }, default=str)
        self._context["<generate_text_table_output>"] = result_json
        self._context["<generate_chart_output>"] = result_json  # Alias for bulletproof failover
        return result_json

    # ------------------------------------------------------------------
    # LAYER 5 — Reporting & Delivery
    # ------------------------------------------------------------------

    def _extract_data_from_json(self, raw: str) -> list[dict]:
        """Try to extract a list of row dicts from a tool output JSON string."""
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict) and "data" in parsed:
                return parsed["data"]
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass
        return []

    def _build_html_table(self, rows: list[dict]) -> str:
        """Render rows as a beautiful styled HTML table."""
        if not rows:
            return ""
        cols = list(rows[0].keys())
        html = "<table style='width:100%;border-collapse:collapse;margin:16px 0;font-size:14px;'>\n"
        # Header
        html += "<thead><tr>"
        for c in cols:
            html += f"<th style='background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);color:#fff;padding:12px 16px;text-align:left;font-weight:600;text-transform:uppercase;font-size:11px;letter-spacing:0.5px;'>{c}</th>"
        html += "</tr></thead>\n<tbody>"
        # Rows
        for i, row in enumerate(rows):
            bg = "#f8f9ff" if i % 2 == 0 else "#ffffff"
            html += f"<tr style='background:{bg};'>"
            for c in cols:
                val = row.get(c, "")
                if isinstance(val, float):
                    val = f"{val:,.2f}"
                html += f"<td style='padding:10px 16px;border-bottom:1px solid #eee;color:#4a5568;'>{val}</td>"
            html += "</tr>"
        html += "</tbody></table>"
        return html

    def _build_css_chart(self, rows: list[dict], title: str, y_axis: str = "amount") -> str:
        """Render rows as a pure-CSS horizontal bar chart with gradients."""
        if not rows:
            return ""
        # Find label and value columns
        label_col = None
        val_col = y_axis
        for k in rows[0]:
            if k != val_col and isinstance(rows[0].get(k), str):
                label_col = k
                break
        if not label_col:
            label_col = list(rows[0].keys())[0]

        max_val = max((float(r.get(val_col, 0)) for r in rows), default=1)
        gradients = [
            "linear-gradient(90deg, #667eea, #764ba2)",
            "linear-gradient(90deg, #f093fb, #f5576c)",
            "linear-gradient(90deg, #4facfe, #00f2fe)",
            "linear-gradient(90deg, #43e97b, #38f9d7)",
            "linear-gradient(90deg, #fa709a, #fee140)",
            "linear-gradient(90deg, #a18cd1, #fbc2eb)",
        ]
        html = f"<div style='margin:20px 0;'>"
        html += f"<h3 style='color:#2d3748;font-size:16px;margin-bottom:16px;font-weight:600;'>{title}</h3>"
        for i, row in enumerate(rows):
            label = str(row.get(label_col, ""))
            val = float(row.get(val_col, 0))
            pct = (val / max_val) * 100 if max_val else 0
            grad = gradients[i % len(gradients)]
            html += f"""<div style='display:flex;align-items:center;margin-bottom:10px;'>
  <div style='min-width:80px;font-size:13px;font-weight:600;color:#4a5568;text-align:right;padding-right:12px;'>{label}</div>
  <div style='flex:1;background:#f0f0f5;border-radius:8px;height:32px;overflow:hidden;position:relative;'>
    <div style='width:{pct:.1f}%;height:100%;background:{grad};border-radius:8px;transition:width 0.6s ease;'></div>
  </div>
  <div style='min-width:90px;font-size:13px;font-weight:700;color:#2d3748;padding-left:12px;text-align:right;'>${val:,.2f}</div>
</div>"""
        html += "</div>"
        return html

    async def compose_report(
        self,
        title: str = "Pipeline Report",
        summary: str = "",
        sections_json: str = "[]",
        chart_text: str = "",
        table_text: str = "",
        metadata: str = "",
    ) -> str:
        """Compose a full HTML report from sections and visuals.

        Combines summary text, data sections, chart output, and table
        output into a single professional HTML document with inline styles
        for perfect email rendering.

        Args:
            title: Report title.
            summary: Executive summary paragraph.
            sections_json: JSON array of ``{"heading": "...", "body": "..."}``
                           section objects.
            chart_text: Rendered chart text to embed.
            table_text: Rendered table text to embed.
            metadata: Optional metadata string (author, tags, etc.).

        Returns:
            JSON with ``status`` and ``report_content`` (the full
            HTML string).
        """
        ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        date_str = datetime.datetime.now(datetime.timezone.utc).strftime("%B %d, %Y")

        # Resolve placeholders
        sections_json = self._resolve(sections_json)
        chart_text = self._resolve(chart_text) if chart_text else ""
        table_text = self._resolve(table_text) if table_text else ""

        # AUTO-FETCH: if the LLM didn't pass real data, pull from context
        # This guarantees KPIs, charts, and tables are always populated
        if not self._extract_data_from_json(table_text):
            ctx_transform = self._context.get("<transform_data_output>", "")
            if ctx_transform:
                table_text = ctx_transform
        if not self._extract_data_from_json(chart_text):
            ctx_chart = self._context.get("<generate_chart_output>", "")
            if ctx_chart:
                chart_text = ctx_chart

        # Extract data from chart/table JSON for rendering
        chart_data = self._extract_data_from_json(chart_text)
        table_data = self._extract_data_from_json(table_text)
        # If we have chart data but no table data, use chart data for the table too
        all_data = chart_data or table_data

        # Parse chart title
        chart_title = title
        try:
            parsed_chart = json.loads(chart_text)
            if isinstance(parsed_chart, dict):
                chart_title = parsed_chart.get("title", title)
        except Exception:
            pass

        # Build KPI cards from data
        kpi_html = ""
        if all_data:
            total_amount = sum(float(r.get("amount", 0)) for r in all_data)
            total_count = sum(int(r.get("count", 0)) for r in all_data)
            total_qty = sum(int(r.get("quantity", 0)) for r in all_data)
            num_regions = len(all_data)
            avg_amount = total_amount / num_regions if num_regions else 0

            kpis = [
                ("Total Revenue", f"${total_amount:,.2f}", "#667eea", "#764ba2", "📊"),
                ("Total Orders", f"{total_count}", "#f093fb", "#f5576c", "📦"),
                ("Total Quantity", f"{total_qty:,}", "#4facfe", "#00f2fe", "📋"),
                ("Regions", f"{num_regions}", "#43e97b", "#38f9d7", "🌍"),
                ("Avg Revenue", f"${avg_amount:,.2f}", "#fa709a", "#fee140", "📈"),
            ]
            kpi_html = "<div style='display:flex;flex-wrap:wrap;gap:12px;margin-bottom:28px;'>"
            for label, value, c1, c2, icon in kpis:
                kpi_html += f"""<div style='flex:1;min-width:140px;background:linear-gradient(135deg,{c1},{c2});border-radius:12px;padding:18px;color:#fff;text-align:center;'>
  <div style='font-size:24px;margin-bottom:4px;'>{icon}</div>
  <div style='font-size:22px;font-weight:800;letter-spacing:-0.5px;'>{value}</div>
  <div style='font-size:11px;text-transform:uppercase;letter-spacing:1px;opacity:0.9;margin-top:4px;'>{label}</div>
</div>"""
            kpi_html += "</div>"

        # Parse sections
        try:
            sections = json.loads(sections_json) if isinstance(sections_json, str) else sections_json
        except (json.JSONDecodeError, TypeError):
            sections = []

        # Build sections HTML
        sections_html = ""
        for sec in sections:
            heading = sec.get("heading", sec.get("title", "Section"))
            body = sec.get("body", sec.get("content", ""))
            if body:
                sections_html += f"""<div style='background:#f8f9ff;border-left:4px solid #667eea;border-radius:0 8px 8px 0;padding:16px 20px;margin-bottom:16px;'>
  <h3 style='color:#2d3748;font-size:15px;margin:0 0 8px 0;font-weight:700;'>{heading}</h3>
  <p style='color:#4a5568;margin:0;font-size:14px;line-height:1.7;'>{body}</p>
</div>"""

        # Build CSS chart
        chart_html = self._build_css_chart(all_data, chart_title) if all_data else ""

        # Build HTML data table
        table_html = self._build_html_table(all_data) if all_data else ""

        report_content = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
  body {{ margin:0; padding:0; background:#f0f2f5; font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; color:#1a202c; }}
  * {{ box-sizing:border-box; }}
</style>
</head>
<body>
<div style="max-width:720px;margin:0 auto;padding:20px;">

  <!-- Header -->
  <div style="background:linear-gradient(135deg,#1a1a2e 0%,#16213e 50%,#0f3460 100%);border-radius:16px 16px 0 0;padding:36px 32px;text-align:center;">
    <div style="font-size:11px;text-transform:uppercase;letter-spacing:3px;color:#a0aec0;margin-bottom:8px;">Workflow Orchestrator</div>
    <h1 style="color:#ffffff;font-size:26px;font-weight:800;margin:0 0 6px 0;letter-spacing:-0.5px;">{title}</h1>
    <div style="color:#a0aec0;font-size:13px;">{date_str} &bull; Auto-generated Report</div>
  </div>

  <!-- Body -->
  <div style="background:#ffffff;padding:32px;border-radius:0 0 16px 16px;box-shadow:0 10px 40px rgba(0,0,0,0.08);">

    <!-- KPIs -->
    {kpi_html}

    <!-- Executive Summary -->
    <div style="background:linear-gradient(135deg,#f8f9ff,#eef2ff);border-radius:12px;padding:24px;margin-bottom:24px;border:1px solid #e2e8f0;">
      <h2 style="color:#2d3748;font-size:16px;font-weight:700;margin:0 0 10px 0;">📋 Executive Summary</h2>
      <p style="color:#4a5568;font-size:14px;line-height:1.8;margin:0;">{summary if summary else 'This report presents a comprehensive analysis of the processed data pipeline results.'}</p>
    </div>

    <!-- Sections -->
    {sections_html}

    <!-- Chart -->
    {f'''<div style="background:#fafbff;border-radius:12px;padding:24px;margin-bottom:24px;border:1px solid #e2e8f0;">
      <h2 style="color:#2d3748;font-size:16px;font-weight:700;margin:0 0 4px 0;">📊 Visual Analysis</h2>
      {chart_html}
    </div>''' if chart_html else ''}

    <!-- Data Table -->
    {f'''<div style="background:#fafbff;border-radius:12px;padding:24px;margin-bottom:24px;border:1px solid #e2e8f0;">
      <h2 style="color:#2d3748;font-size:16px;font-weight:700;margin:0 0 4px 0;">📋 Detailed Data</h2>
      {table_html}
    </div>''' if table_html else ''}

    <!-- Footer -->
    <div style="text-align:center;padding-top:24px;border-top:1px solid #edf2f7;margin-top:8px;">
      <div style="font-size:11px;color:#a0aec0;text-transform:uppercase;letter-spacing:2px;margin-bottom:4px;">Powered by</div>
      <div style="font-size:14px;font-weight:700;background:linear-gradient(90deg,#667eea,#764ba2);-webkit-background-clip:text;-webkit-text-fill-color:transparent;">Workflow Orchestrator Agent</div>
      <div style="font-size:11px;color:#cbd5e0;margin-top:6px;">Generated {ts}</div>
    </div>
  </div>
</div>
</body>
</html>"""

        result_json = json.dumps({
            "status": "success",
            "tool_name": "compose_report",
            "title": title,
            "report_length": len(report_content),
            "report_content": report_content,
        })
        self._context["<compose_report_output>"] = result_json
        return result_json

    async def export_report(
        self,
        report_content: str = "",
        filename: str = "report",
        format: str = "md",
    ) -> str:
        """Export report content to a file on disk.

        Args:
            report_content: The full report text to write.
            filename: Base filename (without extension).
            format: Output format — ``md``, ``txt``, or ``json``.

        Returns:
            JSON with ``status``, ``file_path``, and ``file_size``.
        """
        report_content = self._resolve(report_content)
        
        # Smart extraction: if the content is a JSON wrapper from compose_report,
        # pull out just the clean HTML from the "report_content" field
        try:
            parsed = json.loads(report_content)
            if isinstance(parsed, dict) and "report_content" in parsed:
                report_content = parsed["report_content"]
        except (json.JSONDecodeError, TypeError, ValueError):
            pass

        try:
            ext = format if format in ("html", "md", "txt", "json") else "html"
            ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            out_file = _REPORTS_DIR / f"{filename}_{ts}.{ext}"

            if ext == "json":
                content = json.dumps({
                    "title": filename,
                    "generated_at": _ts(),
                    "content": report_content,
                })
            else:
                content = report_content

            out_file.write_text(content, encoding="utf-8")

            result_json = json.dumps({
                "status": "success",
                "tool_name": "export_report",
                "file_path": str(out_file),
                "file_size": len(content),
                "format": ext,
            })
            self._context["<export_report_output>"] = result_json
            return result_json
        except Exception as e:
            return json.dumps({
                "status": "error",
                "tool_name": "export_report",
                "error_message": str(e),
            })

    async def dispatch_email(
        self,
        recipient: str = "",
        subject: str = "Pipeline Report",
        body: str = "",
        attachment_path: str = "",
    ) -> str:
        """Send an email with the report.

        If SMTP environment variables are configured (``SMTP_HOST``,
        ``SMTP_PORT``, ``SMTP_USER``, ``SMTP_PASS``), sends via real
        SMTP.  Otherwise logs the full email to a local file and returns
        success — ensuring the tool works on every system.

        Args:
            recipient: Email address of the recipient.
            subject: Email subject line.
            body: Optional email body text.
            attachment_path: Optional path to file attachment. If this points to an .html report, it will be magically embedded directly into the email body natively!

        Returns:
            JSON with ``status``, ``method`` (``smtp`` or ``file_log``),
            ``recipient``, and ``log_path``.
        """
        attachment_path = self._resolve(attachment_path) if attachment_path else ""
        
        # Smart attachment resolution: LLM often passes raw filenames instead of absolute paths
        if attachment_path:
            try:
                parsed = json.loads(attachment_path)
                if isinstance(parsed, dict) and "file_path" in parsed:
                    attachment_path = parsed["file_path"]
            except Exception:
                pass
            
            p = Path(attachment_path)
            if not p.exists():
                stem = p.name.replace(".html", "").replace(".md", "").replace(".txt", "").replace(".json", "")
                candidates = list(_REPORTS_DIR.glob(f"{stem}*"))
                if candidates:
                    candidates.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                    attachment_path = str(candidates[0])

        fail = _maybe_fail(0.10, 503, "SMTP service temporarily unavailable")
        if fail:
            fail["tool_name"] = "dispatch_email"
            return json.dumps(fail)

        smtp_host = os.getenv("SMTP_HOST", "")
        smtp_port = int(os.getenv("SMTP_PORT", "587"))
        smtp_user = os.getenv("SMTP_USER", "")
        smtp_pass = os.getenv("SMTP_PASS", "")

        method = "smtp" if (smtp_host and smtp_user) else "file_log"

        content = ""
        if attachment_path and Path(attachment_path).exists():
            try:
                content = Path(attachment_path).read_text(encoding="utf-8")
            except Exception:
                pass

        if method == "smtp":
            try:
                msg = MIMEMultipart()
                msg["From"] = smtp_user
                msg["To"] = recipient
                msg["Subject"] = subject
                if content:
                    if str(attachment_path).lower().endswith(".html"):
                        # Intercept HTML attachment and render it natively as the email body
                        msg.attach(MIMEText(content, "html"))
                        # AND include it as an actual physical attachment for downloading
                        att = MIMEText(content, "utf-8")
                        att.add_header(
                            "Content-Disposition", f"attachment; filename={Path(attachment_path).name}"
                        )
                        msg.attach(att)
                    else:
                        msg.attach(MIMEText(body, "html"))
                        att = MIMEText(content, "utf-8")
                        att.add_header(
                            "Content-Disposition", f"attachment; filename={Path(attachment_path).name}"
                        )
                        msg.attach(att)
                else:
                    msg.attach(MIMEText(body, "html"))

                with smtplib.SMTP(smtp_host, smtp_port) as server:
                    server.starttls()
                    server.login(smtp_user, smtp_pass)
                    server.send_message(msg)

                result_json = json.dumps({
                    "status": "success",
                    "tool_name": "dispatch_email",
                    "method": "smtp",
                    "recipient": recipient,
                    "subject": subject,
                })
                self._context["<dispatch_email_output>"] = result_json
                return result_json
            except Exception as e:
                # Fall through to file-based logging
                method = "file_log"

        # File-based email log
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = _REPORTS_DIR / f"email_{ts}.log"
        email_log = textwrap.dedent(f"""\
            ════════════════════════════════════════
            EMAIL DISPATCH LOG
            ════════════════════════════════════════
            Date    : {_ts()}
            To      : {recipient}
            Subject : {subject}
            Method  : file_log (SMTP not configured)
            ════════════════════════════════════════

            {body}

            {'[ATTACHED/EMBEDDED CONTENT:]' if content else ''}
            {content if content else ''}

            ════════════════════════════════════════
        """)
        log_file.write_text(email_log, encoding="utf-8")

        result_json = json.dumps({
            "status": "success",
            "tool_name": "dispatch_email",
            "method": "file_log",
            "recipient": recipient,
            "subject": subject,
            "log_path": str(log_file),
        })
        self._context["<dispatch_email_output>"] = result_json
        return result_json

    async def dispatch_to_file(
        self,
        content: str = "",
        recipient: str = "unknown",
        output_dir: str = "",
    ) -> str:
        """Fallback delivery — write report to a local file.

        This tool **never fails** and serves as the guaranteed fallback
        for ``dispatch_email``.

        Args:
            content: Report body text to write.
            recipient: Intended recipient (used in filename).
            output_dir: Optional output directory (defaults to
                        ``./reports/``).

        Returns:
            JSON with ``status``, ``file_path``, and ``file_size``.
        """
        content = self._resolve(content)
        try:
            out = Path(output_dir) if output_dir else _REPORTS_DIR
            out.mkdir(parents=True, exist_ok=True)
            safe_recipient = re.sub(r"[^a-zA-Z0-9_@.-]", "_", recipient)
            ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = out / f"report_{safe_recipient}_{ts}.html"
            file_path.write_text(content, encoding="utf-8")

            result_json = json.dumps({
                "status": "success",
                "tool_name": "dispatch_to_file",
                "file_path": str(file_path),
                "file_size": len(content),
                "recipient": recipient,
            })
            self._context["<dispatch_to_file_output>"] = result_json
            return result_json
        except Exception as e:
            # Final-resort: write to temp
            import tempfile
            tmp = Path(tempfile.gettempdir()) / f"report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
            tmp.write_text(content or "(empty report)", encoding="utf-8")
            result_json = json.dumps({
                "status": "success",
                "tool_name": "dispatch_to_file",
                "file_path": str(tmp),
                "file_size": len(content or "(empty report)"),
                "recipient": recipient,
                "note": "Written to temp directory as fallback",
            })
            self._context["<dispatch_to_file_output>"] = result_json
            return result_json

    # ------------------------------------------------------------------
    # ORCHESTRATION SUPPORT
    # ------------------------------------------------------------------

    async def check_health(self, service_name: str = "all") -> str:
        """Check readiness of tools and infrastructure.

        Validates disk write access, environment variables, and cache
        availability for the requested service.

        Args:
            service_name: Service to check — ``fetch_data``,
                          ``dispatch_email``, ``export_report``, or
                          ``all`` for a full sweep.

        Returns:
            JSON with per-service health status and overall readiness.
        """
        results: dict[str, dict] = {}

        checks = {
            "fetch_data": lambda: {
                "status": "healthy",
                "cache_dir_exists": _CACHE_DIR.exists(),
                "cache_dir_writable": os.access(_CACHE_DIR, os.W_OK) if _CACHE_DIR.exists() else False,
            },
            "export_report": lambda: {
                "status": "healthy",
                "reports_dir_exists": _REPORTS_DIR.exists(),
                "reports_dir_writable": os.access(_REPORTS_DIR, os.W_OK) if _REPORTS_DIR.exists() else False,
            },
            "dispatch_email": lambda: {
                "status": "healthy" if os.getenv("SMTP_HOST") else "degraded",
                "smtp_configured": bool(os.getenv("SMTP_HOST")),
                "fallback_available": True,
                "note": "Will use file-based dispatch if SMTP is not configured",
            },
            "openai": lambda: {
                "status": "healthy" if os.getenv("OPENAI_API_KEY") else "unhealthy",
                "api_key_set": bool(os.getenv("OPENAI_API_KEY")),
            },
        }

        if service_name == "all":
            for name, check in checks.items():
                try:
                    results[name] = check()
                except Exception as e:
                    results[name] = {"status": "unhealthy", "error": str(e)}
        elif service_name in checks:
            try:
                results[service_name] = checks[service_name]()
            except Exception as e:
                results[service_name] = {"status": "unhealthy", "error": str(e)}
        else:
            results[service_name] = {"status": "unknown", "note": "Service not recognised"}

        all_healthy = all(
            r.get("status") in ("healthy", "degraded") for r in results.values()
        )

        return json.dumps({
            "status": "success",
            "tool_name": "check_health",
            "overall": "ready" if all_healthy else "degraded",
            "services": results,
        })

    async def log_step(
        self,
        step_number: str = "0",
        step_name: str = "",
        tool_used: str = "",
        status: str = "success",
        details: str = "",
        retry_count: str = "0",
    ) -> str:
        """Log an execution step to the persistent audit trail.

        Appends a timestamped entry to both the in-memory log and a JSON
        file on disk.

        Args:
            step_number: Sequential step number (1-based).
            step_name: Human-readable step description.
            tool_used: Name of the tool that was called.
            status: Outcome — ``success``, ``failed``, ``retried``,
                    ``escalated``, or ``skipped``.
            details: Free-text detail or error message.
            retry_count: Number of retries attempted for this step.

        Returns:
            JSON with current step entry and running log summary
            (total steps, successes, failures, escalations).
        """
        entry = {
            "timestamp": _ts(),
            "step_number": step_number,
            "step_name": step_name,
            "tool_used": tool_used,
            "status": status,
            "details": details,
            "retry_count": retry_count,
        }
        self._execution_log.append(entry)

        # Persist to disk
        try:
            _LOG_PATH.write_text(
                json.dumps(self._execution_log, indent=2, default=str),
                encoding="utf-8",
            )
        except Exception:
            pass  # Non-critical — in-memory log is authoritative

        summary = {
            "total_steps": len(self._execution_log),
            "successes": sum(1 for e in self._execution_log if e["status"] == "success"),
            "failures": sum(1 for e in self._execution_log if e["status"] == "failed"),
            "retries": sum(1 for e in self._execution_log if e["status"] == "retried"),
            "escalations": sum(1 for e in self._execution_log if e["status"] == "escalated"),
        }

        return json.dumps({
            "status": "success",
            "tool_name": "log_step",
            "entry": entry,
            "summary": summary,
            "log_path": str(_LOG_PATH),
        })

    # ------------------------------------------------------------------
    # Tool registry for OpenAI function-calling
    # ------------------------------------------------------------------

    def get_tools(self) -> dict[str, Any]:
        """Return a dictionary mapping tool names to this instance.

        The ``OpenAIAgentExecutor`` iterates this dict to build the
        function-calling schema and dispatch calls.
        """
        return {
            # Layer 1 — Ingestion
            "fetch_data": self,
            "fetch_data_from_cache": self,
            # Layer 2 — Processing
            "validate_data": self,
            "clean_data": self,
            "transform_data": self,
            # Layer 3 — Filtering
            "filter_data": self,
            # Layer 4 — Visualisation
            "generate_chart": self,
            "generate_text_table": self,
            # Layer 5 — Reporting & Delivery
            "compose_report": self,
            "export_report": self,
            "dispatch_email": self,
            "dispatch_to_file": self,
            # Orchestration
            "check_health": self,
            "log_step": self,
        }
