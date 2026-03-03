#!/usr/bin/env python3
"""Data validation and profiling pipeline using Great Expectations + Pandas.

This script:
1) Runs a small ETL process on a CSV file.
2) Validates transformed data with Great Expectations (when installed).
3) Builds Data Docs and writes violation summaries.
4) Falls back to manual Pandas-based checks when GE is unavailable.
"""

from __future__ import annotations

import argparse
import html as html_lib
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import pandas as pd


def reset_gx_uncommitted_artifacts(gx_dir: Path) -> None:
    """Remove run-specific GX artifacts so each run starts clean."""
    uncommitted_dir = gx_dir / "uncommitted"
    for artifact_dir in ["validations", "data_docs"]:
        target = uncommitted_dir / artifact_dir
        if target.exists():
            shutil.rmtree(target)


def relativize_paths_in_obj(value: Any, project_root: Path) -> Any:
    """Replace absolute machine paths with repository-relative paths."""
    project_root_str = str(project_root)
    project_root_uri = f"file://{project_root_str}"

    if isinstance(value, dict):
        return {k: relativize_paths_in_obj(v, project_root) for k, v in value.items()}
    if isinstance(value, list):
        return [relativize_paths_in_obj(item, project_root) for item in value]
    if isinstance(value, str):
        if value.startswith(project_root_uri):
            file_path = value[len("file://") :]
            try:
                relative = Path(file_path).resolve().relative_to(project_root)
                return relative.as_posix()
            except Exception:
                return value
        if value.startswith(project_root_str):
            try:
                relative = Path(value).resolve().relative_to(project_root)
                return relative.as_posix()
            except Exception:
                return value
        return value
    return value


def sanitize_checkpoint_config_paths(
    gx_dir: Path,
    checkpoint_name: str,
    project_root: Path,
) -> None:
    """Rewrite absolute runtime path in checkpoint YAML to a relative path."""
    checkpoint_file = gx_dir / "checkpoints" / f"{checkpoint_name}.yml"
    if not checkpoint_file.exists():
        return

    checkpoint_text = checkpoint_file.read_text(encoding="utf-8")
    project_root_prefix = f"{project_root.as_posix()}/"
    sanitized = checkpoint_text.replace(project_root_prefix, "")
    checkpoint_file.write_text(sanitized, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ETL + Great Expectations validation")
    parser.add_argument("--input", default="data/sample_sales.csv", help="Input CSV path")
    parser.add_argument(
        "--processed-output",
        default="output/clean_sales.csv",
        help="Path to write transformed CSV",
    )
    parser.add_argument("--reports-dir", default="reports", help="Directory for JSON/MD reports")
    parser.add_argument("--docs-dir", default="data_docs", help="Directory for docs landing page")
    parser.add_argument(
        "--gx-dir",
        default="great_expectations",
        help="Great Expectations project directory",
    )
    parser.add_argument(
        "--suite-name",
        default="sales_data_suite",
        help="Expectation suite name",
    )
    parser.add_argument(
        "--checkpoint-name",
        default="sales_data_checkpoint",
        help="Checkpoint name",
    )
    return parser.parse_args()


def to_snake_case(name: str) -> str:
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip())
    return re.sub(r"_+", "_", name).strip("_").lower()


def transform_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    transformed = df.copy()
    transformed.columns = [to_snake_case(col) for col in transformed.columns]

    if "order_date" in transformed.columns:
        transformed["order_date"] = pd.to_datetime(
            transformed["order_date"], errors="coerce"
        )

    numeric_columns = ["quantity", "unit_price", "discount_pct"]
    for col in numeric_columns:
        if col in transformed.columns:
            transformed[col] = pd.to_numeric(transformed[col], errors="coerce")

    if {"quantity", "unit_price"}.issubset(transformed.columns):
        transformed["gross_amount"] = transformed["quantity"] * transformed["unit_price"]

    if {"gross_amount", "discount_pct"}.issubset(transformed.columns):
        transformed["net_amount"] = transformed["gross_amount"] * (
            1 - transformed["discount_pct"] / 100.0
        )

    if "payment_method" in transformed.columns:
        transformed["payment_method"] = transformed["payment_method"].astype("string").str.lower()

    return transformed


def add_expectations(validator: Any) -> None:
    validator.expect_table_row_count_to_be_between(min_value=1, max_value=100000)

    for col in ["order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price"]:
        validator.expect_column_values_to_not_be_null(column=col)

    validator.expect_column_values_to_be_unique(column="order_id")

    validator.expect_column_values_to_be_between(
        column="quantity", min_value=1, max_value=20
    )
    validator.expect_column_values_to_be_between(
        column="unit_price", min_value=1, max_value=5000
    )
    validator.expect_column_values_to_be_between(
        column="discount_pct", min_value=0, max_value=60
    )
    validator.expect_column_values_to_be_in_set(
        column="payment_method", value_set=["card", "upi", "cash"]
    )

    validator.expect_column_values_to_be_in_type_list(
        column="order_id", type_list=["int64", "int32", "float64"]
    )
    validator.expect_column_values_to_be_in_type_list(
        column="customer_id", type_list=["int64", "int32", "float64"]
    )
    validator.expect_column_values_to_be_in_type_list(
        column="product_id", type_list=["int64", "int32", "float64"]
    )
    validator.expect_column_values_to_be_in_type_list(
        column="quantity", type_list=["int64", "int32", "float64"]
    )
    validator.expect_column_values_to_be_in_type_list(
        column="unit_price", type_list=["float64", "int64", "int32"]
    )


def summarize_checkpoint(result_json: dict[str, Any], docs_url: str | None) -> dict[str, Any]:
    run_results = result_json.get("run_results", {})

    evaluated = 0
    successful = 0
    failed_expectations: list[dict[str, Any]] = []

    for run_data in run_results.values():
        validation_result = run_data.get("validation_result", {})
        statistics = validation_result.get("statistics", {})
        evaluated += int(statistics.get("evaluated_expectations", 0))
        successful += int(statistics.get("successful_expectations", 0))

        for item in validation_result.get("results", []):
            if item.get("success"):
                continue
            config = item.get("expectation_config", {})
            kwargs = config.get("kwargs", {})
            result = item.get("result", {})

            failed_expectations.append(
                {
                    "expectation_type": config.get("expectation_type", "unknown"),
                    "column": kwargs.get("column"),
                    "unexpected_count": result.get("unexpected_count"),
                    "sample_unexpected": (result.get("partial_unexpected_list") or [])[:5],
                }
            )

    if evaluated == 0:
        top_stats = result_json.get("statistics", {})
        evaluated = int(top_stats.get("evaluated_expectations", 0))
        successful = int(top_stats.get("successful_expectations", 0))

    success = bool(result_json.get("success", evaluated == successful))

    return {
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "engine": "great_expectations",
        "success": success,
        "evaluated_expectations": evaluated,
        "successful_expectations": successful,
        "failed_expectations_count": max(evaluated - successful, 0),
        "failed_expectations": failed_expectations,
        "data_docs_url": docs_url,
    }


def run_great_expectations(
    clean_csv_path: Path,
    project_root: Path,
    gx_dir: Path,
    suite_name: str,
    checkpoint_name: str,
) -> tuple[dict[str, Any], dict[str, Any], str | None]:
    import great_expectations as gx
    from great_expectations.core.batch import RuntimeBatchRequest
    from great_expectations.data_context import FileDataContext

    gx_config = gx_dir / "great_expectations.yml"
    if not gx_config.exists():
        FileDataContext.create(project_root_dir=str(project_root))

    context = gx.get_context(context_root_dir=str(gx_dir))

    datasource_name = "sales_pandas_datasource"
    datasource_config = {
        "name": datasource_name,
        "class_name": "Datasource",
        "execution_engine": {"class_name": "PandasExecutionEngine"},
        "data_connectors": {
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier_name"],
            }
        },
    }

    try:
        context.add_or_update_datasource(**datasource_config)
    except AttributeError:
        context.add_datasource(**datasource_config)
    except Exception as exc:
        if "already exists" not in str(exc).lower():
            raise

    try:
        context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
    except AttributeError:
        context.create_expectation_suite(
            expectation_suite_name=suite_name, overwrite_existing=True
        )

    batch_request = RuntimeBatchRequest(
        datasource_name=datasource_name,
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="clean_sales_data",
        runtime_parameters={"path": str(clean_csv_path)},
        batch_identifiers={"default_identifier_name": "default_batch"},
    )

    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name=suite_name
    )
    add_expectations(validator)
    validator.save_expectation_suite(discard_failed_expectations=False)

    checkpoint_kwargs = {
        "name": checkpoint_name,
        "validations": [
            {
                "batch_request": batch_request,
                "expectation_suite_name": suite_name,
            }
        ],
    }

    try:
        context.add_or_update_checkpoint(**checkpoint_kwargs)
    except AttributeError:
        context.add_checkpoint(**checkpoint_kwargs)

    checkpoint_result = context.run_checkpoint(checkpoint_name=checkpoint_name)
    context.build_data_docs()

    docs_sites = context.get_docs_sites_urls() or []
    docs_url = docs_sites[0].get("site_url") if docs_sites else None

    checkpoint_json = checkpoint_result.to_json_dict()
    summary = summarize_checkpoint(checkpoint_json, docs_url)
    return summary, checkpoint_json, docs_url


def run_manual_validation(df: pd.DataFrame) -> dict[str, Any]:
    expectations = 0
    failures: list[dict[str, Any]] = []

    def add_failure(name: str, column: str | None, detail: str) -> None:
        failures.append(
            {
                "expectation_type": name,
                "column": column,
                "detail": detail,
            }
        )

    for col in ["order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price"]:
        expectations += 1
        null_count = int(df[col].isna().sum()) if col in df.columns else len(df)
        if null_count > 0:
            add_failure(
                "expect_column_values_to_not_be_null",
                col,
                f"Found {null_count} null values",
            )

    expectations += 1
    if "order_id" in df.columns:
        duplicate_count = int(df.duplicated(subset=["order_id"]).sum())
        if duplicate_count > 0:
            add_failure(
                "expect_column_values_to_be_unique",
                "order_id",
                f"Found {duplicate_count} duplicate order_id values",
            )
    else:
        add_failure(
            "expect_column_values_to_be_unique",
            "order_id",
            "Missing required column",
        )

    expectations += 1
    if "quantity" in df.columns:
        invalid_quantity = int(((df["quantity"] < 1) | (df["quantity"] > 20)).sum())
        if invalid_quantity > 0:
            add_failure(
                "expect_column_values_to_be_between",
                "quantity",
                f"Found {invalid_quantity} values outside [1, 20]",
            )
    else:
        add_failure(
            "expect_column_values_to_be_between",
            "quantity",
            "Missing required column",
        )

    expectations += 1
    if "unit_price" in df.columns:
        invalid_unit_price = int(((df["unit_price"] < 1) | (df["unit_price"] > 5000)).sum())
        if invalid_unit_price > 0:
            add_failure(
                "expect_column_values_to_be_between",
                "unit_price",
                f"Found {invalid_unit_price} values outside [1, 5000]",
            )
    else:
        add_failure(
            "expect_column_values_to_be_between",
            "unit_price",
            "Missing required column",
        )

    expectations += 1
    if "discount_pct" in df.columns:
        invalid_discount = int(((df["discount_pct"] < 0) | (df["discount_pct"] > 60)).sum())
        if invalid_discount > 0:
            add_failure(
                "expect_column_values_to_be_between",
                "discount_pct",
                f"Found {invalid_discount} values outside [0, 60]",
            )
    else:
        add_failure(
            "expect_column_values_to_be_between",
            "discount_pct",
            "Missing required column",
        )

    expectations += 1
    if "payment_method" in df.columns:
        invalid_payment_mask = ~df["payment_method"].isin(["card", "upi", "cash"])
        invalid_payment_values = (
            df.loc[invalid_payment_mask, "payment_method"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )
        if invalid_payment_values:
            add_failure(
                "expect_column_values_to_be_in_set",
                "payment_method",
                f"Invalid payment_method values: {sorted(invalid_payment_values)}",
            )
    else:
        add_failure(
            "expect_column_values_to_be_in_set",
            "payment_method",
            "Missing required column",
        )

    successful = expectations - len(failures)

    return {
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "engine": "manual_fallback",
        "success": len(failures) == 0,
        "evaluated_expectations": expectations,
        "successful_expectations": successful,
        "failed_expectations_count": len(failures),
        "failed_expectations": failures,
        "data_docs_url": "data_docs/index.html",
    }


def write_summary_reports(summary: dict[str, Any], reports_dir: Path) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)

    summary_json = reports_dir / "validation_summary.json"
    summary_md = reports_dir / "validation_summary.md"

    summary_json.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

    lines = [
        "# Validation Summary",
        "",
        f"- Run timestamp (UTC): {summary['run_timestamp_utc']}",
        f"- Engine: {summary['engine']}",
        f"- Overall success: {summary['success']}",
        f"- Evaluated expectations: {summary['evaluated_expectations']}",
        f"- Successful expectations: {summary['successful_expectations']}",
        f"- Failed expectations: {summary['failed_expectations_count']}",
        f"- Data docs URL: {summary.get('data_docs_url')}",
        "",
        "## Failed Expectations",
    ]

    if summary["failed_expectations"]:
        for item in summary["failed_expectations"]:
            detail = item.get("detail")
            if detail is None:
                detail = (
                    f"unexpected_count={item.get('unexpected_count')}, "
                    f"sample_unexpected={item.get('sample_unexpected')}"
                )
            lines.append(
                f"- `{item.get('expectation_type')}` on `{item.get('column')}` -> {detail}"
            )
    else:
        lines.append("- No validation failures.")

    summary_md.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_manual_data_docs(
    df: pd.DataFrame,
    summary: dict[str, Any],
    docs_dir: Path,
    ge_docs_url: str | None = None,
) -> None:
    docs_dir.mkdir(parents=True, exist_ok=True)
    project_root = docs_dir.parent

    try:
        profile_df = df.describe(include="all", datetime_is_numeric=True)
    except TypeError:
        profile_df = df.describe(include="all")
    profile_df = profile_df.transpose().reset_index()
    profile_df = profile_df.rename(columns={"index": "column"})
    sample_df = df.head(50).copy()

    ge_docs_href = None
    if ge_docs_url and ge_docs_url.startswith("file://"):
        parsed = urlparse(ge_docs_url)
        local_index = Path(unquote(parsed.path))
        if local_index.exists():
            source_dir = local_index.parent
            target_dir = docs_dir / "local_site"
            if target_dir.exists():
                shutil.rmtree(target_dir)
            shutil.copytree(source_dir, target_dir)
            # Remove machine-specific absolute paths from copied GX docs.
            root_prefix = f"{project_root.as_posix()}/"
            file_prefix = f"file://{root_prefix}"
            for file_path in target_dir.rglob("*"):
                if not file_path.is_file() or file_path.suffix.lower() not in {
                    ".html",
                    ".json",
                    ".js",
                    ".txt",
                }:
                    continue
                try:
                    content = file_path.read_text(encoding="utf-8")
                except UnicodeDecodeError:
                    continue
                cleaned = content.replace(file_prefix, "").replace(root_prefix, "")
                if cleaned != content:
                    file_path.write_text(cleaned, encoding="utf-8")
            ge_docs_href = "local_site/index.html"

    if ge_docs_href is None and ge_docs_url:
        ge_docs_href = ge_docs_url

    def check_label(expectation_type: str) -> str:
        mapping = {
            "expect_column_values_to_be_unique": "Duplicate value found",
            "expect_column_values_to_not_be_null": "Missing required value",
            "expect_column_values_to_be_between": "Value outside allowed range",
            "expect_column_values_to_be_in_set": "Invalid category value",
            "expect_column_values_to_be_in_type_list": "Data type mismatch",
            "expect_table_row_count_to_be_between": "Unexpected row count",
        }
        return mapping.get(expectation_type, expectation_type.replace("_", " ").title())

    def priority(expectation_type: str, column: str | None) -> str:
        if expectation_type == "expect_column_values_to_be_unique":
            return "P1"
        if expectation_type == "expect_column_values_to_not_be_null" and column in {
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
        }:
            return "P1"
        if expectation_type in {
            "expect_column_values_to_be_between",
            "expect_column_values_to_be_in_set",
        }:
            return "P2"
        return "P3"

    def action_text(expectation_type: str, column: str | None) -> str:
        if expectation_type == "expect_column_values_to_be_unique":
            return "Keep one `order_id`; block duplicates."
        if expectation_type == "expect_column_values_to_not_be_null":
            return f"Fill or drop blank `{column}`."
        if expectation_type == "expect_column_values_to_be_between":
            return f"Fix `{column}` outside limits."
        if expectation_type == "expect_column_values_to_be_in_set":
            return f"Map `{column}` to card/upi/cash."
        if expectation_type == "expect_column_values_to_be_in_type_list":
            return f"Cast `{column}` to right type."
        return "Check source rules."

    def impact_text(expectation_type: str) -> str:
        if expectation_type == "expect_column_values_to_be_unique":
            return "Wrong totals."
        if expectation_type == "expect_column_values_to_not_be_null":
            return "Incomplete records."
        if expectation_type == "expect_column_values_to_be_between":
            return "Skewed metrics/models."
        if expectation_type == "expect_column_values_to_be_in_set":
            return "Broken grouping."
        if expectation_type == "expect_column_values_to_be_in_type_list":
            return "ETL errors."
        return "Data risk."

    def format_cell_value(value: Any) -> str:
        if isinstance(value, pd.Timestamp):
            return value.strftime("%Y-%m-%d")
        try:
            if pd.isna(value):
                return "NULL"
        except Exception:
            pass
        return str(value)

    def infer_failed_rows(expectation_type: str, column: str | None) -> tuple[list[int], set[str]]:
        if expectation_type == "expect_column_values_to_not_be_null" and column in df.columns:
            mask = df[column].isna()
            return [int(i) for i in df.index[mask].tolist()], {column}

        if expectation_type == "expect_column_values_to_be_unique":
            unique_col = column if column in df.columns else ("order_id" if "order_id" in df.columns else None)
            if unique_col:
                mask = df.duplicated(subset=[unique_col], keep=False)
                return [int(i) for i in df.index[mask].tolist()], {unique_col}
            return [], set()

        if expectation_type == "expect_column_values_to_be_between" and column in df.columns:
            bounds = {
                "quantity": (1, 20),
                "unit_price": (1, 5000),
                "discount_pct": (0, 60),
            }
            if column in bounds:
                min_value, max_value = bounds[column]
                mask = (df[column] < min_value) | (df[column] > max_value)
                return [int(i) for i in df.index[mask].tolist()], {column}
            return [], set()

        if expectation_type == "expect_column_values_to_be_in_set" and column in df.columns:
            allowed_values = {
                "payment_method": {"card", "upi", "cash"},
            }
            if column in allowed_values:
                mask = (~df[column].isin(allowed_values[column])) & (~df[column].isna())
                return [int(i) for i in df.index[mask].tolist()], {column}
            return [], set()

        return [], set()

    def preview_values(values: list[str], limit: int = 4) -> str:
        if not values:
            return "-"
        shown = values[:limit]
        if len(values) > limit:
            shown.append(f"+{len(values) - limit} more")
        return ", ".join(shown)

    def format_row_refs(row_indices: list[int], limit: int = 8) -> str:
        if not row_indices:
            return "-"
        row_numbers = [idx + 1 for idx in row_indices]
        shown = ", ".join(str(num) for num in row_numbers[:limit])
        if len(row_numbers) > limit:
            shown = f"{shown} (+{len(row_numbers) - limit} more)"
        return shown

    def format_order_ids(row_indices: list[int], limit: int = 8) -> str:
        if "order_id" not in df.columns or not row_indices:
            return "-"
        order_ids: list[str] = []
        for idx in row_indices:
            value = df.at[idx, "order_id"]
            try:
                if pd.isna(value):
                    continue
            except Exception:
                pass
            text = format_cell_value(value)
            if text not in order_ids:
                order_ids.append(text)
        if not order_ids:
            return "-"
        shown = ", ".join(order_ids[:limit])
        if len(order_ids) > limit:
            shown = f"{shown} (+{len(order_ids) - limit} more)"
        return shown

    failed_items = summary.get("failed_expectations", [])
    issues: list[dict[str, Any]] = []
    row_issues: dict[int, dict[str, Any]] = {}

    for item in failed_items:
        expectation_type = str(item.get("expectation_type", "unknown"))
        column = item.get("column")
        unexpected_count = item.get("unexpected_count")
        sample_unexpected = item.get("sample_unexpected") or []
        detail = item.get("detail")

        row_indices, highlight_columns = infer_failed_rows(expectation_type, column)
        issue_name = check_label(expectation_type)
        issue_tag = f"{issue_name} ({column})" if column else issue_name

        for idx in row_indices:
            row_info = row_issues.setdefault(int(idx), {"issues": [], "columns": set()})
            if issue_tag not in row_info["issues"]:
                row_info["issues"].append(issue_tag)
            row_info["columns"].update(highlight_columns)

        affected_count = int(unexpected_count) if isinstance(unexpected_count, int) else len(row_indices)
        if detail is None:
            detail = (
                f"{affected_count} row(s) failed this check."
                if affected_count > 0
                else "Validation rule failed."
            )

        example_values = preview_values([format_cell_value(v) for v in sample_unexpected])

        issue_priority = priority(expectation_type, column)
        issues.append(
            {
                "priority": issue_priority,
                "problem": issue_name,
                "column": column or "-",
                "affected_count": affected_count,
                "affected_rows": format_row_refs(row_indices),
                "affected_ids": format_order_ids(row_indices),
                "example_values": example_values,
                "impact": impact_text(expectation_type),
                "action": action_text(expectation_type, column),
                "detail": detail,
                "_rank": {"P1": 1, "P2": 2, "P3": 3}.get(issue_priority, 9),
                "_count_sort": affected_count,
            }
        )

    if not issues:
        issues = [
            {
                "priority": "-",
                "problem": "No failed checks",
                "column": "-",
                "affected_count": 0,
                "affected_rows": "-",
                "affected_ids": "-",
                "example_values": "-",
                "impact": "Data passed all configured validation rules.",
                "action": "No immediate action required.",
                "detail": "No validation failures.",
                "_rank": 9,
                "_count_sort": 0,
            }
        ]

    sorted_issues = sorted(issues, key=lambda x: (x["_rank"], -x["_count_sort"]))

    action_items: list[str] = []
    seen_actions: set[str] = set()
    for issue in sorted_issues:
        action = issue["action"]
        if action in seen_actions:
            continue
        seen_actions.add(action)
        action_items.append(f"{issue['priority']}: {action}")
    if not action_items:
        action_items = ["No action required."]

    top_findings: list[str] = []
    for issue in sorted_issues:
        if issue["problem"] == "No failed checks":
            continue
        top_findings.append(
            f"<li><strong>{html_lib.escape(issue['problem'])}</strong> | "
            f"Row {html_lib.escape(issue['affected_rows'])} | "
            f"{html_lib.escape(issue['action'])}</li>"
        )
        if len(top_findings) >= 3:
            break
    if not top_findings:
        top_findings = ["<li>All checks passed.</li>"]

    issue_table_lines: list[str] = []
    issue_table_lines.append('<table class="clean-table issue-table">')
    issue_table_lines.append(
        "<thead><tr>"
        "<th>Pri</th>"
        "<th>Issue</th>"
        "<th>Col</th>"
        "<th>Rows</th>"
        "<th>IDs</th>"
        "<th>Impact</th>"
        "<th>Fix</th>"
        "</tr></thead><tbody>"
    )
    for issue in sorted_issues:
        priority_value = str(issue["priority"])
        if priority_value == "P1":
            priority_class = "pill p1"
        elif priority_value == "P2":
            priority_class = "pill p2"
        elif priority_value == "P3":
            priority_class = "pill p3"
        else:
            priority_class = "pill p0"

        issue_table_lines.append("<tr>")
        issue_table_lines.append(
            f'<td><span class="{priority_class}">{html_lib.escape(priority_value)}</span></td>'
        )
        issue_table_lines.append(f"<td>{html_lib.escape(str(issue['problem']))}</td>")
        issue_table_lines.append(f"<td>{html_lib.escape(str(issue['column']))}</td>")
        issue_table_lines.append(
            "<td>"
            f"{html_lib.escape(str(issue['affected_rows']))}"
            "<div class='subtle'>"
            f"{html_lib.escape(str(issue['affected_count']))} row(s)</div>"
            "</td>"
        )
        issue_table_lines.append(f"<td>{html_lib.escape(str(issue['affected_ids']))}</td>")
        issue_table_lines.append(f"<td>{html_lib.escape(str(issue['impact']))}</td>")
        issue_table_lines.append(f"<td>{html_lib.escape(str(issue['action']))}</td>")
        issue_table_lines.append("</tr>")
    issue_table_lines.append("</tbody></table>")
    issue_summary_table_html = "".join(issue_table_lines)

    row_issue_count = len(row_issues)
    total_rows = len(df)
    clean_rows = max(total_rows - row_issue_count, 0)

    row_table_lines: list[str] = []
    if row_issue_count > 0:
        focus_columns = [
            col
            for col in [
                "customer_id",
                "product_id",
                "order_date",
                "quantity",
                "unit_price",
                "discount_pct",
                "payment_method",
                "city",
            ]
            if col in df.columns
        ]

        row_table_lines.append('<table class="clean-table issue-rows-table">')
        row_table_lines.append("<thead><tr>")
        row_table_lines.append("<th>Row</th>")
        row_table_lines.append("<th>order_id</th>")
        row_table_lines.append("<th>Issue</th>")
        for col in focus_columns:
            row_table_lines.append(f"<th>{html_lib.escape(col)}</th>")
        row_table_lines.append("</tr></thead><tbody>")

        for idx in sorted(row_issues.keys()):
            row = df.loc[idx]
            row_number = idx + 1
            order_id_value = (
                format_cell_value(row["order_id"]) if "order_id" in df.columns else "-"
            )
            issues_text = "; ".join(row_issues[idx]["issues"])
            issue_columns = row_issues[idx]["columns"]

            row_table_lines.append('<tr class="issue-row">')
            row_table_lines.append(f"<td>{row_number}</td>")
            order_id_class = ' class="cell-issue"' if "order_id" in issue_columns else ""
            row_table_lines.append(
                f"<td{order_id_class}>{html_lib.escape(order_id_value)}</td>"
            )
            row_table_lines.append(
                f"<td class='issues-col'>{html_lib.escape(issues_text)}</td>"
            )

            for col in focus_columns:
                value = format_cell_value(row[col])
                class_attr = ' class="cell-issue"' if col in issue_columns else ""
                row_table_lines.append(f"<td{class_attr}>{html_lib.escape(value)}</td>")
            row_table_lines.append("</tr>")

        row_table_lines.append("</tbody></table>")
    else:
        row_table_lines.append("<p>No row-level issues detected.</p>")
    row_level_table_html = "".join(row_table_lines)

    dashboard_status = bool(summary.get("success"))
    evaluated = int(summary.get("evaluated_expectations", 0))
    failed = int(summary.get("failed_expectations_count", 0))
    successful = int(summary.get("successful_expectations", max(evaluated - failed, 0)))
    score = round((successful / evaluated) * 100, 1) if evaluated > 0 else 0.0
    status_label = "READY" if dashboard_status else "NOT READY"
    decision_text = (
        "Ready for use."
        if dashboard_status
        else "Not ready. Fix rows below. Run python main.py."
    )

    action_items_html = "".join(f"<li>{html_lib.escape(item)}</li>" for item in action_items[:6])
    top_findings_html = "".join(top_findings)

    profile_table = profile_df.to_html(index=False, classes="clean-table", border=0)
    sample_table = sample_df.to_html(index=False, classes="clean-table", border=0)

    ge_link_html = ""
    if ge_docs_href:
        ge_link_html = (
            f'<a class="btn secondary" href="{ge_docs_href}" target="_blank" rel="noopener">'
            "Open Full GX Report</a>"
        )

    page_html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Data Quality Report</title>
  <style>
    :root {{
      --bg: #f4f6fb;
      --surface: #ffffff;
      --text: #1f2937;
      --muted: #5b6475;
      --border: #e3e8f0;
      --primary: #1d4ed8;
      --pass: #157347;
      --pass-soft: #e9f8ee;
      --fail: #b42318;
      --fail-soft: #fff1f0;
      --warn-soft: #fff8e8;
      --shadow: 0 8px 24px rgba(12, 24, 43, 0.07);
    }}
    * {{
      box-sizing: border-box;
    }}
    body {{
      margin: 0;
      font-family: "Segoe UI", Arial, sans-serif;
      background: linear-gradient(180deg, #eef3ff 0, var(--bg) 220px);
      color: var(--text);
      line-height: 1.65;
    }}
    .container {{
      max-width: 1120px;
      margin: 0 auto;
      padding: 36px 22px 60px;
    }}
    .hero {{
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 16px;
      box-shadow: var(--shadow);
      padding: 26px;
      margin-bottom: 22px;
    }}
    h1 {{
      margin: 0 0 8px 0;
      font-size: 34px;
      letter-spacing: 0.2px;
    }}
    .subtitle {{
      color: var(--muted);
      margin: 0 0 16px 0;
      font-size: 17px;
    }}
    .meta {{
      color: var(--muted);
      font-size: 14px;
    }}
    .decision {{
      margin-top: 16px;
      padding: 14px 16px;
      border-radius: 12px;
      border: 1px solid {"#b8ebcb" if dashboard_status else "#f7c8c3"};
      background: {"var(--pass-soft)" if dashboard_status else "var(--fail-soft)"};
      color: {"var(--pass)" if dashboard_status else "var(--fail)"};
      font-weight: 700;
    }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 14px;
      margin: 20px 0 24px;
    }}
    .card {{
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 16px;
      box-shadow: var(--shadow);
    }}
    .card .label {{
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.6px;
      margin-bottom: 7px;
    }}
    .card .value {{
      font-size: 26px;
      line-height: 1;
      font-weight: 700;
    }}
    .btn {{
      display: inline-block;
      border: none;
      background: var(--primary);
      color: white;
      text-decoration: none;
      font-weight: 600;
      font-size: 15px;
      border-radius: 8px;
      padding: 10px 14px;
      margin-top: 14px;
    }}
    .btn.secondary {{
      background: #0f766e;
    }}
    .section {{
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 14px;
      box-shadow: var(--shadow);
      padding: 22px;
      margin-bottom: 20px;
    }}
    .section h2 {{
      margin: 0 0 14px 0;
      font-size: 20px;
    }}
    .plain-list, .actions {{
      margin: 0;
      padding-left: 22px;
    }}
    .hint {{
      margin-top: 12px;
      font-size: 14px;
      color: var(--muted);
    }}
    .legend {{
      background: var(--warn-soft);
      border: 1px solid #f5ddb0;
      border-radius: 8px;
      padding: 10px 12px;
      font-size: 14px;
      margin-bottom: 14px;
    }}
    table.clean-table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
      margin-top: 8px;
    }}
    table.clean-table th, table.clean-table td {{
      border: 1px solid var(--border);
      padding: 11px 12px;
      vertical-align: top;
      text-align: left;
    }}
    table.clean-table th {{
      background: #f7faff;
      font-weight: 700;
    }}
    .subtle {{
      font-size: 12px;
      color: var(--muted);
      margin-top: 3px;
    }}
    .pill {{
      display: inline-block;
      min-width: 34px;
      text-align: center;
      font-weight: 700;
      border-radius: 999px;
      padding: 2px 8px;
      font-size: 12px;
    }}
    .pill.p1 {{
      background: #ffe7e5;
      color: #b42318;
      border: 1px solid #f6c7c3;
    }}
    .pill.p2 {{
      background: #fff5de;
      color: #9a6700;
      border: 1px solid #f2e2b3;
    }}
    .pill.p3, .pill.p0 {{
      background: #edf2ff;
      color: #3656b5;
      border: 1px solid #dbe4ff;
    }}
    .issue-rows-table .cell-issue {{
      background: #fff1c2;
      color: #7a4a00;
      font-weight: 700;
    }}
    .issue-row td:first-child,
    .issue-row td:nth-child(2) {{
      font-weight: 600;
    }}
    .issues-col {{
      min-width: 240px;
    }}
    .issue-table tbody tr:nth-child(even),
    .issue-rows-table tbody tr:nth-child(even) {{
      background: #fcfdff;
    }}
    details {{
      margin-top: 14px;
    }}
    summary {{
      cursor: pointer;
      font-weight: 600;
      color: #344054;
    }}
    @media (max-width: 768px) {{
      h1 {{
        font-size: 28px;
      }}
      .container {{
        padding: 18px 12px 28px;
      }}
      table.clean-table {{
        font-size: 13px;
      }}
      .section {{
        padding: 16px;
      }}
    }}
  </style>
</head>
<body>
  <div class="container">
    <div class="hero">
      <h1>Data Quality Report</h1>
      <p class="subtitle">Quick view. Clear action.</p>
      <div class="meta">Generated (UTC): {summary.get('run_timestamp_utc')} | Engine: {summary.get('engine')}</div>
      <div class="decision">Decision: {status_label} | {decision_text}</div>
      {ge_link_html}
    </div>

    <div class="cards">
      <div class="card">
        <div class="label">Status</div>
        <div class="value">{status_label}</div>
      </div>
      <div class="card">
        <div class="label">Score</div>
        <div class="value">{score}%</div>
      </div>
      <div class="card">
        <div class="label">Checks</div>
        <div class="value">{successful}/{evaluated}</div>
      </div>
      <div class="card">
        <div class="label">Rows To Fix</div>
        <div class="value">{row_issue_count}/{total_rows}</div>
      </div>
    </div>

    <div class="section">
      <h2>Quick Steps</h2>
      <ol class="plain-list">
        <li>See <strong>Status</strong> and <strong>Score</strong>.</li>
        <li>Open <strong>Issues (Row + ID)</strong>.</li>
        <li>Fix yellow cells in <strong>Fix Rows</strong>.</li>
        <li>Run <code>python main.py</code>.</li>
      </ol>
    </div>

    <div class="section">
      <h2>Scope</h2>
      <ol class="plain-list">
        <li>ETL clean + transform CSV.</li>
        <li>GX checks: null, range, unique, type, category.</li>
        <li>Auto reports: JSON, Markdown, HTML.</li>
      </ol>
    </div>

    <div class="section">
      <h2>Top Issues</h2>
      <ul class="plain-list">
        {top_findings_html}
      </ul>
    </div>

    <div class="section">
      <h2>Fix First</h2>
      <ol class="actions">
        {action_items_html}
      </ol>
      <div class="hint">Then run: <code>python main.py</code></div>
    </div>

    <div class="section">
      <h2>Issues (Row + ID)</h2>
      <div class="hint">CSV row numbers start from 1.</div>
      {issue_summary_table_html}
    </div>

    <div class="section">
      <h2>Fix Rows (Highlight = Issue)</h2>
      <div class="legend">Yellow cell = bad value.</div>
      {row_level_table_html}
    </div>

    <div class="section">
      <h2>Optional Details</h2>
      <details>
        <summary>Column Profile</summary>
        {profile_table}
      </details>
      <details>
        <summary>Sample Rows (Top 50)</summary>
        {sample_table}
      </details>
    </div>
  </div>
</body>
</html>
"""
    (docs_dir / "index.html").write_text(page_html, encoding="utf-8")


def main() -> None:
    args = parse_args()

    project_root = Path(__file__).resolve().parent
    input_csv = (project_root / args.input).resolve()
    processed_output = (project_root / args.processed_output).resolve()
    reports_dir = (project_root / args.reports_dir).resolve()
    docs_dir = (project_root / args.docs_dir).resolve()
    gx_dir = (project_root / args.gx_dir).resolve()

    processed_output.parent.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    docs_dir.mkdir(parents=True, exist_ok=True)

    raw_df = pd.read_csv(input_csv)
    transformed_df = transform_dataframe(raw_df)
    transformed_df.to_csv(processed_output, index=False)

    checkpoint_json: dict[str, Any] = {}

    try:
        reset_gx_uncommitted_artifacts(gx_dir=gx_dir)
        summary, checkpoint_json, docs_url = run_great_expectations(
            clean_csv_path=processed_output,
            project_root=project_root,
            gx_dir=gx_dir,
            suite_name=args.suite_name,
            checkpoint_name=args.checkpoint_name,
        )
        sanitize_checkpoint_config_paths(
            gx_dir=gx_dir,
            checkpoint_name=args.checkpoint_name,
            project_root=project_root,
        )
        checkpoint_json = relativize_paths_in_obj(checkpoint_json, project_root)
        write_manual_data_docs(
            df=transformed_df,
            summary=summary,
            docs_dir=docs_dir,
            ge_docs_url=docs_url,
        )
        summary["data_docs_url"] = "data_docs/index.html"
    except Exception as exc:
        summary = run_manual_validation(transformed_df)
        summary["note"] = (
            "Great Expectations validation was not executed. "
            "Install dependencies from requirements.txt to run GE validation. "
            f"Fallback reason: {exc}"
        )
        checkpoint_json = {
            "success": False,
            "error": str(exc),
            "hint": "Install dependencies and rerun script to produce GE checkpoint output.",
        }
        write_manual_data_docs(
            df=transformed_df,
            summary=summary,
            docs_dir=docs_dir,
            ge_docs_url=None,
        )

    summary = relativize_paths_in_obj(summary, project_root)

    write_summary_reports(summary=summary, reports_dir=reports_dir)

    (reports_dir / "checkpoint_result.json").write_text(
        json.dumps(checkpoint_json, indent=2, default=str), encoding="utf-8"
    )

    print(f"Transformed CSV: {processed_output}")
    print(f"Validation summary JSON: {reports_dir / 'validation_summary.json'}")
    print(f"Validation summary Markdown: {reports_dir / 'validation_summary.md'}")
    print(f"Checkpoint result JSON: {reports_dir / 'checkpoint_result.json'}")
    print(f"Data docs entry point: {docs_dir / 'index.html'}")


if __name__ == "__main__":
    main()
