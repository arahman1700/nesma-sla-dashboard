#!/usr/bin/env python3
"""
Sync SLA Dashboard data from Smartsheet Transportation_Tracking
"""

import os
import json
import requests
from datetime import datetime
from collections import Counter

# Configuration
SMARTSHEET_TOKEN = os.environ.get(
    "SMARTSHEET_TOKEN", "r6WG6zpLw2TR84F54tZCCzMtqjkTlTbuWDiws"
)
TRANSPORTATION_SHEET_ID = 7876932495429508  # Transportation_Tracking

# Column mappings
COLUMN_MAPPINGS = {
    "#": "serial_no",
    "Job Order NO.": "job_order_no",
    "Company": "company",
    "Project Name": "project",
    "Rqstr Name": "requester",
    "Rqst Date": "request_date",
    "supplier": "supplier",
    "EQUIPMENT 1": "equipment_1",
    "price1": "price_1",
    "EQUIPMENT 2": "equipment_2",
    "Price2": "price_2",
    "EQUIPMENT 3": "equipment_3",
    "Price3": "price_3",
    "EQUIPMENT 4": "equipment_4",
    "Price4": "price_4",
    "EQUIPMENT 5": "equipment_5",
    "price5": "price_5",
    "Type of Rent": "rent_type",
    "Total Amount": "total_amount",
    "Act Date2": "actual_date",
    "Duration": "duration",
    "Status": "status",
    "Pending with": "pending_with",
    "Remarks": "remarks",
}


def safe_float(value):
    """Safely convert value to float"""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    try:
        cleaned = (
            str(value)
            .replace(",", "")
            .replace(" ", "")
            .replace("SAR", "")
            .replace("USD", "")
        )
        return float(cleaned) if cleaned else 0.0
    except:
        return 0.0


def get_sheet_data(sheet_id):
    """Fetch data from Smartsheet API"""
    headers = {
        "Authorization": f"Bearer {SMARTSHEET_TOKEN}",
        "Content-Type": "application/json",
    }

    url = f"https://api.smartsheet.com/2.0/sheets/{sheet_id}"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def process_sheet(sheet_data):
    """Process sheet data into records"""
    col_map = {}
    for col in sheet_data.get("columns", []):
        if col["title"] in COLUMN_MAPPINGS:
            col_map[col["id"]] = COLUMN_MAPPINGS[col["title"]]

    records = []
    for row in sheet_data.get("rows", []):
        record = {}

        for cell in row.get("cells", []):
            col_id = cell.get("columnId")
            value = cell.get("value") or cell.get("displayValue")

            if col_id in col_map and value is not None:
                record[col_map[col_id]] = value

        if record.get("job_order_no") or record.get("project"):
            records.append(record)

    return records


def calculate_sla_metrics(records):
    """Calculate SLA metrics from transportation records"""

    # Normalize status
    for r in records:
        status = str(r.get("status", "")).strip().lower()
        if status in ["done", "completed", "complete"]:
            r["status"] = "Done"
        elif status in [
            "in progress",
            "inprogress",
            "pending",
            "under process",
            "waiting for quotation",
        ]:
            r["status"] = "In Progress"
        else:
            r["status"] = "Not Done" if status else "In Progress"

    # Calculate totals
    total_orders = len(records)
    done_orders = len([r for r in records if r.get("status") == "Done"])
    in_progress_orders = len([r for r in records if r.get("status") == "In Progress"])
    not_done_orders = len([r for r in records if r.get("status") == "Not Done"])

    # Calculate amounts
    for r in records:
        if not r.get("total_amount"):
            total = sum(
                [
                    safe_float(r.get("price_1")),
                    safe_float(r.get("price_2")),
                    safe_float(r.get("price_3")),
                    safe_float(r.get("price_4")),
                    safe_float(r.get("price_5")),
                ]
            )
            r["total_amount"] = total
        else:
            r["total_amount"] = safe_float(r.get("total_amount"))

    total_amount = sum(r.get("total_amount", 0) for r in records)

    # Duration statistics
    durations = [
        safe_float(r.get("duration"))
        for r in records
        if r.get("duration") and safe_float(r.get("duration")) > 0
    ]

    if durations:
        avg_duration = sum(durations) / len(durations)
        sorted_durations = sorted(durations)
        median_duration = sorted_durations[len(sorted_durations) // 2]
        p90_index = int(len(sorted_durations) * 0.9)
        p90_duration = sorted_durations[min(p90_index, len(sorted_durations) - 1)]

        # On-time rate (completed within 3 days)
        on_time = len([d for d in durations if d <= 3])
        on_time_rate = (on_time / len(durations)) * 100
    else:
        avg_duration = 0
        median_duration = 0
        p90_duration = 0
        on_time_rate = 0

    # Completion rate
    completion_rate = (done_orders / total_orders * 100) if total_orders > 0 else 0

    # Open orders
    open_orders = in_progress_orders + not_done_orders

    # Company breakdown
    company_counts = Counter(
        r.get("company", "Unknown") for r in records if r.get("company")
    )

    # Supplier statistics
    supplier_counts = Counter(
        r.get("supplier")
        for r in records
        if r.get("supplier") and not str(r.get("supplier", "")).startswith("202")
    )
    supplier_amounts = {}
    for r in records:
        supplier = r.get("supplier")
        if supplier and not str(supplier).startswith("202"):
            supplier_amounts[supplier] = supplier_amounts.get(supplier, 0) + r.get(
                "total_amount", 0
            )

    top_suppliers = dict(supplier_counts.most_common(10))
    top_suppliers_by_amount = dict(
        sorted(supplier_amounts.items(), key=lambda x: x[1], reverse=True)[:10]
    )

    # Project statistics
    project_counts = Counter(r.get("project") for r in records if r.get("project"))
    project_amounts = {}
    for r in records:
        project = r.get("project")
        if project:
            project_amounts[project] = project_amounts.get(project, 0) + r.get(
                "total_amount", 0
            )

    top_projects_by_orders = dict(project_counts.most_common(20))
    top_projects_by_amount = dict(
        sorted(project_amounts.items(), key=lambda x: x[1], reverse=True)[:20]
    )

    # Equipment statistics
    equipment_counts = Counter()
    equipment_amounts = {}
    for r in records:
        for i in range(1, 6):
            eq = r.get(f"equipment_{i}")
            price = safe_float(r.get(f"price_{i}"))
            if eq and isinstance(eq, str):
                equipment_counts[eq] += 1
                equipment_amounts[eq] = equipment_amounts.get(eq, 0) + price

    equipment_distribution = dict(equipment_counts.most_common(15))
    equipment_by_amount = dict(
        sorted(equipment_amounts.items(), key=lambda x: x[1], reverse=True)[:15]
    )

    # Monthly trend
    monthly_data = {}
    for r in records:
        date_str = r.get("request_date")
        if date_str:
            try:
                month = str(date_str)[:7]  # YYYY-MM
                if month not in monthly_data:
                    monthly_data[month] = {"orders": 0, "amount": 0, "done": 0}
                monthly_data[month]["orders"] += 1
                monthly_data[month]["amount"] += r.get("total_amount", 0)
                if r.get("status") == "Done":
                    monthly_data[month]["done"] += 1
            except:
                pass

    monthly_trend = [
        {
            "month": k,
            "orders": v["orders"],
            "amount": v["amount"],
            "done": v["done"],
            "completion_rate": round((v["done"] / v["orders"] * 100), 1)
            if v["orders"] > 0
            else 0,
        }
        for k, v in sorted(monthly_data.items())
    ]

    return {
        "summary": {
            "total_orders": total_orders,
            "done_orders": done_orders,
            "in_progress_orders": in_progress_orders,
            "not_done_orders": not_done_orders,
            "open_orders": open_orders,
            "on_time_rate": round(on_time_rate, 1),
            "completion_rate": round(completion_rate, 1),
            "total_amount": round(total_amount, 2),
            "avg_duration": round(avg_duration, 2),
            "median_duration": round(median_duration, 1),
            "p90_duration": round(p90_duration, 1),
            "last_update": datetime.now().strftime("%Y-%m-%d"),
        },
        "status": {
            "Done": done_orders,
            "In Progress": in_progress_orders,
            "Not Done": not_done_orders,
        },
        "company_breakdown": dict(company_counts),
        "top_suppliers": top_suppliers,
        "suppliers_by_amount": top_suppliers_by_amount,
        "top_projects_by_orders": top_projects_by_orders,
        "top_projects_by_amount": top_projects_by_amount,
        "equipment_distribution": equipment_distribution,
        "equipment_by_amount": equipment_by_amount,
        "monthly_trend": monthly_trend,
    }


def main():
    print(f"=== SLA Dashboard Data Sync ===")
    print(f"Started at: {datetime.now()}")
    print(f"Sheet ID: {TRANSPORTATION_SHEET_ID}")

    try:
        # Fetch data
        print("\nFetching data from Smartsheet...")
        sheet_data = get_sheet_data(TRANSPORTATION_SHEET_ID)
        print(f"Sheet name: {sheet_data.get('name')}")

        # Process data
        print("\nProcessing records...")
        records = process_sheet(sheet_data)
        print(f"Total records: {len(records)}")

        # Calculate SLA metrics
        print("\nCalculating SLA metrics...")
        sla_data = calculate_sla_metrics(records)

        # Add metadata
        output_data = {
            "metadata": {
                "last_update": datetime.now().isoformat(),
                "source_sheet": sheet_data.get("name"),
                "total_records": len(records),
            },
            **sla_data,
        }

        # Save to JSON
        output_path = "data/sla_data.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)

        print(f"\n=== Sync Complete ===")
        print(f"Data saved to: {output_path}")
        print(f"\nSummary:")
        print(f"  - Total Orders: {sla_data['summary']['total_orders']}")
        print(f"  - Done: {sla_data['summary']['done_orders']}")
        print(f"  - In Progress: {sla_data['summary']['in_progress_orders']}")
        print(f"  - On-Time Rate: {sla_data['summary']['on_time_rate']}%")
        print(f"  - Completion Rate: {sla_data['summary']['completion_rate']}%")
        print(f"  - Avg Duration: {sla_data['summary']['avg_duration']} days")
        print(f"  - Total Amount: {sla_data['summary']['total_amount']:,.2f} SAR")

        return True

    except Exception as e:
        print(f"\nError: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
