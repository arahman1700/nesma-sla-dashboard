#!/usr/bin/env python3
"""
Sync Transportation & Payments data from Smartsheet
Sheet: Transportation_Tracking
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

        # Only add if has job order number or project
        if record.get("job_order_no") or record.get("project"):
            records.append(record)

    return records


def prepare_transportation_data(records):
    """Prepare transportation dashboard data"""
    # Calculate total amount for each record
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

    # Status normalization
    for r in records:
        status = str(r.get("status", "")).strip().lower()
        if status in ["done", "completed", "complete"]:
            r["status"] = "Done"
        elif status in ["in progress", "inprogress", "pending"]:
            r["status"] = "In Progress"
        elif status in ["not done", "cancelled", "canceled"]:
            r["status"] = "Not Done"
        elif not status:
            r["status"] = "In Progress"
        else:
            r["status"] = r.get("status", "In Progress")

    # Extract unique values for filters
    projects = sorted(
        [str(p) for p in set(r.get("project") for r in records if r.get("project"))]
    )
    suppliers = sorted(
        [
            str(s)
            for s in set(
                r.get("supplier")
                for r in records
                if r.get("supplier")
                and not str(r.get("supplier", "")).startswith("202")
            )
        ]
    )
    equipment = set()
    for r in records:
        for i in range(1, 6):
            eq = r.get(f"equipment_{i}")
            if eq and isinstance(eq, str):
                equipment.add(eq)
    equipment = sorted(list(equipment))

    rent_types = sorted(set(r.get("rent_type") for r in records if r.get("rent_type")))
    statuses = sorted(set(r.get("status") for r in records if r.get("status")))
    companies = sorted(set(r.get("company") for r in records if r.get("company")))

    # Format records for output
    formatted_records = []
    for r in records:
        formatted_records.append(
            {
                "job_order_no": r.get("job_order_no", ""),
                "company": r.get("company", ""),
                "project": r.get("project", "Unknown"),
                "requester": r.get("requester", ""),
                "request_date": str(r.get("request_date", ""))
                if r.get("request_date")
                else "",
                "supplier": r.get("supplier", "Unknown"),
                "equipment_1": r.get("equipment_1", ""),
                "equipment_2": r.get("equipment_2", ""),
                "equipment_3": r.get("equipment_3", ""),
                "equipment_4": r.get("equipment_4", ""),
                "equipment_5": r.get("equipment_5", ""),
                "rent_type": r.get("rent_type", "Daily"),
                "total_amount": r.get("total_amount", 0),
                "actual_date": str(r.get("actual_date", ""))
                if r.get("actual_date")
                else "",
                "duration": safe_float(r.get("duration")),
                "status": r.get("status", "In Progress"),
                "pending_with": r.get("pending_with", ""),
                "remarks": r.get("remarks", ""),
            }
        )

    return {
        "metadata": {
            "last_update": datetime.now().strftime("%Y-%m-%d"),
            "total_records": len(formatted_records),
            "source_sheet": "Transportation_Tracking",
        },
        "filters": {
            "projects": projects,
            "suppliers": suppliers,
            "equipment": equipment,
            "rent_types": rent_types if rent_types else ["Daily", "Monthly", "Hourly"],
            "status": statuses if statuses else ["Done", "In Progress", "Not Done"],
            "companies": companies,
        },
        "records": formatted_records,
    }


def prepare_payments_data(records):
    """Prepare payments dashboard data - filter records with amounts"""
    payment_records = [r for r in records if safe_float(r.get("total_amount")) > 0]

    # Extract unique values for filters
    projects = sorted(
        set(r.get("project") for r in payment_records if r.get("project"))
    )
    suppliers = sorted(
        set(
            r.get("supplier")
            for r in payment_records
            if r.get("supplier") and not str(r.get("supplier", "")).startswith("202")
        )
    )

    # Determine payment status based on job status
    for r in payment_records:
        status = str(r.get("status", "")).strip().lower()
        if status == "done":
            r["payment_status"] = "Paid"
        elif status in ["in progress", "pending"]:
            r["payment_status"] = "Pending"
        else:
            r["payment_status"] = "Pending"

    payment_statuses = sorted(set(r.get("payment_status") for r in payment_records))

    # Format records for output
    formatted_records = []
    for r in payment_records:
        formatted_records.append(
            {
                "job_order_no": r.get("job_order_no", ""),
                "company": r.get("company", ""),
                "project": r.get("project", "Unknown"),
                "requester": r.get("requester", ""),
                "request_date": str(r.get("request_date", ""))
                if r.get("request_date")
                else "",
                "supplier": r.get("supplier", "Unknown"),
                "equipment_1": r.get("equipment_1", ""),
                "total_amount": safe_float(r.get("total_amount")),
                "payment_status": r.get("payment_status", "Pending"),
                "duration": safe_float(r.get("duration")),
                "invoice_received": "Yes" if r.get("status") == "Done" else "No",
                "invoice_receive_days": safe_float(r.get("duration")),
                "payment_cycle_days": safe_float(r.get("duration")) + 30,  # Estimate
            }
        )

    return {
        "metadata": {
            "last_update": datetime.now().strftime("%Y-%m-%d"),
            "total_records": len(formatted_records),
            "source_sheet": "Transportation_Tracking",
        },
        "filters": {
            "projects": projects,
            "suppliers": suppliers,
            "payment_statuses": payment_statuses
            if payment_statuses
            else ["Paid", "Pending"],
        },
        "records": formatted_records,
    }


def main():
    print(f"=== Logistics Data Sync ===")
    print(f"Started at: {datetime.now()}")
    print(f"Sheet ID: {TRANSPORTATION_SHEET_ID}")

    try:
        # Fetch data from Smartsheet
        print("\nFetching data from Smartsheet...")
        sheet_data = get_sheet_data(TRANSPORTATION_SHEET_ID)
        print(f"Sheet name: {sheet_data.get('name')}")

        # Process data
        print("\nProcessing records...")
        records = process_sheet(sheet_data)
        print(f"Total records found: {len(records)}")

        # Prepare transportation data
        print("\nPreparing transportation data...")
        transportation_data = prepare_transportation_data(records)

        # Save transportation data
        with open("transportation_full_data.json", "w", encoding="utf-8") as f:
            json.dump(transportation_data, f, ensure_ascii=False, indent=2)
        print(
            f"Saved transportation_full_data.json ({transportation_data['metadata']['total_records']} records)"
        )

        # Prepare payments data
        print("\nPreparing payments data...")
        payments_data = prepare_payments_data(records)

        # Save payments data
        with open("payments_full_data.json", "w", encoding="utf-8") as f:
            json.dump(payments_data, f, ensure_ascii=False, indent=2)
        print(
            f"Saved payments_full_data.json ({payments_data['metadata']['total_records']} records)"
        )

        # Summary
        print(f"\n=== Sync Complete ===")
        print(
            f"Transportation Records: {transportation_data['metadata']['total_records']}"
        )
        print(f"  - Projects: {len(transportation_data['filters']['projects'])}")
        print(f"  - Suppliers: {len(transportation_data['filters']['suppliers'])}")
        print(
            f"  - Equipment Types: {len(transportation_data['filters']['equipment'])}"
        )
        print(f"\nPayments Records: {payments_data['metadata']['total_records']}")
        print(
            f"  - Total Amount: {sum(r['total_amount'] for r in payments_data['records']):,.2f} SAR"
        )

        return True

    except Exception as e:
        print(f"\nError: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
