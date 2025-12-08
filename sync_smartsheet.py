#!/usr/bin/env python3
"""
Sync SLA data from Smartsheet to data.js
This script is run by GitHub Actions to keep the dashboard updated
"""

import os
import json
import requests
from datetime import datetime
from collections import Counter

# Configuration
SMARTSHEET_TOKEN = os.environ.get('SMARTSHEET_TOKEN')
SHEET_ID = 2606397737881476  # Job Orders Tracking sheet

# Column mappings
COLUMN_MAPPINGS = {
    '#': 'id',
    'Job Order No.': 'job_order_no',
    'Job Order Date': 'job_order_date',
    'Requesting Project': 'project',
    'Requester Name': 'requester',
    'Type of Equipment': 'equipment_type',
    'Requested Job Date': 'requested_date',
    'Job Performed By Logistics': 'performed',
    'Job Completion Date': 'completion_date',
    'Completion Time (Days)': 'completion_days',
    'Supplier': 'supplier',
    'Cost (Excluding VAT)': 'cost',
    'Invoice Applicable': 'invoice_applicable',
    'Invoice Received': 'invoice_received',
    'Payment Status': 'payment_status',
    'Comments': 'comments'
}

def get_sheet_data():
    """Fetch data from Smartsheet API"""
    headers = {
        'Authorization': f'Bearer {SMARTSHEET_TOKEN}',
        'Content-Type': 'application/json'
    }

    url = f'https://api.smartsheet.com/2.0/sheets/{SHEET_ID}'
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def process_sheet(sheet_data):
    """Process sheet data into dashboard format"""
    # Build column mapping
    col_map = {}
    for col in sheet_data.get('columns', []):
        if col['title'] in COLUMN_MAPPINGS:
            col_map[col['id']] = COLUMN_MAPPINGS[col['title']]

    # Extract orders
    orders = []
    for row in sheet_data.get('rows', []):
        order = {}

        for cell in row.get('cells', []):
            col_id = cell.get('columnId')
            value = cell.get('value') or cell.get('displayValue')

            if col_id in col_map and value is not None:
                order[col_map[col_id]] = value

        # Only add if has job order number
        if order.get('job_order_no'):
            orders.append(order)

    return orders

def calculate_kpis(orders):
    """Calculate KPIs from orders data"""
    total = len(orders)

    # Count statuses
    done = sum(1 for o in orders if o.get('performed') == 'Yes' and o.get('completion_date'))
    in_progress = sum(1 for o in orders if o.get('performed') == 'Yes' and not o.get('completion_date'))
    not_done = total - done - in_progress

    # Calculate completion times
    completion_times = [float(o.get('completion_days', 0)) for o in orders if o.get('completion_days')]

    avg_duration = sum(completion_times) / len(completion_times) if completion_times else 0
    sorted_times = sorted(completion_times)
    median_duration = sorted_times[len(sorted_times)//2] if sorted_times else 0
    p90_duration = sorted_times[int(len(sorted_times)*0.9)] if sorted_times else 0

    # On-time rate (completed within 3 days)
    on_time = sum(1 for t in completion_times if t <= 3)
    on_time_rate = (on_time / len(completion_times) * 100) if completion_times else 0

    # Total cost
    total_amount = sum(float(o.get('cost', 0)) for o in orders if o.get('cost'))

    # Open orders (not completed)
    open_orders = sum(1 for o in orders if not o.get('completion_date'))

    # Suppliers distribution
    suppliers = Counter(o.get('supplier', 'Unknown') for o in orders if o.get('supplier'))
    top_suppliers = dict(suppliers.most_common(10))

    # Projects distribution
    projects = {}
    for o in orders:
        proj = o.get('project', 'Unknown')
        cost = float(o.get('cost', 0)) if o.get('cost') else 0
        projects[proj] = projects.get(proj, 0) + cost
    top_projects = dict(sorted(projects.items(), key=lambda x: x[1], reverse=True)[:10])

    # Equipment distribution
    equipment = Counter(o.get('equipment_type', 'Unknown') for o in orders if o.get('equipment_type'))
    equipment_distribution = dict(equipment.most_common(10))

    # Monthly trend
    monthly = {}
    for o in orders:
        date_str = o.get('job_order_date', '')
        if date_str:
            try:
                if isinstance(date_str, str):
                    month = date_str[:7]  # YYYY-MM
                else:
                    month = str(date_str)[:7]
                if month not in monthly:
                    monthly[month] = {'orders': 0, 'amount': 0}
                monthly[month]['orders'] += 1
                monthly[month]['amount'] += float(o.get('cost', 0)) if o.get('cost') else 0
            except:
                pass

    monthly_trend = [
        {'month': k, 'orders': v['orders'], 'amount': v['amount']}
        for k, v in sorted(monthly.items())
    ]

    return {
        'summary': {
            'total_orders': total,
            'done_orders': done,
            'in_progress_orders': in_progress,
            'not_done_orders': not_done,
            'on_time_rate': round(on_time_rate, 2),
            'total_amount': round(total_amount, 2),
            'avg_duration': round(avg_duration, 2),
            'median_duration': median_duration,
            'p90_duration': p90_duration,
            'open_orders': open_orders,
            'last_update': datetime.utcnow().strftime('%Y-%m-%d')
        },
        'status': {
            'Done': done,
            'In Progress': in_progress,
            'Not Done': not_done
        },
        'top_suppliers': top_suppliers,
        'top_projects': top_projects,
        'equipment_distribution': equipment_distribution,
        'monthly_trend': monthly_trend
    }

def write_data_js(kpis, orders):
    """Write data to data.js"""
    js_content = f'''// NESMA SLA Dashboard - Data
// Auto-synced from Smartsheet
// Last updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}

const SLA_DATA = {json.dumps(kpis, ensure_ascii=False, indent=2)};

const ORDERS_DATA = {json.dumps(orders[:100], ensure_ascii=False, indent=2)};
'''

    with open('data.js', 'w', encoding='utf-8') as f:
        f.write(js_content)

    print(f"Written {len(orders)} orders to data.js")

def main():
    if not SMARTSHEET_TOKEN:
        print("Error: SMARTSHEET_TOKEN environment variable not set")
        return 1

    print("Fetching data from Smartsheet...")
    sheet_data = get_sheet_data()

    print("Processing orders data...")
    orders = process_sheet(sheet_data)

    print(f"Found {len(orders)} orders")

    print("Calculating KPIs...")
    kpis = calculate_kpis(orders)

    write_data_js(kpis, orders)

    print("Sync complete!")
    return 0

if __name__ == '__main__':
    exit(main())
