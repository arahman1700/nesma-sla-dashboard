#!/usr/bin/env python3
"""
Sync SLA data from Smartsheet to data.js
This script is run by GitHub Actions to keep all dashboards updated
"""

import os
import json
import re
import requests
from datetime import datetime
from collections import Counter

def parse_cost(value):
    """Parse cost value that may contain currency symbols and formatting"""
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return float(value)
    # Remove currency symbols, commas, and whitespace
    cleaned = re.sub(r'[^\d.]', '', str(value))
    try:
        return float(cleaned) if cleaned else 0
    except ValueError:
        return 0

def parse_days(value):
    """Parse days value"""
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except:
        return 0

# Configuration
SMARTSHEET_TOKEN = os.environ.get('SMARTSHEET_TOKEN')

# Sheet IDs
JOB_ORDERS_SHEET_ID = 2606397737881476  # Job Orders Tracking sheet (SLA)

# Column mappings for Job Orders (SLA/Transportation/Payments)
JOB_ORDERS_COLUMNS = {
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
    'Invoice Receive Time (Days)': 'invoice_receive_days',
    'Payment Status': 'payment_status',
    'Payment Cycle (Days)': 'payment_cycle_days',
    'Comments': 'comments'
}

def get_sheet_data(sheet_id):
    """Fetch data from Smartsheet API"""
    headers = {
        'Authorization': f'Bearer {SMARTSHEET_TOKEN}',
        'Content-Type': 'application/json'
    }

    url = f'https://api.smartsheet.com/2.0/sheets/{sheet_id}'
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def process_sheet(sheet_data, column_mappings):
    """Process sheet data into dashboard format"""
    # Build column mapping
    col_map = {}
    for col in sheet_data.get('columns', []):
        if col['title'] in column_mappings:
            col_map[col['id']] = column_mappings[col['title']]

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

def calculate_sla_kpis(orders):
    """Calculate SLA KPIs from orders data"""
    total = len(orders)

    # Count statuses
    done = sum(1 for o in orders if o.get('performed') == 'Yes' and o.get('completion_date'))
    in_progress = sum(1 for o in orders if o.get('performed') == 'Yes' and not o.get('completion_date'))
    not_done = total - done - in_progress

    # Calculate completion times
    completion_times = [parse_days(o.get('completion_days')) for o in orders if o.get('completion_days')]

    avg_duration = sum(completion_times) / len(completion_times) if completion_times else 0
    sorted_times = sorted(completion_times)
    median_duration = sorted_times[len(sorted_times)//2] if sorted_times else 0
    p90_duration = sorted_times[int(len(sorted_times)*0.9)] if sorted_times else 0

    # On-time rate (completed within 3 days)
    on_time = sum(1 for t in completion_times if t <= 3)
    on_time_rate = (on_time / len(completion_times) * 100) if completion_times else 0

    # Total cost
    total_amount = sum(parse_cost(o.get('cost')) for o in orders if o.get('cost'))

    # Open orders (not completed)
    open_orders = sum(1 for o in orders if not o.get('completion_date'))

    # Suppliers distribution
    suppliers = Counter(o.get('supplier', 'Unknown') for o in orders if o.get('supplier'))
    top_suppliers = dict(suppliers.most_common(10))

    # Suppliers by cost
    supplier_costs = {}
    for o in orders:
        sup = o.get('supplier', 'Unknown')
        if sup:
            supplier_costs[sup] = supplier_costs.get(sup, 0) + parse_cost(o.get('cost'))
    top_supplier_costs = dict(sorted(supplier_costs.items(), key=lambda x: x[1], reverse=True)[:10])

    # Projects distribution by count
    projects_count = Counter(o.get('project', 'Unknown') for o in orders if o.get('project'))
    top_projects_count = dict(projects_count.most_common(20))

    # Projects distribution by cost
    projects = {}
    for o in orders:
        proj = o.get('project', 'Unknown')
        cost = parse_cost(o.get('cost'))
        projects[proj] = projects.get(proj, 0) + cost
    top_projects = dict(sorted(projects.items(), key=lambda x: x[1], reverse=True)[:20])

    # Equipment distribution by count
    equipment = Counter(o.get('equipment_type', 'Unknown') for o in orders if o.get('equipment_type'))
    equipment_count = dict(equipment.most_common(15))

    # Equipment distribution by cost
    equipment_costs = {}
    for o in orders:
        eq = o.get('equipment_type', 'Unknown')
        if eq:
            equipment_costs[eq] = equipment_costs.get(eq, 0) + parse_cost(o.get('cost'))
    equipment_cost = dict(sorted(equipment_costs.items(), key=lambda x: x[1], reverse=True)[:15])

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
                monthly[month]['amount'] += parse_cost(o.get('cost'))
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
        'suppliers': top_supplier_costs,
        'top_projects': top_projects,
        'projects_orders': top_projects_count,
        'projects_amounts': top_projects,
        'equipment_distribution': equipment_count,
        'equipment_count': equipment_count,
        'equipment_cost': equipment_cost,
        'monthly_trend': monthly_trend
    }

def calculate_payments_kpis(orders):
    """Calculate Payments KPIs from orders data"""
    # Filter orders with invoice applicable
    invoice_orders = [o for o in orders if o.get('invoice_applicable') == 'Yes']
    total = len(invoice_orders)

    # Payment status counts
    paid = sum(1 for o in invoice_orders if o.get('payment_status') == 'Paid')
    pending = sum(1 for o in invoice_orders if o.get('payment_status') in ['Pending Approval', 'Pending', 'Under Review'])
    other = total - paid - pending

    # Total amount
    total_amount = sum(parse_cost(o.get('cost')) for o in invoice_orders if o.get('cost'))

    # Calculate averages
    completion_days = [parse_days(o.get('completion_days')) for o in invoice_orders if o.get('completion_days')]
    avg_completion = sum(completion_days) / len(completion_days) if completion_days else 0

    payment_cycles = [parse_days(o.get('payment_cycle_days')) for o in invoice_orders if o.get('payment_cycle_days')]
    avg_payment_cycle = sum(payment_cycles) / len(payment_cycles) if payment_cycles else 0

    invoice_receive = [parse_days(o.get('invoice_receive_days')) for o in invoice_orders if o.get('invoice_receive_days')]
    avg_invoice_receive = sum(invoice_receive) / len(invoice_receive) if invoice_receive else 0

    payment_rate = (paid / total * 100) if total else 0

    # Suppliers count
    suppliers = Counter(o.get('supplier', 'Unknown') for o in invoice_orders if o.get('supplier'))
    top_suppliers = dict(suppliers.most_common(15))

    # Projects count
    projects = Counter(o.get('project', 'Unknown') for o in invoice_orders if o.get('project'))
    top_projects = dict(projects.most_common(20))

    # Equipment requested
    equipment = Counter(o.get('equipment_type', 'Unknown') for o in invoice_orders if o.get('equipment_type'))
    equipment_requested = dict(equipment.most_common(15))

    # Requesters
    requesters = Counter(o.get('requester', 'Unknown') for o in invoice_orders if o.get('requester'))
    top_requesters = dict(requesters.most_common(10))

    # Monthly trend for invoices
    monthly = {}
    for o in invoice_orders:
        date_str = o.get('job_order_date', '')
        if date_str:
            try:
                if isinstance(date_str, str):
                    month = date_str[:7]
                else:
                    month = str(date_str)[:7]
                if month not in monthly:
                    monthly[month] = {'invoices': 0, 'amount': 0}
                monthly[month]['invoices'] += 1
                monthly[month]['amount'] += parse_cost(o.get('cost'))
            except:
                pass

    monthly_trend = [
        {'month': k, 'invoices': v['invoices'], 'amount': v['amount']}
        for k, v in sorted(monthly.items())
    ]

    return {
        'summary': {
            'total_invoices': total,
            'paid_invoices': paid,
            'pending_invoices': pending,
            'other_status': other,
            'total_amount': round(total_amount, 2),
            'avg_completion_days': round(avg_completion, 1),
            'avg_payment_cycle': round(avg_payment_cycle, 1),
            'avg_invoice_receive': round(avg_invoice_receive, 1),
            'payment_rate': round(payment_rate, 1),
            'last_update': datetime.utcnow().strftime('%Y-%m-%d')
        },
        'payment_status': {
            'Paid': paid,
            'Pending Approval': pending,
            'Other': other
        },
        'suppliers': top_suppliers,
        'projects': top_projects,
        'equipment_requested': equipment_requested,
        'requesters': top_requesters,
        'monthly_trend': monthly_trend
    }

def write_data_js(sla_data, transportation_data, payments_data, orders):
    """Write all data to data.js"""
    js_content = f'''// NESMA Supply Chain Management - Dashboard Data
// Auto-synced from Smartsheet
// Last updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}

// SLA Dashboard Data
const SLA_DATA = {json.dumps(sla_data, ensure_ascii=False, indent=2)};

// Transportation Dashboard Data
const TRANSPORTATION_DATA = {json.dumps(transportation_data, ensure_ascii=False, indent=2)};

// Payments Dashboard Data
const PAYMENTS_DATA = {json.dumps(payments_data, ensure_ascii=False, indent=2)};

// Raw Orders Data (last 200)
const ORDERS_DATA = {json.dumps(orders[:200], ensure_ascii=False, indent=2)};
'''

    with open('data.js', 'w', encoding='utf-8') as f:
        f.write(js_content)

    print(f"Written {len(orders)} orders to data.js")

def main():
    if not SMARTSHEET_TOKEN:
        print("Error: SMARTSHEET_TOKEN environment variable not set")
        return 1

    print("Fetching data from Smartsheet...")
    sheet_data = get_sheet_data(JOB_ORDERS_SHEET_ID)

    print("Processing orders data...")
    orders = process_sheet(sheet_data, JOB_ORDERS_COLUMNS)

    print(f"Found {len(orders)} orders")

    print("Calculating SLA KPIs...")
    sla_data = calculate_sla_kpis(orders)

    print("Calculating Transportation KPIs...")
    # Transportation uses same data as SLA
    transportation_data = sla_data.copy()

    print("Calculating Payments KPIs...")
    payments_data = calculate_payments_kpis(orders)

    print("Writing data.js...")
    write_data_js(sla_data, transportation_data, payments_data, orders)

    print("Sync complete!")
    print(f"  - Total Orders: {sla_data['summary']['total_orders']}")
    print(f"  - On-Time Rate: {sla_data['summary']['on_time_rate']}%")
    print(f"  - Total Amount: {sla_data['summary']['total_amount']:,.2f} SAR")
    print(f"  - Total Invoices: {payments_data['summary']['total_invoices']}")
    print(f"  - Payment Rate: {payments_data['summary']['payment_rate']}%")

    return 0

if __name__ == '__main__':
    exit(main())
