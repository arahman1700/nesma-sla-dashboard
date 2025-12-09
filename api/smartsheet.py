"""
Vercel Serverless Function - Fetch live data from Smartsheet
"""
import os
import json
import re
from http.server import BaseHTTPRequestHandler
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from datetime import datetime
from collections import Counter

# Configuration
SMARTSHEET_TOKEN = os.environ.get('SMARTSHEET_TOKEN')
JOB_ORDERS_SHEET_ID = 2606397737881476

# Column mappings
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

def parse_cost(value):
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = re.sub(r'[^\d.]', '', str(value))
    try:
        return float(cleaned) if cleaned else 0
    except ValueError:
        return 0

def parse_days(value):
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except:
        return 0

def get_sheet_data(sheet_id):
    url = f'https://api.smartsheet.com/2.0/sheets/{sheet_id}'
    req = Request(url, headers={
        'Authorization': f'Bearer {SMARTSHEET_TOKEN}',
        'Content-Type': 'application/json'
    })
    with urlopen(req) as response:
        return json.loads(response.read().decode())

def process_sheet(sheet_data, column_mappings):
    col_map = {}
    for col in sheet_data.get('columns', []):
        if col['title'] in column_mappings:
            col_map[col['id']] = column_mappings[col['title']]

    orders = []
    for row in sheet_data.get('rows', []):
        order = {}
        for cell in row.get('cells', []):
            col_id = cell.get('columnId')
            value = cell.get('value') or cell.get('displayValue')
            if col_id in col_map and value is not None:
                order[col_map[col_id]] = value
        if order.get('job_order_no'):
            orders.append(order)
    return orders

def calculate_sla_kpis(orders):
    total = len(orders)
    done = sum(1 for o in orders if o.get('performed') == 'Yes' and o.get('completion_date'))
    in_progress = sum(1 for o in orders if o.get('performed') == 'Yes' and not o.get('completion_date'))
    not_done = total - done - in_progress

    completion_times = [parse_days(o.get('completion_days')) for o in orders if o.get('completion_days')]
    avg_duration = sum(completion_times) / len(completion_times) if completion_times else 0
    sorted_times = sorted(completion_times)
    median_duration = sorted_times[len(sorted_times)//2] if sorted_times else 0
    p90_duration = sorted_times[int(len(sorted_times)*0.9)] if sorted_times else 0

    on_time = sum(1 for t in completion_times if t <= 3)
    on_time_rate = (on_time / len(completion_times) * 100) if completion_times else 0
    total_amount = sum(parse_cost(o.get('cost')) for o in orders if o.get('cost'))
    open_orders = sum(1 for o in orders if not o.get('completion_date'))

    suppliers = Counter(o.get('supplier', 'Unknown') for o in orders if o.get('supplier'))
    top_suppliers = dict(suppliers.most_common(10))

    supplier_costs = {}
    for o in orders:
        sup = o.get('supplier', 'Unknown')
        if sup:
            supplier_costs[sup] = supplier_costs.get(sup, 0) + parse_cost(o.get('cost'))
    top_supplier_costs = dict(sorted(supplier_costs.items(), key=lambda x: x[1], reverse=True)[:10])

    projects_count = Counter(o.get('project', 'Unknown') for o in orders if o.get('project'))
    top_projects_count = dict(projects_count.most_common(20))

    projects = {}
    for o in orders:
        proj = o.get('project', 'Unknown')
        cost = parse_cost(o.get('cost'))
        projects[proj] = projects.get(proj, 0) + cost
    top_projects = dict(sorted(projects.items(), key=lambda x: x[1], reverse=True)[:20])

    equipment = Counter(o.get('equipment_type', 'Unknown') for o in orders if o.get('equipment_type'))
    equipment_count = dict(equipment.most_common(15))

    equipment_costs = {}
    for o in orders:
        eq = o.get('equipment_type', 'Unknown')
        if eq:
            equipment_costs[eq] = equipment_costs.get(eq, 0) + parse_cost(o.get('cost'))
    equipment_cost = dict(sorted(equipment_costs.items(), key=lambda x: x[1], reverse=True)[:15])

    monthly = {}
    for o in orders:
        date_str = o.get('job_order_date', '')
        if date_str:
            try:
                if isinstance(date_str, str):
                    month = date_str[:7]
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
            'last_update': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
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
    invoice_orders = [o for o in orders if o.get('invoice_applicable') == 'Yes']
    total = len(invoice_orders)

    paid = sum(1 for o in invoice_orders if o.get('payment_status') == 'Paid')
    pending = sum(1 for o in invoice_orders if o.get('payment_status') in ['Pending Approval', 'Pending', 'Under Review'])
    other = total - paid - pending

    total_amount = sum(parse_cost(o.get('cost')) for o in invoice_orders if o.get('cost'))

    completion_days = [parse_days(o.get('completion_days')) for o in invoice_orders if o.get('completion_days')]
    avg_completion = sum(completion_days) / len(completion_days) if completion_days else 0

    payment_cycles = [parse_days(o.get('payment_cycle_days')) for o in invoice_orders if o.get('payment_cycle_days')]
    avg_payment_cycle = sum(payment_cycles) / len(payment_cycles) if payment_cycles else 0

    invoice_receive = [parse_days(o.get('invoice_receive_days')) for o in invoice_orders if o.get('invoice_receive_days')]
    avg_invoice_receive = sum(invoice_receive) / len(invoice_receive) if invoice_receive else 0

    payment_rate = (paid / total * 100) if total else 0

    suppliers = Counter(o.get('supplier', 'Unknown') for o in invoice_orders if o.get('supplier'))
    top_suppliers = dict(suppliers.most_common(15))

    projects = Counter(o.get('project', 'Unknown') for o in invoice_orders if o.get('project'))
    top_projects = dict(projects.most_common(20))

    equipment = Counter(o.get('equipment_type', 'Unknown') for o in invoice_orders if o.get('equipment_type'))
    equipment_requested = dict(equipment.most_common(15))

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
            'last_update': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
        },
        'payment_status': {
            'Paid': paid,
            'Pending Approval': pending,
            'Other': other
        },
        'suppliers': top_suppliers,
        'projects': top_projects,
        'equipment_requested': equipment_requested
    }

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            if not SMARTSHEET_TOKEN:
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'error': 'SMARTSHEET_TOKEN not configured'}).encode())
                return

            sheet_data = get_sheet_data(JOB_ORDERS_SHEET_ID)
            orders = process_sheet(sheet_data, JOB_ORDERS_COLUMNS)

            sla_data = calculate_sla_kpis(orders)
            transportation_data = sla_data.copy()
            payments_data = calculate_payments_kpis(orders)

            result = {
                'sla': sla_data,
                'transportation': transportation_data,
                'payments': payments_data,
                'orders': orders[:200],
                'last_update': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
            }

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
            self.end_headers()
            self.wfile.write(json.dumps(result, ensure_ascii=False).encode('utf-8'))

        except HTTPError as e:
            self.send_response(e.code)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({'error': f'Smartsheet API error: {e.reason}'}).encode())
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({'error': str(e)}).encode())

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
