#!/usr/bin/env python3
"""
Sync Smartsheet PR to PO Report data to JSON for Procurement Dashboard
"""

import json
import os
from datetime import datetime
import smartsheet

# Smartsheet API setup
SMARTSHEET_ACCESS_TOKEN = os.environ.get('SMARTSHEET_ACCESS_TOKEN')
PR_TO_PO_SHEET_ID = 2967308268949380  # PR to PO Report sheet

def get_smartsheet_client():
    """Initialize Smartsheet client"""
    return smartsheet.Smartsheet(SMARTSHEET_ACCESS_TOKEN)

def get_pr_data_from_sheet(client, sheet_id):
    """Fetch PR to PO data from Smartsheet"""
    sheet = client.Sheets.get_sheet(sheet_id)

    # Create column name mapping
    column_map = {col.title: col.id for col in sheet.columns}

    prs = []
    for row in sheet.rows:
        pr = {}
        for cell in row.cells:
            col_name = next((name for name, id in column_map.items() if id == cell.column_id), None)
            if col_name:
                pr[col_name] = cell.value
        prs.append(pr)

    return prs

def process_pr_data(raw_prs):
    """Process raw PR data into dashboard format"""
    all_prs = []

    for pr in raw_prs:
        processed_pr = {
            'pr_num': pr.get('Pr Num'),
            'project': pr.get('Project Name', ''),
            'project_no': pr.get('Project No', ''),
            'description': pr.get('Description', ''),
            'status': pr.get('PR Status', ''),
            'pr_closed': pr.get('PR Closed', ''),
            'submission_date': str(pr.get('PR Submission Date', '')) if pr.get('PR Submission Date') else None,
            'pending_with': pr.get('Pending With', ''),
            'pending_since': str(pr.get('Pending Since', '')) if pr.get('Pending Since') else None,
            'approved_date': str(pr.get('PR Approved Date', '')) if pr.get('PR Approved Date') else None,
            'return_date': str(pr.get('PR Return Date', '')) if pr.get('PR Return Date') else None,
            'reject_date': str(pr.get('PR Reject Date', '')) if pr.get('PR Reject Date') else None,
            'pr_note': pr.get('PR Note', ''),
            'pr_value': pr.get('PR Value', 0) or 0,
            'po_num': pr.get('Po Num'),
            'revision_num': pr.get('Revision Num'),
            'po_type': pr.get('PO Type', ''),
            'vendor': pr.get('Vendor Name', ''),
            'currency': pr.get('Currency Code', ''),
            'po_value': pr.get('PO Value', 0) or 0,
            'po_status': pr.get('PO Status', ''),
            'po_approved_date': str(pr.get('PO Approved Date', '')) if pr.get('PO Approved Date') else None,
            'saving_amount': pr.get('Saving Amount', 0) or 0,
            'pr_to_po_days': pr.get('PR to PO in days'),
            'agent': pr.get('Agent', '')
        }
        all_prs.append(processed_pr)

    return all_prs

def calculate_statistics(all_prs):
    """Calculate KPIs and statistics from PR data"""

    # Status counts
    status_breakdown = {}
    for pr in all_prs:
        status = pr.get('status', 'UNKNOWN')
        if status:
            status_breakdown[status] = status_breakdown.get(status, 0) + 1

    # Year 2025 data
    prs_2025 = []
    for pr in all_prs:
        date_str = pr.get('submission_date') or pr.get('approved_date')
        if date_str and '2025' in str(date_str):
            prs_2025.append(pr)

    approved_2025 = len([p for p in prs_2025 if p.get('status') == 'APPROVED'])
    returned_2025 = len([p for p in prs_2025 if p.get('status') == 'RETURNED'])

    # Monthly breakdown for 2025
    monthly_approved = [0] * 12
    monthly_returned = [0] * 12
    monthly_rejected = [0] * 12

    for pr in prs_2025:
        date_str = pr.get('submission_date') or pr.get('approved_date')
        if date_str:
            try:
                month = int(date_str.split('-')[1]) - 1  # 0-indexed
                if 0 <= month < 12:
                    if pr.get('status') == 'APPROVED':
                        monthly_approved[month] += 1
                    elif pr.get('status') == 'RETURNED':
                        monthly_returned[month] += 1
                    elif pr.get('status') == 'REJECTED':
                        monthly_rejected[month] += 1
            except:
                pass

    # Calculate return rates
    monthly_return_rate = []
    for i in range(12):
        if monthly_approved[i] > 0:
            rate = round((monthly_returned[i] / monthly_approved[i]) * 100, 1)
        else:
            rate = 0
        monthly_return_rate.append(rate)

    # PR to PO statistics
    prs_with_po = [p for p in all_prs if p.get('po_num') and p.get('pr_to_po_days')]
    valid_days = [p['pr_to_po_days'] for p in prs_with_po if isinstance(p['pr_to_po_days'], (int, float))]

    avg_pr_to_po = round(sum(valid_days) / len(valid_days), 1) if valid_days else 0
    within_30_days = len([d for d in valid_days if d <= 30])
    after_30_days = len([d for d in valid_days if d > 30])

    # Projects list
    projects = list(set([p.get('project') for p in all_prs if p.get('project')]))
    projects.sort()

    # Vendors list
    vendors = list(set([p.get('vendor') for p in all_prs if p.get('vendor')]))
    vendors.sort()

    # Year extraction
    years = set()
    for pr in all_prs:
        date_str = pr.get('submission_date') or pr.get('approved_date')
        if date_str:
            try:
                year = date_str.split('-')[0]
                if year.isdigit():
                    years.add(year)
            except:
                pass
    years = sorted(list(years), reverse=True)

    return {
        'summary': {
            'total_prs': len(all_prs),
            'total_approved_2025': approved_2025,
            'total_returned_2025': returned_2025,
            'return_rate_2025': round((returned_2025 / approved_2025 * 100), 1) if approved_2025 > 0 else 0,
            'status_breakdown': status_breakdown,
            'avg_pr_to_po_days': avg_pr_to_po,
            'within_30_days': within_30_days,
            'after_30_days': after_30_days,
            'total_with_po': len(prs_with_po)
        },
        'monthly': {
            'labels': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
            'approved': monthly_approved,
            'returned': monthly_returned,
            'rejected': monthly_rejected,
            'return_rate': monthly_return_rate
        },
        'filters': {
            'projects': projects,
            'vendors': vendors,
            'years': years
        }
    }

def main():
    """Main function to sync data"""
    print(f"Starting Smartsheet sync at {datetime.now()}")

    try:
        client = get_smartsheet_client()
        print("Connected to Smartsheet API")

        # Fetch raw data
        print(f"Fetching data from sheet {PR_TO_PO_SHEET_ID}...")
        raw_prs = get_pr_data_from_sheet(client, PR_TO_PO_SHEET_ID)
        print(f"Fetched {len(raw_prs)} rows")

        # Process data
        all_prs = process_pr_data(raw_prs)
        print(f"Processed {len(all_prs)} PRs")

        # Calculate statistics
        stats = calculate_statistics(all_prs)

        # Create output data
        output_data = {
            'last_updated': datetime.now().isoformat(),
            'source_sheet_id': PR_TO_PO_SHEET_ID,
            **stats,
            'all_prs': all_prs
        }

        # Save to JSON
        output_path = 'data/pr_data.json'
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)

        print(f"Data saved to {output_path}")
        print(f"Summary: {stats['summary']}")

        return True

    except Exception as e:
        print(f"Error syncing data: {e}")
        return False

if __name__ == '__main__':
    main()
