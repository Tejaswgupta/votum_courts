import io
from typing import List, Dict, Any, Optional
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer

def generate_cause_list_pdf(
    entries: List[Dict[str, Any]],
    title: str,
    subtitle: Optional[str] = None
) -> bytes:
    """
    Generate a PDF for the cause list / hearing list.
    
    entries: List of dicts with keys:
        - sno (Serial Number / Index)
        - case_no
        - court_name
        - item_no (optional)
        - orders (optional)
        - text (optional - raw text snippet)
    """
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(A4),
        rightMargin=30,
        leftMargin=30,
        topMargin=30,
        bottomMargin=30
    )
    
    styles = getSampleStyleSheet()
    elements = []
    
    # Title
    elements.append(Paragraph(title, styles['Title']))
    if subtitle:
        elements.append(Paragraph(subtitle, styles['Heading2']))
    elements.append(Spacer(1, 20))
    
    # Table Data
    # specific columns: S.No, Case No, Court Name, Item No, Orders (if present in any entry)
    
    has_orders = any(e.get('orders') for e in entries)
    
    headers = ["S.No", "Case No", "Court Name", "Item No"]
    if has_orders:
        headers.append("Orders / Remarks")
        
    data = [headers]
    
    for idx, entry in enumerate(entries, start=1):
        row = [
            str(entry.get('sno', idx)),
            entry.get('case_no', '-'),
            entry.get('court_name', '-'),
            entry.get('item_no', '-')
        ]
        if has_orders:
            row.append(entry.get('orders', '-'))
        data.append(row)
        
    # Table Style
    table_style = TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('ALIGN', (1, 1), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('WORDWRAP', (0, 0), (-1, -1), True),
    ])
    
    # Calculate column widths
    # Available width approx 842 - 60 = 780 (Landscape A4)
    # S.No: 40, Case: 150, Court: 150, Item: 60, Orders: Remainder
    
    cw = [40, 150, 200, 60]
    if has_orders:
        cw.append(330)
    else:
        # redistribute extra space
        cw = [50, 200, 300, 100]
        
    t = Table(data, colWidths=cw)
    t.setStyle(table_style)
    elements.append(t)
    
    doc.build(elements)
    buffer.seek(0)
    return buffer.read()
