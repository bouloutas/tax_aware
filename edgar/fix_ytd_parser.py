"""
Fix YTD vs QTR context selection in XBRL parser.
This script updates _select_numeric to prefer quarterly contexts over YTD.
"""
import re

def fix_parser_ytd_issue():
    """Update filing_parser.py to handle YTD vs QTR contexts correctly."""
    
    parser_file = '/home/tasos/tax_aware/edgar/src/filing_parser.py'
    
    with open(parser_file, 'r') as f:
        content = f.read()
    
    # Replace _select_numeric method to prefer QTR over YTD
    old_method = """    def _select_numeric(self, elements: List[ET.Element]) -> Optional[float]:
        values = []
        for elem in elements:
            value = self._to_float(elem.text)
            if value is None:
                continue
            context = elem.attrib.get('contextRef', '')
            unit = elem.attrib.get('unitRef', '')
            values.append((context or '', unit or '', value))
        if not values:
            return None
        values.sort(key=lambda x: x[0])
        return values[-1][2]"""
    
    new_method = """    def _select_numeric(self, elements: List[ET.Element]) -> Optional[float]:
        """
        Select numeric value, preferring quarterly (QTR) contexts over year-to-date (YTD).
        
        XBRL contexts can be:
        - Instant (point in time): e.g., 'FD2024Q1' (end of Q1)
        - Duration QTR (quarterly): e.g., 'FD2024Q1QTD' (start Q1 to end Q1)
        - Duration YTD (year-to-date): e.g., 'FD2024Q1YTD' (start of year to end Q1)
        
        For quarterly financial items, we prefer QTR over YTD.
        """
        values = []
        for elem in elements:
            value = self._to_float(elem.text)
            if value is None:
                continue
            context = elem.attrib.get('contextRef', '')
            unit = elem.attrib.get('unitRef', '')
            
            # Determine context type: 'instant', 'qtr', 'ytd', or 'unknown'
            context_lower = context.lower()
            is_ytd = 'ytd' in context_lower or 'year' in context_lower or 'cumulative' in context_lower
            is_qtr = 'qtr' in context_lower or 'quarter' in context_lower
            # Check for duration patterns: if context has startDate and endDate, check duration
            # For now, use heuristics based on context ID naming
            
            # Priority: instant > qtr > ytd > unknown
            if is_ytd:
                priority = 1  # Lowest priority
            elif is_qtr:
                priority = 3  # High priority
            elif context:  # Has context but not clearly YTD/QTR
                priority = 2  # Medium priority
            else:
                priority = 0  # No context, lowest
                
            values.append((priority, context or '', unit or '', value))
        
        if not values:
            return None
        
        # Sort by priority (descending), then by context name
        values.sort(key=lambda x: (-x[0], x[1]))
        
        # Return highest priority value
        return values[0][3]"""
    
    if old_method in content:
        content = content.replace(old_method, new_method)
        with open(parser_file, 'w') as f:
            f.write(content)
        print("Updated _select_numeric method to prefer QTR over YTD")
        return True
    else:
        print("Could not find exact method to replace. Manual update needed.")
        return False

if __name__ == "__main__":
    fix_parser_ytd_issue()

