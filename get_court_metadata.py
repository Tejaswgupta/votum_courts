import argparse
import json
import sys
import difflib

# Adjust path to allow importing from backend package if run directly
import os
sys.path.append(os.getcwd())

try:
    from backend.ecourts.ecourts import EcourtsService
except ImportError:
    # Fallback if run from within backend/ecourts/
    sys.path.append(os.path.join(os.getcwd(), '../..'))
    from backend.ecourts.ecourts import EcourtsService

def get_exact_match(query, choices, key=None):
    """
    Find an exact case-insensitive match for a query in a list of choices.
    choices: list of dicts or strings.
    key: function to extract string from dict item.
    """
    if not choices:
        return None
    
    query_clean = query.strip().lower()
    
    for item in choices:
        item_str = key(item) if key else item
        if item_str.strip().lower() == query_clean:
            return item
    return None

def main():
    parser = argparse.ArgumentParser(description="Fetch court complexes and case types for a given City (District) and State.")
    parser.add_argument("state", help="Name of the State (e.g., 'Maharashtra')")
    parser.add_argument("city", help="Name of the City/District (e.g., 'Pune')")
    
    args = parser.parse_args()
    
    state_query = args.state
    city_query = args.city
    
    # Initialize Service
    # Using the UID found in ecourts.py main block
    uid = "3f91159bc5ba1090:in.gov.ecourts.eCourtsServices"
    service = EcourtsService("DC", uid)
    
    # 1. Get States
    print(f"Fetching states...")
    states = service.get_state_list()
    
    matched_state = get_exact_match(state_query, states, key=lambda x: x['state_name'])
    
    if not matched_state:
        print(f"Error: Could not find exact match for state '{state_query}'. Available states:")
        for s in states:
            print(f"  - {s['state_name']}")
        sys.exit(1)
        
    print(f"Matched State: {matched_state['state_name']} (Code: {matched_state['state_code']})")
    
    # 2. Get Districts
    print(f"Fetching districts for {matched_state['state_name']}...")
    districts = service.get_districts_list(matched_state['state_code'])
    
    matched_district = get_exact_match(city_query, districts, key=lambda x: x['dist_name'])
    
    if not matched_district:
        print(f"Error: Could not find exact match for district '{city_query}' in {matched_state['state_name']}. Available districts:")
        for d in districts:
            print(f"  - {d['dist_name']}")
        sys.exit(1)
        
    print(f"Found District: {matched_district['dist_name']} (Code: {matched_district['dist_code']})")
    
    # 3. Get Court Complexes
    print(f"Fetching court complexes for {matched_district['dist_name']}...")
    complexes = service.get_complex_list(matched_state['state_code'], matched_district['dist_code'])
    
    if not complexes:
        print("No court complexes found.")
        sys.exit(0)
        
    results = []
    
    print(f"Found {len(complexes)} court complexes. Fetching case types for each...")
    
    for comp in complexes:
        complex_name = comp.get('court_complex_name', 'Unknown Complex')
        court_code = comp.get('court_code')
        
        # print(f"  Processing: {complex_name}...")
        
        # 4. Get Case Types
        # Note: get_case_type returns a list of tuples (id, name)
        try:
            case_types_raw = service.get_case_type(court_code, matched_district['dist_code'], matched_state['state_code'])
            case_types = [{"type_id": item[0], "type_name": item[1]} for item in case_types_raw]
        except Exception as e:
            print(f"    Failed to fetch case types for {complex_name}: {e}")
            case_types = []
            
        results.append({
            "court_complex_name": complex_name,
            "court_complex_code": comp.get('complex_code'), # or court_code depending on what's needed
            "court_code": court_code,
            "case_types": case_types
        })
        
    # Output final JSON
    print("\n" + "="*30 + " RESULTS " + "="*30)
    print(json.dumps(results, indent=2))
    print("="*69)

if __name__ == "__main__":
    main()
