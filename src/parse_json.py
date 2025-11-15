#!/usr/bin/env python3
"""
Parse the JSON data line by line to handle malformed entries
"""
import json
import re

def parse_json_robust(input_file):
    """Parse JSON file robustly, handling malformed entries"""
    records = []
    
    with open(input_file, 'r') as f:
        content = f.read()
    
    # Split into individual record strings
    # Find all objects between { and }
    brace_count = 0
    current_record = ""
    in_string = False
    escape_next = False
    
    for char in content:
        if escape_next:
            current_record += char
            escape_next = False
            continue
            
        if char == '\\':
            escape_next = True
            current_record += char
            continue
            
        if char == '"' and not escape_next:
            in_string = not in_string
            
        if not in_string:
            if char == '{':
                brace_count += 1
            elif char == '}':
                brace_count -= 1
                
        current_record += char
        
        # When we complete a record
        if brace_count == 0 and current_record.strip().endswith('}'):
            record_str = current_record.strip()
            if record_str.startswith('{'):
                try:
                    # Clean the record string
                    record_str = clean_record(record_str)
                    record = json.loads(record_str)
                    records.append(record)
                except Exception as e:
                    print(f"Skipping malformed record: {e}")
                    print(f"Record preview: {record_str[:200]}...")
            current_record = ""
    
    return records

def clean_record(record_str):
    """Clean individual record string"""
    # Fix unquoted values
    record_str = re.sub(r':\s*([A-Za-z][A-Za-z0-9_\s]*),', r': "\1",', record_str)
    record_str = re.sub(r':\s*([A-Za-z][A-Za-z0-9_\s]*)\s*}', r': "\1"}', record_str)
    
    # Fix numeric values with units
    record_str = re.sub(r':\s*(\d+)\s+[a-zA-Z]+', r': \1', record_str)
    
    # Remove trailing commas
    record_str = re.sub(r',(\s*})', r'\1', record_str)
    
    return record_str

if __name__ == "__main__":
    print("Parsing JSON file...")
    records = parse_json_robust('/home/sai/data_engineer_assessment/data/fake_property_data_new.json')
    
    if records:
        print(f"Successfully parsed {len(records)} records")
        print("Sample record keys:", list(records[0].keys())[:10])
        
        # Save cleaned data
        with open('/home/sai/data_engineer_assessment/data/cleaned_data.json', 'w') as f:
            json.dump(records, f, indent=2)
        print("Saved cleaned data to cleaned_data.json")
    else:
        print("No records parsed successfully")
