#!/usr/bin/env python3
import json
import re

def fix_json_line(line):
    """Fix common JSON formatting issues in a line"""
    # Fix unquoted string values
    line = re.sub(r':\s*([A-Za-z][A-Za-z0-9_\s]*),', r': "\1",', line)
    line = re.sub(r':\s*([A-Za-z][A-Za-z0-9_\s]*)\s*}', r': "\1"}', line)
    
    # Fix numeric values with units (e.g., "1200 sqft" -> 1200)
    line = re.sub(r':\s*"?(\d+)\s+[a-zA-Z]+"?', r': \1', line)
    
    return line

def parse_json_file(file_path):
    """Parse the JSON file with error handling"""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Apply fixes
    lines = content.split('\n')
    fixed_lines = [fix_json_line(line) for line in lines]
    fixed_content = '\n'.join(fixed_lines)
    
    # Remove trailing commas
    fixed_content = re.sub(r',(\s*[}\]])', r'\1', fixed_content)
    
    try:
        return json.loads(fixed_content)
    except json.JSONDecodeError as e:
        print(f"JSON parsing failed: {e}")
        return []

if __name__ == "__main__":
    data = parse_json_file('/home/sai/data_engineer_assessment/data/fake_property_data_new.json')
    print(f"Parsed {len(data)} records")
    if data:
        print("Sample keys:", list(data[0].keys())[:10])
