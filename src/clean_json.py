#!/usr/bin/env python3
"""
Clean and fix the JSON data file
"""
import json
import re

def clean_json_file(input_file, output_file):
    """Clean the malformed JSON file"""
    with open(input_file, 'r') as f:
        content = f.read()
    
    # Fix common JSON formatting issues
    # Fix unquoted values
    content = re.sub(r': ([A-Za-z][A-Za-z0-9_]*),', r': "\1",', content)
    content = re.sub(r': ([A-Za-z][A-Za-z0-9_]*)\s*}', r': "\1"}', content)
    content = re.sub(r': ([A-Za-z][A-Za-z0-9_]*)\s*\n', r': "\1"\n', content)
    
    # Fix values with units (like "9191 sqfts")
    content = re.sub(r': (\d+)\s+[a-zA-Z]+,', r': \1,', content)
    content = re.sub(r': (\d+)\s+[a-zA-Z]+\s*}', r': \1}', content)
    
    # Remove trailing commas
    content = re.sub(r',(\s*[}\]])', r'\1', content)
    
    try:
        data = json.loads(content)
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Successfully cleaned and saved {len(data)} records to {output_file}")
        return data
    except Exception as e:
        print(f"Error: {e}")
        return None

if __name__ == "__main__":
    data = clean_json_file(
        '/home/sai/data_engineer_assessment/data/fake_property_data_new.json',
        '/home/sai/data_engineer_assessment/data/cleaned_data.json'
    )
