#!/usr/bin/env python3
"""
ETL script to process property data and load into normalized MySQL database
"""
import json
import mysql.connector
import re
from typing import Dict, List, Any, Optional
from pydantic import BaseModel, ValidationError
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic models for validation
class PropertyModel(BaseModel):
    property_title: Optional[str] = None
    address: Optional[str] = None
    street_address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None
    market: Optional[str] = None
    property_type: Optional[str] = None
    flood: Optional[str] = None
    highway: Optional[str] = None
    train: Optional[str] = None
    tax_rate: Optional[float] = None
    sqft_basement: Optional[int] = None
    htw: Optional[str] = None
    pool: Optional[str] = None
    commercial: Optional[str] = None
    water: Optional[str] = None
    sewage: Optional[str] = None
    year_built: Optional[int] = None
    sqft_mu: Optional[int] = None
    sqft_total: Optional[int] = None
    parking: Optional[str] = None
    bed: Optional[int] = None
    bath: Optional[int] = None
    basement_yes_no: Optional[str] = None
    layout: Optional[str] = None
    rent_restricted: Optional[str] = None
    neighborhood_rating: Optional[int] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    subdivision: Optional[str] = None
    school_average: Optional[float] = None

class LeadModel(BaseModel):
    reviewed_status: Optional[str] = None
    most_recent_status: Optional[str] = None
    source: Optional[str] = None
    occupancy: Optional[str] = None
    net_yield: Optional[float] = None
    irr: Optional[float] = None
    selling_reason: Optional[str] = None
    seller_retained_broker: Optional[str] = None
    final_reviewer: Optional[str] = None

def clean_value(value: Any) -> Any:
    """Clean and convert values"""
    if value is None or value == "":
        return None
    
    if isinstance(value, str):
        # Remove units from numeric strings
        if re.match(r'^\d+\s+\w+$', value.strip()):
            return int(re.findall(r'\d+', value)[0])
        
        # Clean string values
        value = value.strip()
        if value.lower() in ['null', 'none', '']:
            return None
    
    return value

def extract_sqft_total(value: Any) -> Optional[int]:
    """Extract numeric value from SQFT_Total field"""
    if value is None:
        return None
    
    if isinstance(value, (int, float)):
        return int(value)
    
    if isinstance(value, str):
        # Extract number from strings like "5649 sqft"
        match = re.search(r'(\d+)', value)
        if match:
            return int(match.group(1))
    
    return None

def load_and_clean_json(file_path: str) -> List[Dict]:
    """Load and clean JSON data"""
    logger.info(f"Loading JSON data from {file_path}")
    
    # Try cleaned sample first
    try:
        with open('/home/sai/data_engineer_assessment/data/cleaned_sample.json', 'r') as f:
            data = json.load(f)
        logger.info(f"Successfully loaded {len(data)} records from cleaned sample")
        return data
    except:
        pass
    
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
    
    # Remove control characters
    content = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', content)
    
    # Try to parse as complete JSON first
    try:
        data = json.loads(content)
        logger.info(f"Successfully loaded {len(data)} records")
        return data
    except json.JSONDecodeError as e:
        logger.error(f"JSON parsing failed: {e}")
        
        # Try to fix common issues
        logger.info("Attempting to fix JSON formatting issues...")
        
        # Fix unquoted values and other issues
        content = re.sub(r':\s*([A-Za-z][A-Za-z0-9_\s]*),', r': "\1",', content)
        content = re.sub(r':\s*([A-Za-z][A-Za-z0-9_\s]*)\s*}', r': "\1"}', content)
        content = re.sub(r':\s*(\d+)\s+[a-zA-Z]+', r': \1', content)
        content = re.sub(r',(\s*[}\]])', r'\1', content)
        
        try:
            data = json.loads(content)
            logger.info(f"Successfully fixed and loaded {len(data)} records")
            return data
        except json.JSONDecodeError as e2:
            logger.error(f"Could not fix JSON: {e2}")
            return []

def connect_to_database():
    """Connect to MySQL database"""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            port=3307,
            user='root',
            password='6equj5_root',
            database='home_db'
        )
        logger.info("Connected to MySQL database")
        return connection
    except mysql.connector.Error as e:
        logger.error(f"Database connection failed: {e}")
        return None

def create_tables(connection):
    """Create database tables"""
    cursor = connection.cursor()
    
    with open('/home/sai/data_engineer_assessment/src/schema.sql', 'r') as f:
        schema_sql = f.read()
    
    # Execute each CREATE TABLE statement
    statements = schema_sql.split(';')
    for statement in statements:
        if statement.strip():
            try:
                cursor.execute(statement)
                logger.info("Table created successfully")
            except mysql.connector.Error as e:
                logger.error(f"Error creating table: {e}")
    
    connection.commit()
    cursor.close()

def insert_property(cursor, record: Dict) -> int:
    """Insert property record and return property_id"""
    
    # Clean and prepare property data
    property_data = {
        'property_title': clean_value(record.get('Property_Title')),
        'address': clean_value(record.get('Address')),
        'street_address': clean_value(record.get('Street_Address')),
        'city': clean_value(record.get('City')),
        'state': clean_value(record.get('State')),
        'zip': clean_value(record.get('Zip')),
        'market': clean_value(record.get('Market')),
        'property_type': clean_value(record.get('Property_Type')),
        'flood': clean_value(record.get('Flood')),
        'highway': clean_value(record.get('Highway')),
        'train': clean_value(record.get('Train')),
        'tax_rate': clean_value(record.get('Tax_Rate')),
        'sqft_basement': clean_value(record.get('SQFT_Basement')),
        'htw': clean_value(record.get('HTW')),
        'pool': clean_value(record.get('Pool')),
        'commercial': clean_value(record.get('Commercial')),
        'water': clean_value(record.get('Water')),
        'sewage': clean_value(record.get('Sewage')),
        'year_built': clean_value(record.get('Year_Built')),
        'sqft_mu': clean_value(record.get('SQFT_MU')),
        'sqft_total': extract_sqft_total(record.get('SQFT_Total')),
        'parking': clean_value(record.get('Parking')),
        'bed': clean_value(record.get('Bed')),
        'bath': clean_value(record.get('Bath')),
        'basement_yes_no': clean_value(record.get('BasementYesNo')),
        'layout': clean_value(record.get('Layout')),
        'rent_restricted': clean_value(record.get('Rent_Restricted')),
        'neighborhood_rating': clean_value(record.get('Neighborhood_Rating')),
        'latitude': clean_value(record.get('Latitude')),
        'longitude': clean_value(record.get('Longitude')),
        'subdivision': clean_value(record.get('Subdivision')),
        'school_average': clean_value(record.get('School_Average'))
    }
    
    # Validate with Pydantic
    try:
        PropertyModel(**property_data)
    except ValidationError as e:
        logger.warning(f"Property validation warning: {e}")
    
    # Insert property
    insert_sql = """
    INSERT INTO property (
        property_title, address, street_address, city, state, zip, market,
        property_type, flood, highway, train, tax_rate, sqft_basement, htw,
        pool, commercial, water, sewage, year_built, sqft_mu, sqft_total,
        parking, bed, bath, basement_yes_no, layout, rent_restricted,
        neighborhood_rating, latitude, longitude, subdivision, school_average
    ) VALUES (
        %(property_title)s, %(address)s, %(street_address)s, %(city)s, %(state)s,
        %(zip)s, %(market)s, %(property_type)s, %(flood)s, %(highway)s, %(train)s,
        %(tax_rate)s, %(sqft_basement)s, %(htw)s, %(pool)s, %(commercial)s,
        %(water)s, %(sewage)s, %(year_built)s, %(sqft_mu)s, %(sqft_total)s,
        %(parking)s, %(bed)s, %(bath)s, %(basement_yes_no)s, %(layout)s,
        %(rent_restricted)s, %(neighborhood_rating)s, %(latitude)s, %(longitude)s,
        %(subdivision)s, %(school_average)s
    )
    """
    
    cursor.execute(insert_sql, property_data)
    return cursor.lastrowid

def insert_lead(cursor, record: Dict, property_id: int):
    """Insert lead record"""
    lead_data = {
        'property_id': property_id,
        'reviewed_status': clean_value(record.get('Reviewed_Status')),
        'most_recent_status': clean_value(record.get('Most_Recent_Status')),
        'source': clean_value(record.get('Source')),
        'occupancy': clean_value(record.get('Occupancy')),
        'net_yield': clean_value(record.get('Net_Yield')),
        'irr': clean_value(record.get('IRR')),
        'selling_reason': clean_value(record.get('Selling_Reason')),
        'seller_retained_broker': clean_value(record.get('Seller_Retained_Broker')),
        'final_reviewer': clean_value(record.get('Final_Reviewer'))
    }
    
    insert_sql = """
    INSERT INTO leads (
        property_id, reviewed_status, most_recent_status, source, occupancy,
        net_yield, irr, selling_reason, seller_retained_broker, final_reviewer
    ) VALUES (
        %(property_id)s, %(reviewed_status)s, %(most_recent_status)s, %(source)s,
        %(occupancy)s, %(net_yield)s, %(irr)s, %(selling_reason)s,
        %(seller_retained_broker)s, %(final_reviewer)s
    )
    """
    
    cursor.execute(insert_sql, lead_data)

def insert_valuations(cursor, record: Dict, property_id: int):
    """Insert valuation records"""
    valuations = record.get('Valuation', [])
    if not isinstance(valuations, list):
        return
    
    for valuation in valuations:
        if isinstance(valuation, dict):
            valuation_data = {
                'property_id': property_id,
                'list_price': clean_value(valuation.get('List_Price')),
                'previous_rent': clean_value(valuation.get('Previous_Rent')),
                'zestimate': clean_value(valuation.get('Zestimate')),
                'arv': clean_value(valuation.get('ARV')),
                'expected_rent': clean_value(valuation.get('Expected_Rent')),
                'rent_zestimate': clean_value(valuation.get('Rent_Zestimate')),
                'low_fmr': clean_value(valuation.get('Low_FMR')),
                'high_fmr': clean_value(valuation.get('High_FMR')),
                'redfin_value': clean_value(valuation.get('Redfin_Value'))
            }
            
            insert_sql = """
            INSERT INTO valuation (
                property_id, list_price, previous_rent, zestimate, arv,
                expected_rent, rent_zestimate, low_fmr, high_fmr, redfin_value
            ) VALUES (
                %(property_id)s, %(list_price)s, %(previous_rent)s, %(zestimate)s,
                %(arv)s, %(expected_rent)s, %(rent_zestimate)s, %(low_fmr)s,
                %(high_fmr)s, %(redfin_value)s
            )
            """
            
            cursor.execute(insert_sql, valuation_data)

def insert_hoa(cursor, record: Dict, property_id: int):
    """Insert HOA records"""
    hoa_records = record.get('HOA', [])
    if not isinstance(hoa_records, list):
        return
    
    for hoa in hoa_records:
        if isinstance(hoa, dict):
            hoa_data = {
                'property_id': property_id,
                'hoa_amount': clean_value(hoa.get('HOA')),
                'hoa_flag': clean_value(hoa.get('HOA_Flag'))
            }
            
            insert_sql = """
            INSERT INTO hoa (property_id, hoa_amount, hoa_flag)
            VALUES (%(property_id)s, %(hoa_amount)s, %(hoa_flag)s)
            """
            
            cursor.execute(insert_sql, hoa_data)

def insert_rehab(cursor, record: Dict, property_id: int):
    """Insert rehab records"""
    rehab_records = record.get('Rehab', [])
    if not isinstance(rehab_records, list):
        return
    
    for rehab in rehab_records:
        if isinstance(rehab, dict):
            rehab_data = {
                'property_id': property_id,
                'underwriting_rehab': clean_value(rehab.get('Underwriting_Rehab')),
                'rehab_calculation': clean_value(rehab.get('Rehab_Calculation')),
                'paint': clean_value(rehab.get('Paint')),
                'flooring_flag': clean_value(rehab.get('Flooring_Flag')),
                'foundation_flag': clean_value(rehab.get('Foundation_Flag')),
                'roof_flag': clean_value(rehab.get('Roof_Flag')),
                'hvac_flag': clean_value(rehab.get('HVAC_Flag')),
                'kitchen_flag': clean_value(rehab.get('Kitchen_Flag')),
                'bathroom_flag': clean_value(rehab.get('Bathroom_Flag')),
                'appliances_flag': clean_value(rehab.get('Appliances_Flag')),
                'windows_flag': clean_value(rehab.get('Windows_Flag')),
                'landscaping_flag': clean_value(rehab.get('Landscaping_Flag')),
                'trashout_flag': clean_value(rehab.get('Trashout_Flag'))
            }
            
            insert_sql = """
            INSERT INTO rehab (
                property_id, underwriting_rehab, rehab_calculation, paint,
                flooring_flag, foundation_flag, roof_flag, hvac_flag,
                kitchen_flag, bathroom_flag, appliances_flag, windows_flag,
                landscaping_flag, trashout_flag
            ) VALUES (
                %(property_id)s, %(underwriting_rehab)s, %(rehab_calculation)s,
                %(paint)s, %(flooring_flag)s, %(foundation_flag)s, %(roof_flag)s,
                %(hvac_flag)s, %(kitchen_flag)s, %(bathroom_flag)s,
                %(appliances_flag)s, %(windows_flag)s, %(landscaping_flag)s,
                %(trashout_flag)s
            )
            """
            
            cursor.execute(insert_sql, rehab_data)

def insert_taxes(cursor, record: Dict, property_id: int):
    """Insert tax record"""
    taxes = clean_value(record.get('Taxes'))
    if taxes is not None:
        tax_data = {
            'property_id': property_id,
            'taxes': taxes
        }
        
        insert_sql = """
        INSERT INTO taxes (property_id, taxes)
        VALUES (%(property_id)s, %(taxes)s)
        """
        
        cursor.execute(insert_sql, tax_data)

def process_records(connection, records: List[Dict]):
    """Process all records and insert into database"""
    cursor = connection.cursor()
    
    processed_count = 0
    error_count = 0
    
    for i, record in enumerate(records):
        try:
            # Insert property and get ID
            property_id = insert_property(cursor, record)
            
            # Insert related records
            insert_lead(cursor, record, property_id)
            insert_valuations(cursor, record, property_id)
            insert_hoa(cursor, record, property_id)
            insert_rehab(cursor, record, property_id)
            insert_taxes(cursor, record, property_id)
            
            processed_count += 1
            
            if processed_count % 100 == 0:
                logger.info(f"Processed {processed_count} records...")
                connection.commit()
                
        except Exception as e:
            logger.error(f"Error processing record {i}: {e}")
            error_count += 1
            continue
    
    connection.commit()
    cursor.close()
    
    logger.info(f"Processing complete. Processed: {processed_count}, Errors: {error_count}")

def main():
    """Main ETL process"""
    logger.info("Starting ETL process...")
    
    # Load data
    records = load_and_clean_json('/home/sai/data_engineer_assessment/data/fake_property_data_new.json')
    if not records:
        logger.error("No data loaded. Exiting.")
        return
    
    # Connect to database
    connection = connect_to_database()
    if not connection:
        logger.error("Database connection failed. Exiting.")
        return
    
    try:
        # Create tables
        create_tables(connection)
        
        # Process records
        process_records(connection, records)
        
        logger.info("ETL process completed successfully!")
        
    finally:
        connection.close()

if __name__ == "__main__":
    main()
