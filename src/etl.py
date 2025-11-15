#!/usr/bin/env python3
"""
ETL script following exact Field Config.xlsx mapping
"""
import json
import mysql.connector

def main():
    # Load sample data
    with open('/home/sai/data_engineer_assessment/data/cleaned_sample.json', 'r') as f:
        data = json.load(f)
    
    print(f"Loaded {len(data)} records")
    
    # Connect to database
    conn = mysql.connector.connect(
        host='localhost', port=3307, user='root', 
        password='6equj5_root', database='home_db'
    )
    cursor = conn.cursor()
    
    # Drop existing tables to recreate with correct schema
    cursor.execute("DROP TABLE IF EXISTS taxes")
    cursor.execute("DROP TABLE IF EXISTS rehab") 
    cursor.execute("DROP TABLE IF EXISTS hoa")
    cursor.execute("DROP TABLE IF EXISTS valuation")
    cursor.execute("DROP TABLE IF EXISTS leads")
    cursor.execute("DROP TABLE IF EXISTS property")
    
    # Create tables using schema.sql
    with open('/home/sai/data_engineer_assessment/src/schema.sql', 'r') as f:
        schema_sql = f.read()
    
    # Execute each CREATE TABLE statement
    statements = schema_sql.split(';')
    for statement in statements:
        if statement.strip() and 'CREATE TABLE' in statement:
            cursor.execute(statement)
    
    print("Tables created successfully")
    
    # Insert data following Field Config mapping
    for record in data:
        # Insert property (all property fields from Field Config)
        cursor.execute("""
            INSERT INTO property (
                property_title, address, market, flood, street_address, city, state, zip,
                property_type, highway, train, tax_rate, sqft_basement, htw, pool, commercial,
                water, sewage, year_built, sqft_mu, sqft_total, parking, bed, bath,
                basement_yes_no, layout, rent_restricted, neighborhood_rating,
                latitude, longitude, subdivision, school_average
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            record.get('Property_Title'), record.get('Address'), record.get('Market'),
            record.get('Flood'), record.get('Street_Address'), record.get('City'),
            record.get('State'), record.get('Zip'), record.get('Property_Type'),
            record.get('Highway'), record.get('Train'), record.get('Tax_Rate'),
            record.get('SQFT_Basement'), record.get('HTW'), record.get('Pool'),
            record.get('Commercial'), record.get('Water'), record.get('Sewage'),
            record.get('Year_Built'), record.get('SQFT_MU'), record.get('SQFT_Total'),
            record.get('Parking'), record.get('Bed'), record.get('Bath'),
            record.get('BasementYesNo'), record.get('Layout'), record.get('Rent_Restricted'),
            record.get('Neighborhood_Rating'), record.get('Latitude'), record.get('Longitude'),
            record.get('Subdivision'), record.get('School_Average')
        ))
        
        property_id = cursor.lastrowid
        
        # Insert leads (all leads fields from Field Config)
        cursor.execute("""
            INSERT INTO leads (
                property_id, reviewed_status, most_recent_status, source, occupancy,
                net_yield, irr, selling_reason, seller_retained_broker, final_reviewer
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            property_id, record.get('Reviewed_Status'), record.get('Most_Recent_Status'),
            record.get('Source'), record.get('Occupancy'), record.get('Net_Yield'),
            record.get('IRR'), record.get('Selling_Reason'), record.get('Seller_Retained_Broker'),
            record.get('Final_Reviewer')
        ))
        
        # Insert valuations (all valuation fields from Field Config)
        for val in record.get('Valuation', []):
            cursor.execute("""
                INSERT INTO valuation (
                    property_id, previous_rent, list_price, zestimate, arv, expected_rent,
                    rent_zestimate, low_fmr, high_fmr, redfin_value
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                property_id, val.get('Previous_Rent'), val.get('List_Price'),
                val.get('Zestimate'), val.get('ARV'), val.get('Expected_Rent'),
                val.get('Rent_Zestimate'), val.get('Low_FMR'), val.get('High_FMR'),
                val.get('Redfin_Value')
            ))
        
        # Insert HOA (all HOA fields from Field Config)
        for hoa in record.get('HOA', []):
            cursor.execute("""
                INSERT INTO hoa (property_id, hoa, hoa_flag) VALUES (%s,%s,%s)
            """, (property_id, hoa.get('HOA'), hoa.get('HOA_Flag')))
        
        # Insert rehab (all rehab fields from Field Config)
        for rehab in record.get('Rehab', []):
            cursor.execute("""
                INSERT INTO rehab (
                    property_id, underwriting_rehab, rehab_calculation, paint, flooring_flag,
                    foundation_flag, roof_flag, hvac_flag, kitchen_flag, bathroom_flag,
                    appliances_flag, windows_flag, landscaping_flag, trashout_flag
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                property_id, rehab.get('Underwriting_Rehab'), rehab.get('Rehab_Calculation'),
                rehab.get('Paint'), rehab.get('Flooring_Flag'), rehab.get('Foundation_Flag'),
                rehab.get('Roof_Flag'), rehab.get('HVAC_Flag'), rehab.get('Kitchen_Flag'),
                rehab.get('Bathroom_Flag'), rehab.get('Appliances_Flag'), rehab.get('Windows_Flag'),
                rehab.get('Landscaping_Flag'), rehab.get('Trashout_Flag')
            ))
        
        # Insert taxes (taxes field from Field Config)
        if record.get('Taxes'):
            cursor.execute("""
                INSERT INTO taxes (property_id, taxes) VALUES (%s,%s)
            """, (property_id, record.get('Taxes')))
    
    conn.commit()
    
    # Verify data
    cursor.execute("SELECT COUNT(*) FROM property")
    print(f"Properties inserted: {cursor.fetchone()[0]}")
    
    cursor.execute("SELECT COUNT(*) FROM valuation")
    print(f"Valuations inserted: {cursor.fetchone()[0]}")
    
    cursor.execute("SELECT COUNT(*) FROM leads")
    print(f"Leads inserted: {cursor.fetchone()[0]}")
    
    cursor.close()
    conn.close()
    
    print("ETL completed successfully!")

if __name__ == "__main__":
    main()
