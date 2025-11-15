#!/usr/bin/env python3
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
    
    # Drop existing tables to recreate
    cursor.execute("DROP TABLE IF EXISTS taxes")
    cursor.execute("DROP TABLE IF EXISTS rehab") 
    cursor.execute("DROP TABLE IF EXISTS hoa")
    cursor.execute("DROP TABLE IF EXISTS valuation")
    cursor.execute("DROP TABLE IF EXISTS leads")
    cursor.execute("DROP TABLE IF EXISTS property")
    
    # Create tables
    cursor.execute("""
        CREATE TABLE property (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(500), address VARCHAR(500), market VARCHAR(100),
            property_type VARCHAR(50), sqft_total INT, bed INT, bath INT,
            year_built INT, latitude DECIMAL(10,8), longitude DECIMAL(11,8)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE valuation (
            id INT AUTO_INCREMENT PRIMARY KEY, property_id INT,
            list_price INT, arv INT, expected_rent INT, zestimate INT,
            FOREIGN KEY (property_id) REFERENCES property(id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE hoa (
            id INT AUTO_INCREMENT PRIMARY KEY, property_id INT,
            amount INT, flag VARCHAR(10),
            FOREIGN KEY (property_id) REFERENCES property(id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE rehab (
            id INT AUTO_INCREMENT PRIMARY KEY, property_id INT,
            paint VARCHAR(10), flooring_flag VARCHAR(10), kitchen_flag VARCHAR(10),
            FOREIGN KEY (property_id) REFERENCES property(id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE leads (
            id INT AUTO_INCREMENT PRIMARY KEY, property_id INT,
            reviewed_status VARCHAR(50), most_recent_status VARCHAR(50), source VARCHAR(100),
            FOREIGN KEY (property_id) REFERENCES property(id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE taxes (
            id INT AUTO_INCREMENT PRIMARY KEY, property_id INT, amount INT,
            FOREIGN KEY (property_id) REFERENCES property(id)
        )
    """)
    
    print("Tables created successfully")
    
    # Insert data
    for record in data:
        # Insert property
        cursor.execute("""
            INSERT INTO property (title, address, market, property_type, sqft_total, bed, bath, year_built, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            record.get('Property_Title'),
            record.get('Address'), 
            record.get('Market'),
            record.get('Property_Type'),
            record.get('SQFT_Total'),
            record.get('Bed'),
            record.get('Bath'),
            record.get('Year_Built'),
            record.get('Latitude'),
            record.get('Longitude')
        ))
        
        property_id = cursor.lastrowid
        
        # Insert leads
        cursor.execute("""
            INSERT INTO leads (property_id, reviewed_status, most_recent_status, source)
            VALUES (%s, %s, %s, %s)
        """, (property_id, record.get('Reviewed_Status'), record.get('Most_Recent_Status'), record.get('Source')))
        
        # Insert valuations
        for val in record.get('Valuation', []):
            cursor.execute("""
                INSERT INTO valuation (property_id, list_price, arv, expected_rent, zestimate)
                VALUES (%s, %s, %s, %s, %s)
            """, (property_id, val.get('List_Price'), val.get('ARV'), val.get('Expected_Rent'), val.get('Zestimate')))
        
        # Insert HOA
        for hoa in record.get('HOA', []):
            cursor.execute("""
                INSERT INTO hoa (property_id, amount, flag) VALUES (%s, %s, %s)
            """, (property_id, hoa.get('HOA'), hoa.get('HOA_Flag')))
        
        # Insert rehab
        for rehab in record.get('Rehab', []):
            cursor.execute("""
                INSERT INTO rehab (property_id, paint, flooring_flag, kitchen_flag)
                VALUES (%s, %s, %s, %s)
            """, (property_id, rehab.get('Paint'), rehab.get('Flooring_Flag'), rehab.get('Kitchen_Flag')))
        
        # Insert taxes
        if record.get('Taxes'):
            cursor.execute("""
                INSERT INTO taxes (property_id, amount) VALUES (%s, %s)
            """, (property_id, record.get('Taxes')))
    
    conn.commit()
    
    # Verify data
    cursor.execute("SELECT COUNT(*) FROM property")
    print(f"Properties inserted: {cursor.fetchone()[0]}")
    
    cursor.execute("SELECT COUNT(*) FROM valuation")
    print(f"Valuations inserted: {cursor.fetchone()[0]}")
    
    cursor.close()
    conn.close()
    
    print("ETL completed successfully!")

if __name__ == "__main__":
    main()
