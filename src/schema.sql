-- Normalized database schema based on Field Config.xlsx

CREATE TABLE IF NOT EXISTS property (
    property_id INT AUTO_INCREMENT PRIMARY KEY,
    property_title VARCHAR(500),
    address VARCHAR(500),
    market VARCHAR(100),
    flood VARCHAR(50),
    street_address VARCHAR(300),
    city VARCHAR(100),
    state VARCHAR(10),
    zip VARCHAR(20),
    property_type VARCHAR(50),
    highway VARCHAR(50),
    train VARCHAR(50),
    tax_rate DECIMAL(5,2),
    sqft_basement INT,
    htw VARCHAR(10),
    pool VARCHAR(10),
    commercial VARCHAR(10),
    water VARCHAR(50),
    sewage VARCHAR(50),
    year_built INT,
    sqft_mu INT,
    sqft_total INT,
    parking VARCHAR(50),
    bed INT,
    bath INT,
    basement_yes_no VARCHAR(10),
    layout VARCHAR(50),
    rent_restricted VARCHAR(10),
    neighborhood_rating INT,
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),
    subdivision VARCHAR(100),
    school_average DECIMAL(4,2)
);

CREATE TABLE IF NOT EXISTS leads (
    lead_id INT AUTO_INCREMENT PRIMARY KEY,
    property_id INT,
    reviewed_status VARCHAR(50),
    most_recent_status VARCHAR(50),
    source VARCHAR(100),
    occupancy VARCHAR(50),
    net_yield DECIMAL(8,2),
    irr DECIMAL(8,2),
    selling_reason VARCHAR(200),
    seller_retained_broker VARCHAR(200),
    final_reviewer VARCHAR(100),
    FOREIGN KEY (property_id) REFERENCES property(property_id)
);

CREATE TABLE IF NOT EXISTS valuation (
    valuation_id INT AUTO_INCREMENT PRIMARY KEY,
    property_id INT,
    previous_rent INT,
    list_price INT,
    zestimate INT,
    arv INT,
    expected_rent INT,
    rent_zestimate INT,
    low_fmr INT,
    high_fmr INT,
    redfin_value INT,
    FOREIGN KEY (property_id) REFERENCES property(property_id)
);

CREATE TABLE IF NOT EXISTS hoa (
    hoa_id INT AUTO_INCREMENT PRIMARY KEY,
    property_id INT,
    hoa INT,
    hoa_flag VARCHAR(10),
    FOREIGN KEY (property_id) REFERENCES property(property_id)
);

CREATE TABLE IF NOT EXISTS rehab (
    rehab_id INT AUTO_INCREMENT PRIMARY KEY,
    property_id INT,
    underwriting_rehab INT,
    rehab_calculation INT,
    paint VARCHAR(10),
    flooring_flag VARCHAR(10),
    foundation_flag VARCHAR(10),
    roof_flag VARCHAR(10),
    hvac_flag VARCHAR(10),
    kitchen_flag VARCHAR(10),
    bathroom_flag VARCHAR(10),
    appliances_flag VARCHAR(10),
    windows_flag VARCHAR(10),
    landscaping_flag VARCHAR(10),
    trashout_flag VARCHAR(10),
    FOREIGN KEY (property_id) REFERENCES property(property_id)
);

CREATE TABLE IF NOT EXISTS taxes (
    tax_id INT AUTO_INCREMENT PRIMARY KEY,
    property_id INT,
    taxes INT,
    FOREIGN KEY (property_id) REFERENCES property(property_id)
);
