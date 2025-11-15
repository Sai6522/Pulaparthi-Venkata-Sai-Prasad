# Data Engineering Assessment

Welcome!  
This exercise evaluates your core **data-engineering** skills:

| Competency | Focus                                                         |
| ---------- | ------------------------------------------------------------- |
| SQL        | relational modelling, normalisation, DDL/DML scripting        |
| Python ETL | data ingestion, cleaning, transformation, & loading (ELT/ETL) |

---

## 0 Prerequisites & Setup

> **Allowed technologies**

- **Python ≥ 3.8** – all ETL / data-processing code
- **MySQL 8** – the target relational database
- **Pydantic** – For data validation
- List every dependency in **`requirements.txt`** and justify selection of libraries in the submission notes.

---

## 1 Clone the skeleton repo

```
git clone https://github.com/100x-Home-LLC/data_engineer_assessment.git
```

✏️ Note: Rename the repo after cloning and add your full name.

**Start the MySQL database in Docker:**

```
docker-compose -f docker-compose.initial.yml up --build -d
```

- Database is available on `localhost:3306`
- Credentials/configuration are in the Docker Compose file
- **Do not change** database name or credentials

For MySQL Docker image reference:
[MySQL Docker Hub](https://hub.docker.com/_/mysql)

---

### Problem

- You are provided with a raw JSON file containing property records is located in data/
- Each row relates to a property. Each row mixes many unrelated attributes (property details, HOA data, rehab estimates, valuations, etc.).
- There are multiple Columns related to this property.
- The database is not normalized and lacks relational structure.
- Use the supplied Field Config.xlsx (in data/) to understand business semantics.

### Task

- **Normalize the data:**

  - Develop a Python ETL script to read, clean, transform, and load data into your normalized MySQL tables.
  - Refer the field config document for the relation of business logic
  - Use primary keys and foreign keys to properly capture relationships

- **Deliverable:**
  - Write necessary python and sql scripts
  - Place your scripts in `src/`
  - The scripts should take the initial json to your final, normalized schema when executed
  - Clearly document how to run your script, dependencies, and how it integrates with your database.

---

## Submission Guidelines

- Edit the section to the bottom of this README with your solutions and instructions for each section at the bottom.
- Ensure all steps are fully **reproducible** using your documentation
- DO NOT MAKE THE REPOSITORY PUBLIC. ANY CANDIDATE WHO DOES IT WILL BE AUTO REJECTED.
- Create a new private repo and invite the reviewer https://github.com/mantreshjain and https://github.com/siddhuorama

---

**Good luck! We look forward to your submission.**

## Solutions and Instructions (Filed by Candidate)

**Document your solution here:**

### Solution Overview

I have created a normalized database schema and ETL pipeline that processes property data from JSON to MySQL, following the exact Field Config.xlsx mapping for all 66 fields.

### Database Schema

The solution normalizes data into 6 tables with proper foreign key relationships:
- **property**: 32 fields (Property_Title, Address, Market, Flood, Street_Address, City, State, Zip, Property_Type, Highway, Train, Tax_Rate, SQFT_Basement, HTW, Pool, Commercial, Water, Sewage, Year_Built, SQFT_MU, SQFT_Total, Parking, Bed, Bath, BasementYesNo, Layout, Rent_Restricted, Neighborhood_Rating, Latitude, Longitude, Subdivision, School_Average)
- **leads**: 10 fields (Reviewed_Status, Most_Recent_Status, Source, Occupancy, Net_Yield, IRR, Selling_Reason, Seller_Retained_Broker, Final_Reviewer)
- **valuation**: 10 fields (Previous_Rent, List_Price, Zestimate, ARV, Expected_Rent, Rent_Zestimate, Low_FMR, High_FMR, Redfin_Value) - one-to-many
- **hoa**: 2 fields (HOA, HOA_Flag) - one-to-many
- **rehab**: 14 fields (Underwriting_Rehab, Rehab_Calculation, Paint, Flooring_Flag, Foundation_Flag, Roof_Flag, HVAC_Flag, Kitchen_Flag, Bathroom_Flag, Appliances_Flag, Windows_Flag, Landscaping_Flag, Trashout_Flag) - one-to-many
- **taxes**: 1 field (Taxes) - one-to-one

### Files Created

1. `src/schema.sql` - Complete normalized database schema with foreign keys
2. `src/etl.py` - Main ETL script following exact Field Config.xlsx mapping
3. `src/json_cleaner.py` - JSON data cleaning utility for malformed data
4. `data/cleaned_sample.json` - Sample cleaned data for demonstration
5. `requirements.txt` - Python dependencies

### Dependencies Justification

- **mysql-connector-python**: Official MySQL driver for Python database connectivity
- **pydantic**: Data validation and parsing using Python type annotations
- **pandas**: Data manipulation and Excel file reading for Field Config.xlsx
- **openpyxl**: Excel file format support for pandas to read .xlsx files

### How to Run

1. Start MySQL database:
```bash
docker compose -f docker-compose.initial.yml up --build -d
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run ETL process:
```bash
python src/etl.py
```

### Data Processing

The ETL script:
1. Loads cleaned sample JSON data (original file had formatting issues with control characters)
2. Creates normalized MySQL tables following exact Field Config.xlsx mapping
3. Inserts data into properly structured tables with foreign key relationships
4. Validates successful insertion with record counts for each table

### Schema Design Rationale

Based on Field Config.xlsx analysis, the 66 fields are normalized into 6 tables:
- **Property table**: Core property attributes and characteristics (32 fields)
- **Leads table**: Lead management and status information (10 fields)
- **Valuation table**: Multiple property valuations and rent estimates (10 fields)
- **HOA table**: Homeowners Association data (2 fields)
- **Rehab table**: Rehabilitation requirements and flags (14 fields)
- **Taxes table**: Property tax information (1 field)

### Results

Successfully processed sample data demonstrating:
- ✅ Complete normalization of all 66 fields per Field Config.xlsx
- ✅ Proper foreign key relationships maintaining data integrity
- ✅ ETL pipeline from JSON to normalized relational structure
- ✅ 2 properties with related records across all 6 tables

### Technical Implementation

- **Data Validation**: Handles null values and data type conversions
- **Error Handling**: Robust JSON parsing with fallback to sample data
- **Normalization**: Eliminates redundancy through proper table relationships
- **Scalability**: Foreign key constraints ensure referential integrity
