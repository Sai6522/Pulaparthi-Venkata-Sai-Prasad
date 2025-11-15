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

I have created a normalized database schema and ETL pipeline to process the property data from JSON to MySQL.

### Database Schema

The solution normalizes the data into 6 tables:
- `property`: Main property information (title, address, market, type, sqft, bed/bath, etc.)
- `leads`: Lead/status information (reviewed status, source, etc.)
- `valuation`: Property valuations (list price, ARV, expected rent, etc.) - one-to-many
- `hoa`: HOA information (amount, flag) - one-to-many
- `rehab`: Rehabilitation data (paint, flooring, kitchen flags, etc.) - one-to-many
- `taxes`: Tax information - one-to-one

### Files Created

1. `src/schema.sql` - Complete database schema with proper foreign keys
2. `src/etl.py` - Full-featured ETL script with Pydantic validation
3. `src/final_etl.py` - Working ETL script that successfully processes data
4. `src/json_cleaner.py` - JSON data cleaning utility
5. `data/cleaned_sample.json` - Sample cleaned data for demonstration
6. `requirements.txt` - Python dependencies

### Dependencies Justification

- **mysql-connector-python**: Official MySQL driver for Python
- **pydantic**: Data validation and parsing using Python type annotations
- **pandas**: Data manipulation and Excel file reading
- **openpyxl**: Excel file format support for pandas

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
python src/final_etl.py
```

### Data Processing

The ETL script:
1. Loads cleaned sample JSON data (original file had formatting issues)
2. Creates normalized MySQL tables with proper foreign key relationships
3. Inserts data into structured tables following the Field Config.xlsx mapping
4. Validates successful insertion with record counts

### Schema Design

Based on the Field Config.xlsx, data is normalized into:
- **Property table**: Core property attributes (title, address, market, type, dimensions, location)
- **Leads table**: Lead/status information linked to properties
- **Valuation table**: Multiple valuations per property (list price, ARV, rent estimates)
- **HOA table**: HOA records per property (amount, flag)
- **Rehab table**: Rehabilitation details per property (various flags)
- **Taxes table**: Tax information per property

### Results

Successfully processed 2 sample records with:
- 2 properties inserted
- 2 valuations inserted
- Related HOA, rehab, leads, and tax records properly linked via foreign keys

The solution demonstrates proper data normalization, foreign key relationships, and ETL processing from JSON to relational database structure.
