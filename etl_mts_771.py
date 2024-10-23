import pandas as pd
import os
import traceback
import logging
from sqlalchemy import create_engine, Integer, TIMESTAMP, MetaData, Table, Column, Float, String, BigInteger
from sqlalchemy.sql import text
from datetime import datetime
from dotenv import load_dotenv

def setup_logging():
    # Get the current month and year
    current_month_year = datetime.now().strftime("%Y_%m")
    log_filename = f"etl_error_log_{current_month_year}.txt"  # Filename based on the current year and month

    # Configure logging to log to the specified file and include time, error level, and message
    logging.basicConfig(
        filename=log_filename,
        level=logging.DEBUG,  
        format='%(asctime)s\t%(levelname)s\t%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    
def initialize_metadata(etl_log_id, header_tstamp_first, station_name, test_file_name, table_name):
    global etl_metadata
    etl_metadata['etl_log_id'] = etl_log_id
    etl_metadata['header_tstamp_first'] = header_tstamp_first
    etl_metadata['station_name'] = station_name
    etl_metadata['test_file_name'] = test_file_name
    etl_metadata['table_name'] = table_name

load_dotenv()

# Call the setup_logging function to configure logging when the script starts
setup_logging()

etl_metadata = {
    'etl_log_id': 'N/A',
    'header_tstamp_first': 'N/A',
    'station_name': 'N/A',
    'test_file_name': 'N/A',
    'table_name': 'N/A'
}



# Function to read the last processed line from last_line.txt
def read_last_processed_line(filepath):
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r') as f:
                return int(f.read().strip())
        except ValueError:
            logging.warning(f"Warning: Could not read an integer from {filepath}. Defaulting to 0.")
            return 0
    else:
        logging.error(f"No file found at {filepath}")  # Log the error
        raise FileNotFoundError(f"{filepath} is missing. Please create or provide the file.")

# Function to save the last processed line to a text file
def save_last_processed_line(filepath, line_num):
    with open(filepath, 'w') as f:
        f.write(str(line_num))

# Function to read and increment the etl_log_id from a text file
def read_etl_log_id(log_file):
    if os.path.exists(log_file):
        with open(log_file, 'r') as f:
            return int(f.read())
    return 1  # Start from 1 if the file doesn't exist

# Function to save the next etl_log_id
def save_etl_log_id(log_file, etl_log_id):
    with open(log_file, 'w') as f:
        f.write(str(etl_log_id))

def append_to_log_file(etl_log_id, header_tstamp_first, station_name, test_file_name, table_name):
    # Get the current month and year
    current_month_year = datetime.now().strftime("%Y_%m")
    log_filename = f"etl_log_{current_month_year}.txt"  # Filename based on the current year and month

    # Check if the file exists
    file_exists = os.path.exists(log_filename)

    # Open the log file in append mode
    with open(log_filename, 'a') as log_file:
        # If the file doesn't exist, write the TSV header
        if not file_exists:
            log_file.write("id\tbegan_at_timestamp\theader_tstamp_first\tstation_name\ttest_file_name\ttable_name\n")
        
        # Append the log entry in TSV format, including the table_name
        timestamp_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_file.write(f"{etl_log_id}\t{timestamp_now}\t{header_tstamp_first}\t{station_name}\t{test_file_name}\t{table_name}\n")

# Function to determine the table name based on the folder name (last part of the folder)
def get_table_name_from_folder(folder_name):  
    if folder_name == "Table Top":
        return "table_top"
    elif folder_name == "T24-64":
        return "rotary"
    elif folder_name == "MTS 810":
        return "mts_810"
    elif folder_name == "placeholder":
        return "placeholder"
    else:
        error_message = f"Unrecognized folder name: {folder_name}"
        logging.error(error_message)
        raise ValueError(error_message)

# Function to extract column names and metadata from the file
def extract_columns_and_metadata(lines, last_line_processed):
    headers = []
    header_tstamp_first = None
    station_name = None
    test_file_name = None
    found_test_file = False

    # Start from the last processed line
    try:
        for i in range(last_line_processed, len(lines)):
            line = lines[i].strip()

            if "Data Header:" in line:
                # Extract the timestamp from the latest Data Header
                parts = line.split("\t")
                if len(parts) > 4:
                    timestamp_from_file = parts[-1].strip()
                    # Convert to SQL format
                    parsed_timestamp = datetime.strptime(timestamp_from_file, "%m/%d/%Y %I:%M:%S %p")
                    header_tstamp_first = parsed_timestamp.strftime("%Y-%m-%d %H:%M:%S")

            elif "Station Name:" in line:
                station_name = line.split(":")[1].strip()
            elif "Test File Name:" in line:
                test_file_name = line.split(":")[1].strip()
                found_test_file = True  # After this, we expect the headers to be in the next line
                continue

            # If we just found the test file name, expect the next line to be the headers
            if found_test_file and not headers:
                headers = line.split("\t")  # Use the next line as headers
                print(f"Detected headers: {headers}")  # DEBUG
                break  # We have found the headers, stop searching

        if not headers:
            logging.error("No valid headers found in the file.")
    
    except Exception as e:
        logging.error(f"Error during metadata extraction: {e}")
    
    return headers, header_tstamp_first, station_name, test_file_name

def process_data_file(lines, last_line_processed, etl_log_id, table_name):
    data = []
    most_recent_header_timestamp = None  # Track the most recent timestamp
    
    print(f"Successfully read {len(lines)} lines from the file.")
    
    # Extract headers and metadata
    headers, header_tstamp_first, station_name, test_file_name = extract_columns_and_metadata(lines, last_line_processed)

    if not headers:
        logging.error("No headers found in the file. (end of file?)")
        return pd.DataFrame(), last_line_processed, station_name

    initialize_metadata(etl_log_id, header_tstamp_first, station_name, test_file_name, table_name)

    # Log metadata once for the run
    append_to_log_file(etl_log_id, header_tstamp_first, station_name, test_file_name, table_name)

    in_data_section = False  # Track whether we are in the data section
    data_count = 0  # Track number of data lines processed
    skip_units_row = False  # Set a flag to skip the units row

    for i in range(last_line_processed, len(lines)):
        line = lines[i].strip()

        # Handle the most recent Data Header timestamp
        if "Data Header:" in line:
            # Extract the timestamp from the header
            parts = line.split("\t")
            if len(parts) > 4:
                timestamp_from_file = parts[-1].strip()
                # Convert to SQL format
                parsed_timestamp = datetime.strptime(timestamp_from_file, "%m/%d/%Y %I:%M:%S %p")
                most_recent_header_timestamp = parsed_timestamp.strftime("%Y-%m-%d %H:%M:%S")
            continue  # Continue to the next line, we're still processing metadata

        # Skip metadata sections until the data section begins
        if "Station Name:" in line or "Test File Name:" in line:
            in_data_section = False  # Still in metadata
            continue

        # Detect the actual header section and data following it
        if headers and not in_data_section:  # Start processing data after headers
            in_data_section = True
            skip_units_row = True  # We know the next line will be the units row, so we skip it
            continue

        # Skip the units line after the headers
        if skip_units_row:
            skip_units_row = False  # Reset the flag after skipping
            continue

        if in_data_section:
            columns = line.split("\t")  # Split using tab between columns

            # Ensure the number of columns matches the headers before adding the extra columns
            if len(columns) == len(headers):
                try:
                    # Convert the data to appropriate types (assuming floats for simplicity)
                    row_data = [float(c) for c in columns]
                    
                    # Insert etl_log_id and header_timestamp at the right positions
                    row_data.insert(0, etl_log_id)  # Insert etl_log_id as first column
                    row_data.insert(1, most_recent_header_timestamp)  # Insert most recent header timestamp as second column
                    row_data.insert(2, station_name)
                    
                    data.append(row_data)
                    data_count += 1  # Increment the number of data lines processed
                except ValueError as ve:
                    logging.error(f"Skipping line {i + 1} due to ValueError: {ve}")
    
    # Add 3 extra headers for the new columns: etl_log_id, header_timestamp
    headers = ['etl_log_id', 'header_timestamp', 'station_name'] + headers  

    # Create DataFrame and return it
    df = pd.DataFrame(data, columns=headers)

    print(f"Total rows processed: {len(df)}")

    return df, len(lines), station_name

def create_table_if_not_exists(engine, table_name, df):
    metadata = MetaData()

    # Define dynamic columns based on DataFrame
    columns = [Column('id', BigInteger, primary_key=True)]
    for col in df.columns:
        if col not in ['id']:
            if col == "header_timestamp":
                columns.append(Column(col, TIMESTAMP))
            elif col == "etl_log_id":
                columns.append(Column(col, Integer))
            elif col == "station_name":
                columns.append(Column(col, String))
            else:
                columns.append(Column(col, Float))  # Assume float for other columns
    
    # Step 1: Create the table if it does not exist
    table = Table(table_name, metadata, *columns)
    metadata.create_all(engine)

    # Hardcoded start values for sequences
    start_values = {
        'table_top': 1,
        'rotary': 2 * 10**18,
        'mts_810': 3 * 10**18,
        'placeholder': 4 * 10**18
    }

    start_value = start_values.get(table_name, 1)
    sequence_name = f"{table_name}_id_seq"

    with engine.connect() as conn:
        # Check if sequence exists
        sequence_check_query = f"SELECT 1 FROM pg_class WHERE relname = '{sequence_name}' AND relkind = 'S'"
        sequence_exists = conn.execute(text(sequence_check_query)).fetchone()

        if not sequence_exists:
            print(f"Creating sequence: {sequence_name}")
            conn.execute(text(f"CREATE SEQUENCE {sequence_name} START WITH {start_value};"))
        else:
            print(f"Sequence {sequence_name} already exists, not restarting.")

        # Commit to persist the changes
        conn.commit()

        # Attach the sequence to the id column, but only if it's not already attached
        try:
            conn.execute(text(f"ALTER TABLE \"{table_name}\" ALTER COLUMN id SET DEFAULT nextval('{sequence_name}');"))
        except Exception as e:
            print(f"Error attaching sequence to id column: {e}")

    print(f"Table '{table_name}' is ready.")

# Function to upload data to the PostgreSQL database using SQLAlchemy
def upload_to_database(df, table_name):
    if df.empty:
        print("No data to upload.")
        return False

    try:
        db_username = os.getenv('DB_USERNAME')
        db_password = os.getenv('DB_PASSWORD')
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        db_name = os.getenv('DB_NAME')

        print(f"Attempting to connect to the database and upload to table: {table_name}...")
        engine = create_engine(f'postgresql+psycopg2://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')
        print("Successfully connected to the database.")

        # Drop 'id' from the DataFrame to prevent it from interfering with autoincrement in the DB
        df = df.drop(columns=['id'], errors='ignore')
        
        # Create table using MetaData and Table
        create_table_if_not_exists(engine, table_name, df)

        print(f"Inserting data into the table {table_name}...")
        df.to_sql(table_name, engine, if_exists='append', index=False, chunksize = 10000, dtype={'etl_log_id': Integer(), 'header_timestamp': TIMESTAMP()})
        print(f"Inserted {len(df)} rows into the {table_name} table.")
        return True

    except Exception as e:
        print(f"An error occurred during database interaction: {e}")
        logging.error(f"An error occurred during database interaction: {e}")
        return False

if __name__ == "__main__":
    # input_file = r'C:\MTS 793\Projects\Project1\Current\Table Top\input_data.dat' 
    input_file = r'C:\MTS 793\Projects\Project1\Current\T24-64\ab1v  rt5 input carrier 2006 nm oct 16 2024.dat'
    # input_file = r'C:\MTS 793\Projects\Project1\Current\MTS 810\t24-62 10 shaft torque fatigue test t2.dat'
    # input_file = r'C:\MTS 793\Projects\Project1\Current\temp\temp.dat'

    last_line_file = 'last_line.txt'
    etl_log_file = 'etl_log_id.txt'

    folder_name = os.path.basename(os.path.dirname(input_file))
    
    # Determine the table name based on the folder name
    try:
        table_name = get_table_name_from_folder(folder_name)
        print(f"Table name determined from folder: {table_name}")
    except ValueError as e:
        logging.critical(str(e))
        exit(1)  # Exit if an unrecognized folder name is provided

    
    if os.path.exists(input_file):
        print(f"File found: {input_file}")
        # Proceed to open and read the file
        with open(input_file, 'r') as infile:
            lines = infile.readlines()
            print(f"Read {len(lines)} lines from the file.")
    else:
        logging.error(f"File not found: {input_file}")
        exit(1)

    # Read last processed line
    last_line = read_last_processed_line(last_line_file)

    # Read current etl_log_id and increment it after the run
    etl_log_id = read_etl_log_id(etl_log_file)

    # Read the file contents once and reuse it
    with open(input_file, 'r') as infile:
        lines = infile.readlines()

    # Process the .dat file and convert to DataFrame
    df, last_processed_line, station_name = process_data_file(lines, last_line, etl_log_id, table_name)

    if not df.empty and upload_to_database(df, table_name):  
        save_last_processed_line(last_line_file, last_processed_line) # Only update last_line.txt if data was successfully inserted into the database

    # Increment etl_log_id for the next run and save it
    etl_log_id += 1
    save_etl_log_id(etl_log_file, etl_log_id)
