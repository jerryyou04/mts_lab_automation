import pandas as pd
import os
import traceback
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Function to read the last processed line from a text file
def read_last_processed_line(filepath):
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r') as f:
                return int(f.read().strip())
        except ValueError:
            print(f"Warning: Could not read an integer from {filepath}. Defaulting to 0.")
    print(f"No file found")
    return 0


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

def append_to_log_file(etl_log_id, header_tstamp_first, station_name, test_file_name):
    # Get the current month and year
    current_month_year = datetime.now().strftime("%Y_%m")
    log_filename = f"etl_log_{current_month_year}.txt"  # Filename based on the current year and month

    # Check if the file exists
    file_exists = os.path.exists(log_filename)

    # Open the log file in append mode
    with open(log_filename, 'a') as log_file:
        # If the file doesn't exist, write the TSV header
        if not file_exists:
            log_file.write("id\tbegan_at_timestamp\theader_tstamp_first\tstation_name\ttest_file_name\n")
        
        # Append the log entry in TSV format
        timestamp_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_file.write(f"{etl_log_id}\t{timestamp_now}\t{header_tstamp_first}\t{station_name}\t{test_file_name}\n")

# Function to determine the table name based on the station name
def get_table_name_from_station(station_name):
    if "table" in station_name.lower():
        return "table_top"
    elif "810" in station_name:
        return "mts_810"
    elif "slot" in station_name.lower():
        return "t_slot"
    elif "rotary" in station_name.lower():
        return "rotary"
    else:
        raise ValueError(f"Unrecognized station name: {station_name}")

# Function to extract column names and metadata from the file
def extract_columns_and_metadata(lines, last_line_processed):
    headers = []
    header_tstamp_first = None
    station_name = None
    test_file_name = None
    found_test_file = False

    # Start from the last processed line
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
        print("Error: No valid headers found.")
    
    return headers, header_tstamp_first, station_name, test_file_name





# Function to process the .dat file and convert it into a pandas DataFrame
def process_data_file(lines, last_line_processed, etl_log_id):
    data = []
    
    print(f"Successfully read {len(lines)} lines from the file.")
    
    headers, header_tstamp_first, station_name, test_file_name = extract_columns_and_metadata(lines, last_line_processed)

    if not headers:
        print("Error: No headers found in the file.")
        return pd.DataFrame(), last_line_processed

    # Log metadata once for the run
    append_to_log_file(etl_log_id, header_tstamp_first, station_name, test_file_name)

    in_data_section = False  # Track whether we are in the data section
    data_count = 0  # Track number of data lines processed

    skip_units_row = False  # Set a flag to skip the units row

    for i in range(last_line_processed, len(lines)):
        line = lines[i].strip()


        # Skip metadata sections until the data section begins
        if "Data Header:" in line or "Station Name:" in line or "Test File Name:" in line:
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

            if len(columns) == len(headers):  # Ensure the number of columns matches the headers
                try:
                    # Convert the data to appropriate types (assuming floats for simplicity)
                    row_data = [float(c) for c in columns]
                    row_data.append(etl_log_id)  # Add etl_log_id to each data row
                    data.append(row_data)
                    data_count += 1  # Increment the number of data lines processed
                except ValueError as ve:
                    print(f"Skipping line {i + 1} due to ValueError: {ve}")
                    print(f"Offending line: {line}")

    headers.append("etl_log_id")  # Add the etl_log_id column to the DataFrame headers
    df = pd.DataFrame(data, columns=headers)

    print(f"Total rows processed: {len(df)}")

    return df, len(lines), station_name  # Also return the last line number to update last_line.txt after DB insertion

# Function to upload data to the PostgreSQL database using SQLAlchemy
def upload_to_database(df, station_name):
    if df.empty:
        print("No data to upload.")
        return False

    try:
        # Get the appropriate table name based on the station name
        db_username = os.getenv('DB_USERNAME')
        db_password = os.getenv('DB_PASSWORD')
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        db_name = os.getenv('DB_NAME')

        if not all([db_username, db_password, db_host, db_port, db_name]):
            raise EnvironmentError("Missing one or more required environment variables for the database connection")

        table_name = get_table_name_from_station(station_name)

        print(f"Attempting to connect to the database and upload to table: {table_name}...")
        engine = create_engine(f'postgresql+psycopg2://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')
        print("Successfully connected to the database.")

        print(f"Inserting data into the table {table_name}...")
        df.to_sql(table_name, engine, if_exists='append', index=False)  # Use append, not replace
        print(f"Inserted {len(df)} rows into the {table_name} table.")
        return True

    except Exception as e:
        print(f"An error occurred during database interaction: {e}")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    input_file = 'input_data.dat'  
    last_line_file = 'last_line.txt'
    etl_log_file = 'etl_log_id.txt'

    # Read last processed line
    last_line = read_last_processed_line(last_line_file)

    # Read current etl_log_id and increment it after the run
    etl_log_id = read_etl_log_id(etl_log_file)

    # Read the file contents once and reuse it
    with open(input_file, 'r') as infile:
        lines = infile.readlines()

    # Process the .dat file and convert to DataFrame
    df, last_processed_line, station_name = process_data_file(lines, last_line, etl_log_id)

    # Upload the DataFrame to the PostgreSQL database, passing the station_name
    if not df.empty and upload_to_database(df, station_name):  # Pass station_name here
        # Only update last_line.txt if data was successfully inserted into the database
        save_last_processed_line(last_line_file, last_processed_line)

    # Increment etl_log_id for the next run and save it
    etl_log_id += 1
    save_etl_log_id(etl_log_file, etl_log_id)
