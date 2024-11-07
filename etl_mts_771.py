############################################################# 
'''
Version: 3
 
see below for version info.
'''
#############################################################
import pandas as pd
import os
import traceback
import logging
from sqlalchemy import create_engine, Integer, TIMESTAMP, MetaData, Table, Column, Float, String, BigInteger, insert
from sqlalchemy.sql import text
from datetime import datetime
from dotenv import load_dotenv

###################################################################### Setup and global variables
load_dotenv() 
db_username = os.getenv('DB_USERNAME')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')
engine = create_engine(f'postgresql+psycopg2://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

etl_log_table = None
error_log_table = None

file_mod_times = {}  

############################################################# logging

class DatabaseLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()

    def emit(self, record):
        # Prepare the log entry for the database
        log_entry = {
            'timestamp': datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S'),
            'level': record.levelname,
            'message': record.getMessage(),
        }

        # Insert the log entry into the database
        with engine.connect() as conn:
            conn.execute(insert(error_log_table).values(log_entry))
            conn.commit()  # Ensure the transaction is committed

def setup_logging():
    log_directory = "C:/data/script/system_data"

    # Create the directory if not exists
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
        logging.info(f"Created log directory: {log_directory}")

    current_month_year = datetime.now().strftime("%Y_%m")
    log_filename = os.path.join(log_directory, f"etl_error_log_{current_month_year}.txt")

    logging.basicConfig(     # Configure logging to log to the specified file and include time, error level, and message
        level=logging.DEBUG,  
        format='%(asctime)s\t%(levelname)s\t%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_filename),  # Log to a file
            logging.StreamHandler(),  # Also print to console
            DatabaseLogHandler() 
        ]
    )

def create_logging_tables_if_not_exists():
    global etl_log_table, error_log_table 
    metadata = MetaData()

    # Define schema for etl_log table
    etl_log_table = Table(
        'etl_log', metadata,
        Column('id', BigInteger),
        Column('began_at_timestamp', TIMESTAMP),
        Column('header_tstamp_first', TIMESTAMP),
        Column('station_name', String),
        Column('test_file_name', String),
        Column('table_name', String),
        Column('rows_inserted', Integer),
        Column('minutes_since_last_run', Integer)
    )

    # Define schema for error_log table if needed
    error_log_table = Table(
        'error_log', metadata,
        Column('timestamp', TIMESTAMP),
        Column('level', String),
        Column('message', String),
    )

    # Create tables if they don’t exist
    metadata.create_all(engine)

########################################################## set up logging
setup_logging() 
########################################################## File modification times in OS
# Load modification times from a text file
def load_modification_times():
    global file_mod_times
    file_mod_times = {}
    
    if os.path.exists('mod_times.txt'):
        with open('mod_times.txt', 'r') as f:
            for line in f:
                file_path, mod_time = line.strip().split("\t")
                file_mod_times[file_path] = float(mod_time)
        logging.info("Loaded previous file modification times.")
    else:
        logging.info("No previous modification times found, starting fresh.")


# Save modification times to a text file
def save_modification_times():
    with open('mod_times.txt', 'w') as f:
        for file_path, mod_time in file_mod_times.items():
            f.write(f"{file_path}\t{mod_time}\n")
    logging.info("File modification times saved.")

def get_last_modification_time(file_path):
    return os.path.getmtime(file_path)

def track_modified_files(log_file, directories):
    modified_files = []
    valid_directories = 0  # Track how many valid directories are found

    last_mod_times = {} # Read the last recorded file mod times
    if os.path.exists(log_file):
        with open(log_file, 'r') as f:
            for line in f:
                filepath, last_mod_time = line.strip().split("\t")
                last_mod_times[filepath] = float(last_mod_time)
    
    # Check each directory for modified files
    for directory in directories:
        if os.path.exists(directory):  # Only process the directory if it exists
            valid_directories += 1  # Count how many directories exist
            for filename in os.listdir(directory):
                file_path = os.path.join(directory, filename)
                if os.path.isfile(file_path):
                    current_mod_time = os.path.getmtime(file_path)

                    # Check if it's a new or modified file
                    if file_path not in last_mod_times or current_mod_time > last_mod_times[file_path]:
                        modified_files.append(file_path)
                        last_mod_times[file_path] = current_mod_time
        else:
            logging.warning(f"Directory does not exist: {directory}")

    # Raise an error if no valid directories were found
    if valid_directories == 0:
        logging.critical("No valid directories found. Ensure that at least one folder exists.")
        raise FileNotFoundError("All specified directories are missing.")

    # Write updated mod times back to the log file
    with open(log_file, 'w') as f:
        for filepath, mod_time in last_mod_times.items():
            f.write(f"{filepath}\t{mod_time}\n")
    
    return modified_files

def get_last_line_of_file(file_path): # helper to find the last line of a file.
    with open(file_path, 'r') as f:
        return sum(1 for _ in f)

########################################################################################## Process each file in a loop
# Process each modified file 
def process_and_upload_files(modified_files, last_lines):
    etl_log_file = 'etl_log_id.txt'  # Hardcoded the log file path
    for input_file in modified_files:
        logging.info(f"Starting processing for file: {input_file}")
        try:
            folder_name = os.path.basename(os.path.dirname(input_file))
            try: # Determine the table name based on the folder name
                table_name = get_table_name_from_folder(folder_name)
                logging.info(f"Table name for file '{input_file}' determined: {table_name}")
            except ValueError as e:
                logging.error(f"Error determining table name for file '{input_file}': {str(e)}")
                continue  # Skip the file if folder name is not recognized
            
            if os.path.exists(input_file):             # Process the file
                logging.info(f"File found: {input_file}")
                with open(input_file, 'r') as infile:
                    lines = infile.readlines()
                    logging.info(f"Read {len(lines)} lines from the file: {input_file}")
                
                
                last_line = read_last_processed_line(input_file, last_lines) # Read last processed line for this specific file
                etl_log_id = read_etl_log_id(etl_log_file) # Read current etl_log_idn

                df, last_processed_line, station_name = process_data_file(lines, last_line, etl_log_id, table_name)  # Process the .dat file and convert to DataFrame

                if not df.empty and upload_to_database(df, table_name):
                    update_last_processed_line(input_file, last_processed_line, last_lines) # Update last processed line in memory
                    logging.info(f"Successfully processed and uploaded data from: {input_file}")

                etl_log_id += 1 # Increment etl_log_id for the next run and save it
                save_etl_log_id(etl_log_file, etl_log_id)
            else:
                logging.error(f"File not found: {input_file}")
        except Exception as e:
            logging.error(f"Error processing {input_file}: {str(e)}")
        logging.info(f"Finished processing file: {input_file}")
######################################################################## Last line and logging
def load_last_processed_lines(tsv_file='last_lines.txt'): #retrieves
    last_lines = {}
    if os.path.exists(tsv_file):
        with open(tsv_file, 'r') as f:
            for line in f:
                filename, last_line = line.strip().split('\t')
                last_lines[filename] = int(last_line)
    return last_lines

# Function to save the last processed lines to a tsv file
def save_last_processed_lines(last_lines, tsv_file='last_lines.txt'):
    with open(tsv_file, 'w') as f:
        for filename, last_line in last_lines.items():
            f.write(f"{filename}\t{last_line}\n")

# Function to read the last processed line for a specific file
def read_last_processed_line(input_file, last_lines):
    if input_file in last_lines:
        return last_lines[input_file]
    else:
        return 0  # Default to 0 if the file hasn't been processed before

def update_last_processed_line(input_file, last_line, last_lines):
    last_lines[input_file] = last_line

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
    current_month_year = datetime.now().strftime("%Y_%m")
    log_directory = "C:/data/script/system_data"  

    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    log_filename = os.path.join(log_directory, f"etl_log_{current_month_year}.txt")

    # Check if the file exists
    file_exists = os.path.exists(log_filename)

    with open(log_filename, 'a') as log_file:
        if not file_exists: # If the file doesn't exist, write the TSV header
            log_file.write("id\tbegan_at_timestamp\theader_tstamp_first\tstation_name\ttest_file_name\ttable_name\trows_inserted\tminutes_since_last_run\n")

        rows_inserted = "NULL"
        minutes_since_last_run = "NULL"

        timestamp_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")         # Append the log entry in TSV format, including the table_name
        log_file.write(f"{etl_log_id}\t{timestamp_now}\t{header_tstamp_first}\t{station_name}\t{test_file_name}\t{table_name}\t{rows_inserted}\t{minutes_since_last_run}\n")


    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
    etl_log_entry = {
        'id': etl_log_id,  
        'began_at_timestamp': current_timestamp,
        'header_tstamp_first': header_tstamp_first,
        'station_name': station_name,
        'test_file_name': test_file_name,
        'table_name': table_name,
        'rows_inserted': None,
        'minutes_since_last_run': None
    }

    # Insert the entry into the database
    with engine.connect() as conn:
        conn.execute(insert(etl_log_table).values(etl_log_entry))
        conn.commit()
        logging.info(f"Inserted log entry into etl_log with id: {etl_log_id}")

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

#########################################################################

######################################################################### SQL functions

def extract_columns_and_metadata(lines, last_line_processed):
    headers = []
    header_tstamp_first = None
    station_name = None
    test_file_name = None
    found_test_file = False

    try:
        for i in range(last_line_processed, len(lines)): # Start from the last processed line
            line = lines[i].strip()

            if "Data Header:" in line: # Extract the timestamp from the latest Data Header
                parts = line.split("\t")
                if len(parts) > 4:
                    timestamp_from_file = parts[-1].strip() # Convert to SQL format
                    parsed_timestamp = datetime.strptime(timestamp_from_file, "%m/%d/%Y %I:%M:%S %p")
                    header_tstamp_first = parsed_timestamp.strftime("%Y-%m-%d %H:%M:%S")

            elif "Station Name:" in line:
                station_name = line.split(":")[1].strip()
            elif "Test File Name:" in line:
                test_file_name = line.split(":")[1].strip()
                found_test_file = True  # After this, we expect the headers to be in the next line
                continue

            if found_test_file and not headers:
                headers = line.split("\t")  # Use the next line as headers
                break  # We have found the headers, stop searching

        if not headers:
            logging.error("No valid headers found in the file.")
    
    except Exception as e:
        logging.error(f"Error during metadata extraction: {e}")
    
    return headers, header_tstamp_first, station_name, test_file_name

def process_data_file(lines, last_line_processed, etl_log_id, table_name):
    data = []
    most_recent_header_timestamp = None  # Track the most recent timestamp
    
    headers, header_tstamp_first, station_name, test_file_name = extract_columns_and_metadata(lines, last_line_processed)

    if not headers:
        logging.error("No headers found in the file. (end of file?)")
        return pd.DataFrame(), last_line_processed, station_name

    append_to_log_file(etl_log_id, header_tstamp_first, station_name, test_file_name, table_name)     # Log metadata once for the run

    in_data_section = False  # Track whether we are in the data section
    skip_units_row = False  # Set a flag to skip the units row

    for i in range(last_line_processed, len(lines)):
        line = lines[i].strip()

        if "Data Header:" in line: # Handle the most recent Data Header timestamp
            parts = line.split("\t")
            if len(parts) > 4:
                timestamp_from_file = parts[-1].strip()             # Extract the timestamp from the header and convert to SQL format
                parsed_timestamp = datetime.strptime(timestamp_from_file, "%m/%d/%Y %I:%M:%S %p")
                most_recent_header_timestamp = parsed_timestamp.strftime("%Y-%m-%d %H:%M:%S")
            continue  # Continue to the next line, we're still processing metadata

        if "Station Name:" in line or "Test File Name:" in line:
            in_data_section = False  # Still in metadata
            continue

        if headers and not in_data_section:  # Start processing data after headers
            in_data_section = True
            skip_units_row = True  # We know the next line will be the units row, so we skip it
            continue

        if skip_units_row:         # Skip the units line after the headers - We are assuming that units is always after the headers
            skip_units_row = False  # Reset the flag after skipping
            continue

        if in_data_section:
            columns = line.split("\t")  # Split using tab between columns

            if len(columns) == len(headers):             # Ensure the number of columns matches the headers before adding the extra columns
                try:
                    row_data = [float(c) for c in columns] # Convert the data to appropriate types - assuming they are of type floats
                    
                    row_data.insert(0, etl_log_id)  # Insert etl_log_id as first column
                    row_data.insert(1, most_recent_header_timestamp)  # Insert most recent header timestamp as second column
                    row_data.insert(2, station_name)     
                    
                    data.append(row_data)
                except ValueError as ve:
                    logging.error(f"Skipping line {i + 1} due to ValueError: {ve}")
    
    headers = ['etl_log_id', 'header_timestamp', 'station_name'] + headers      # Add 3 extra headers for the new columns: etl_log_id, header_timestamp
    df = pd.DataFrame(data, columns=headers)

    return df, len(lines), station_name


def create_table_if_not_exists(engine, table_name, df):
    metadata = MetaData()

    columns = [Column('id', BigInteger, primary_key=True)] # Define dynamic columns based on DataFrame
    for col in df.columns:
        if col not in ['id']:
            if col == "header_timestamp":
                columns.append(Column(col, TIMESTAMP))
            elif col == "etl_log_id":
                columns.append(Column(col, BigInteger))
            elif col == "station_name":
                columns.append(Column(col, String))
            else:
                columns.append(Column(col, Float))  # Assume float for other columns

    try:
        table = Table(table_name, metadata, *columns) # Step 1: Create the table if it does not exist
        metadata.create_all(engine)

        start_values = { # Hardcoded start values for sequences
            'table_top': 1,
            'rotary': 2 * 10**18, # 2 quintillion
            'mts_810': 3 * 10**18, # 3 quintillion
            'placeholder': 4 * 10**18 # 4 quintillion
        }

        start_value = start_values.get(table_name, 1)
        sequence_name = f"{table_name}_id_seq"

        with engine.connect() as conn:
            sequence_check_query = f"SELECT 1 FROM pg_class WHERE relname = '{sequence_name}' AND relkind = 'S'" # Check if sequence exists
            sequence_exists = conn.execute(text(sequence_check_query)).fetchone()

            if not sequence_exists:
                logging.info(f"Creating sequence: {sequence_name}")
                conn.execute(text(f"CREATE SEQUENCE {sequence_name} START WITH {start_value};"))
            else:
                logging.info(f"Sequence {sequence_name} already exists, not restarting.")

            conn.commit()

            try:  # Attach the sequence to the id column, but only if it's not already attached
                conn.execute(text(f"ALTER TABLE \"{table_name}\" ALTER COLUMN id SET DEFAULT nextval('{sequence_name}');"))
            except Exception as e:
                logging.error(f"Error attaching sequence to id column for {table_name}: {str(e)}")

        logging.info(f"Table '{table_name}' is ready.")
    
    except Exception as e:
        logging.error(f"Error creating table '{table_name}': {str(e)}")
        return False

    return True

def upload_to_database(df, table_name):
    if df.empty:
        logging.info("No data to upload.")
        return False

    try:
        logging.info(f"Connecting to the database for table: {table_name}")
        
        if create_table_if_not_exists(engine, table_name, df):
            df = df.drop(columns=['id'], errors='ignore') # Drop 'id' from the DataFrame to prevent it from interfering with autoincrement in the DB if it's in there
            
            logging.info(f"Inserting data into the table {table_name} in chunks.")
            df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=10000, dtype={
                'etl_log_id': BigInteger(), 
                'header_timestamp': TIMESTAMP(),
                'station_name': String()
            })
            logging.info(f"Successfully inserted {len(df)} rows into {table_name}.")
            return True
        else:
            logging.error(f"Failed to create table '{table_name}' or it already exists.")
            return False

    except Exception as e:
        logging.error(f"An error occurred during database interaction: {str(e)}")
        return False

############################################################################################
if __name__ == "__main__":
    directories = [
        os.getenv('DIRECTORY_1'),
        os.getenv('DIRECTORY_2'),
        os.getenv('DIRECTORY_3'),
        os.getenv('DIRECTORY_4')
    ]

    create_logging_tables_if_not_exists()
    mod_log_file = 'mod_times.txt'
    last_lines_file = 'last_lines.txt'

    # Check if it's the first run
    if not os.path.exists(mod_log_file) or not os.path.exists(last_lines_file):
        logging.info("First run detected. Initializing file modification times and last_lines.txt.")
        
        last_lines = {}
        last_mod_times = {}

        # Populate both files with current data
        for directory in directories:
            if os.path.exists(directory):
                for filename in os.listdir(directory):
                    file_path = os.path.join(directory, filename)
                    if os.path.isfile(file_path):
                        # Get the current last line number and modification time
                        last_lines[file_path] = get_last_line_of_file(file_path)
                        last_mod_times[file_path] = os.path.getmtime(file_path)

        # Save the last lines to last_lines.txt
        with open(last_lines_file, 'w') as f:
            for file_path, last_line in last_lines.items():
                f.write(f"{file_path}\t{last_line}\n")

        # Save the modification times to mod_times.txt
        with open(mod_log_file, 'w') as f:
            for file_path, mod_time in last_mod_times.items():
                f.write(f"{file_path}\t{mod_time}\n")

        logging.info("Initialization complete. No processing needed on the first run.")
    
    else:
        # Not the first run: proceed with normal processing
        last_lines = load_last_processed_lines()
        modified_files = track_modified_files(mod_log_file, directories)

        if modified_files:
            logging.info(f"Modified files detected: {modified_files}")
            process_and_upload_files(modified_files, last_lines)
            save_last_processed_lines(last_lines)
        else:
            logging.info("No modified files found in any of the folders.")
############################################################# 
'''
Versions
 
1 2024-10-25 initial version

2 2024-11-04 relocated log files to C:/data/script/system_data

3 2024-11-05 Added null columns rows_inserted and minutes_since_last_run to etl_log, and created new tables for error_log and etl_log
 
'''
#############################################################
    