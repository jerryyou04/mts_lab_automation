import pandas as pd
import os
import traceback
import logging
from sqlalchemy import create_engine, Integer, TIMESTAMP, MetaData, Table, Column, Float, String, BigInteger
from sqlalchemy.sql import text
from datetime import datetime
from dotenv import load_dotenv

############################################################# logging
def setup_logging():
    # Get the current month and year
    current_month_year = datetime.now().strftime("%Y_%m")
    log_filename = f"etl_error_log_{current_month_year}.txt"  # Filename based on the current year and month

    # Configure logging to log to the specified file and include time, error level, and message
    logging.basicConfig(
        level=logging.DEBUG,  
        format='%(asctime)s\t%(levelname)s\t%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_filename),  # Log to a file
            logging.StreamHandler()  # Also print to console
        ]
    )
########################################################################


###################################################################### Setup and global variables
load_dotenv()

# Call the setup_logging function to configure logging when the script starts
setup_logging()

folders = [
    r'C:\MTS 793\Projects\Project1\Current\Table Top',
    r'C:\MTS 793\Projects\Project1\Current\T24-64',
    r'C:\MTS 793\Projects\Project1\Current\MTS 810',
    r'C:\MTS 793\Projects\Project1\Current\temp'
]

mod_times_file = 'file_mod_times.txt'  # File to track modification times
file_mod_times = {}  
last_lines_tsv_file = 'last_lines.txt'

#########################################################


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

    # Read the last recorded file mod times
    last_mod_times = {}
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


# Process each modified file (existing logic)
def process_and_upload_files(modified_files, last_lines):
    etl_log_file = 'etl_log_id.txt'  # Hardcoded the log file path
    for input_file in modified_files:
        logging.info(f"Starting processing for file: {input_file}")
        try:
            folder_name = os.path.basename(os.path.dirname(input_file))
            
            # Determine the table name based on the folder name
            try:
                table_name = get_table_name_from_folder(folder_name)
                logging.info(f"Table name for file '{input_file}' determined: {table_name}")
            except ValueError as e:
                logging.error(f"Error determining table name for file '{input_file}': {str(e)}")
                continue  # Skip the file if folder name is not recognized
            
            # Process the file
            if os.path.exists(input_file):
                logging.info(f"File found: {input_file}")
                with open(input_file, 'r') as infile:
                    lines = infile.readlines()
                    logging.info(f"Read {len(lines)} lines from the file: {input_file}")
                
                # Read last processed line for this specific file
                last_line = read_last_processed_line(input_file, last_lines)

                # Read current etl_log_id and increment it after the run
                etl_log_id = read_etl_log_id(etl_log_file)

                # Process the .dat file and convert to DataFrame
                df, last_processed_line, station_name = process_data_file(lines, last_line, etl_log_id, table_name)

                if not df.empty and upload_to_database(df, table_name):
                    # Update last processed line in memory
                    update_last_processed_line(input_file, last_processed_line, last_lines)
                    logging.info(f"Successfully processed and uploaded data from: {input_file}")

                # Increment etl_log_id for the next run and save it
                etl_log_id += 1
                save_etl_log_id(etl_log_file, etl_log_id)
            else:
                logging.error(f"File not found: {input_file}")
        except Exception as e:
            logging.error(f"Error processing {input_file}: {str(e)}")
        logging.info(f"Finished processing file: {input_file}")
#######################################################################

######################################################################## Last line and logging
def load_last_processed_lines(tsv_file='last_lines.txt'):
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

# Function to update the last processed line for a specific file
def update_last_processed_line(input_file, last_line, last_lines):
    last_lines[input_file] = last_line

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

#########################################################################

######################################################################### SQL functions

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

    try:
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
                logging.info(f"Creating sequence: {sequence_name}")
                conn.execute(text(f"CREATE SEQUENCE {sequence_name} START WITH {start_value};"))
            else:
                logging.info(f"Sequence {sequence_name} already exists, not restarting.")

            # Commit to persist the changes
            conn.commit()

            # Attach the sequence to the id column, but only if it's not already attached
            try:
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
        db_username = os.getenv('DB_USERNAME')
        db_password = os.getenv('DB_PASSWORD')
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        db_name = os.getenv('DB_NAME')

        logging.info(f"Connecting to the database for table: {table_name}")
        engine = create_engine(f'postgresql+psycopg2://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')
        
        if create_table_if_not_exists(engine, table_name, df):
            # Drop 'id' from the DataFrame to prevent it from interfering with autoincrement in the DB
            df = df.drop(columns=['id'], errors='ignore')
            
            logging.info(f"Inserting data into the table {table_name} in chunks.")
            df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=10000, dtype={
                'etl_log_id': Integer(), 
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
    # Define the directories to check for modified files
    directories = [
        r'C:\MTS 793\Projects\Project1\Current\Table Top',
        r'C:\MTS 793\Projects\Project1\Current\T24-64',
        r'C:\MTS 793\Projects\Project1\Current\MTS 810',
        r'C:\MTS 793\Projects\Project1\Current\temp'
    ]

    # Log file to track modification times
    mod_log_file = 'file_mod_times.txt'

    # Load the last processed lines from the file
    last_lines = load_last_processed_lines()

    # Get the list of modified files
    modified_files = track_modified_files(mod_log_file, directories)

    if modified_files:
        logging.info(f"Modified files detected: {modified_files}")
    else:
        logging.info("No modified files found in any of the folders.")

    # Process each modified file and track last processed lines
    process_and_upload_files(modified_files, last_lines)

    # Save the updated last processed lines to the file
    save_last_processed_lines(last_lines)
