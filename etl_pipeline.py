import pandas as pd
import boto3
import os
import logging
import redshift_connector
from glob import glob
from botocore.exceptions import ClientError
from secret import secrets
from redshift import redshift_cred
from IPython.display import display


#get aws access key
session = boto3.Session(
    aws_access_key_id = secrets.get ('aws_access_key_id'),
    aws_secret_access_key = secrets.get ('aws_secret_access_key')
)
s3 = session.resource('s3')
    

def extract_data(file_pattern: str, file2: str):
    """
    Function to read data from two CSV files.

    Parameters:
    - file_pattern (str): File pattern to match multiple CSV files.
    - file2 (str): Path to the second CSV file.

    Returns:
    - pd.DataFrame: Two DataFrames containing the data from the CSV files.
    """
    try:
        # Use glob to find files matching the pattern
        file1_paths = glob(file_pattern)

        # Extract data from multiple CSV files
        phv_dfs = [pd.read_csv(file_path) for file_path in file1_paths]

        # Concatenate DataFrames from multiple files
        phv_df = pd.concat(phv_dfs, ignore_index=True)

        # Extract data from the institution CSV file
        inst_df = pd.read_csv(file2)

        return phv_df, inst_df

    except (FileNotFoundError, pd.errors.EmptyDataError, pd.errors.ParserError, Exception) as e:
        # Handle specific exceptions related to file not found, empty data, parser error, or a generic exception
        logging.error(f"Error in extract_data: {e}")
        return None, None

def transform_data(phv_df, inst_df):
    """
    Function to transform data.

    Parameters:
    - phv_df (pd.DataFrame): DataFrame for patients' hospital visits data.
    - inst_df (pd.DataFrame): DataFrame for institution data.

    Returns:
    - pd.DataFrame: Transformed DataFrame after merging and dropping columns.
    """
    try:
        # Drop unnecessary columns from phv_df
        phv_df.drop(columns=['preferred_language', 'religion', 'smoker'], inplace=True)

        # Remove leading and trailing whitespaces from 'institution_id' columns
        phv_df['institution_id'] = phv_df['institution_id'].apply(lambda x: x.strip())
        inst_df['institution_id'] = inst_df['institution_id'].apply(lambda x: x.strip())

        # Merge DataFrames on 'institution_id'
        merged_df = pd.merge(phv_df, inst_df, on="institution_id")

        # Remove duplicate rows based on all columns
        merged_df_no_duplicates = merged_df.drop_duplicates()

        return merged_df_no_duplicates

    except (KeyError, Exception) as e:
        # Handle specific exceptions related to missing columns or a generic exception
        logging.error(f"Error in transform_data: {e}")
        return None

def upload_file_to_s3(file_name, bucket, object_name=None):
    """
    Upload a file to an S3 bucket.

    Parameters:
    - file_name (str): File to upload
    - bucket (str): Bucket to upload to
    - object_name (str): S3 object name. If not specified then file_name is used

    Returns:
    - bool: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    try: 
        s3.meta.client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def copy_data_to_redshift(table_name, s3_path, aws_access_key, aws_secret_key):
    """
    Copy data from S3 to Redshift.

    Parameters:
    - table_name (str): Name of the Redshift table.
    - s3_path (str): S3 path to the CSV file.
    - aws_access_key (str): AWS access key.
    - aws_secret_key (str): AWS secret key.
    """
    
    try:
        # Establish the Redshift connection
        conn = redshift_connector.connect(
            host=redshift_cred.get('host'),
            database='dev',
            port=5439,
            user=redshift_cred.get('user'),
            password=redshift_cred.get('password'),
        )

        # Create a cursor
        cur = conn.cursor()

        # Truncate the table before copying data
        cur.execute(f"TRUNCATE {table_name};")

        # Copy data into the table
        copy_command = f"""
            COPY {table_name}
            FROM '{s3_path}'
            CREDENTIALS 'aws_access_key_id={aws_access_key};aws_secret_access_key={aws_secret_key}'
            DELIMITER ','
            CSV;
        """
        cur.execute(copy_command)

        # Commit the changes
        conn.commit()

    except (redshift_connector.InterfaceError, Exception) as e:
        # Handle specific exceptions related to Redshift connection or a generic exception
        logging.error(f"Error in copy_data_to_redshift: {e}")

    finally:
        # Close the connection
        if conn is not None:
            conn.close()
            
def main():
    file1_pattern = 'patients_hospital_visits*.csv'
    file2_path = 'institution.csv'
    redshift_table_name = 'stg_patients_hospital_visits'
    
    # Extract data
    patients_df, institution_df = extract_data(file1_pattern, file2_path)

    # Transform data
    result_df = transform_data(patients_df, institution_df)
    
    # Display the result
    display(result_df)

    # Save the result_df to a CSV file
    result_df.to_csv('stg_patients_hospital_visits.csv', index=False)
    
    # Load the result to S3
    combined_files = upload_file_to_s3(file_name='stg_patients_hospital_visits.csv', bucket='ojinsight-data-engineer', object_name='hsa_assessment/stg_patients_hospital_visits.csv')
    print("Done")
    
    # Example usage of copy_data_to_redshift function
    redshift_table_name = 'stg_patients_hospital_visits'
    s3_path = 's3://ojinsight-data-engineer/hsa_assessment/stg_patients_hospital_visits.csv'
    aws_access_key = secrets.get ('aws_access_key_id')
    aws_secret_key = secrets.get ('aws_secret_access_key')

    copy_data_to_redshift(redshift_table_name, s3_path, aws_access_key, aws_secret_key)

if __name__ == "__main__":
    main()
