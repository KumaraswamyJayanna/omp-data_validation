import os
from io import StringIO

import boto3
import botocore
import pandas as pd
import psycopg2
from dbconfig import (
    CATEGORY_NAME,
    DB_TABLE,
    FLATFILE_NAME,
    QUERY,
    bucket_name,
    prefix,
    prefix_flatfile,
)


class PostgresLogger:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.connection = self.get_postgres_connection()
        self.test_directory = "db_ff_test_directory"
        if not os.path.exists(self.test_directory):
            os.makedirs(self.test_directory)


    def get_postgres_connection(self):
        try:
            connection = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
            )
            print("PostgreSQL connection established.")
            return connection
        except Exception as e:
            raise RuntimeError(f"Error connecting to PostgreSQL: {str(e)}")

    def fetch_data_contents(self):
        # Table name to fetch data from
        table_name = "price_point"

        try:
            connection = self.get_postgres_connection()
            cursor = connection.cursor()

            # Executing the SQL query to fetch data
            fetch_query = f"SELECT * FROM public.{table_name};"
            cursor.execute(fetch_query)

            # Fetching all rows from the executed query
            rows = cursor.fetchall()
            data = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
            print(len(data))

            #write date to an excel file
            # Create the download path if it doesn't exist
            download_path = os.path.join(os.getcwd(), "db_data")
            if not os.path.exists(download_path):
                os.makedirs(download_path)
            # Create the full download path
            file = os.path.join(download_path, f"res_{table_name}.xlsx")
            # Save the DataFrame to an Excel file
            data.to_excel(file, index=False)
            print(f"File {file} downloaded to {download_path}")
            # # Display the contents
            # data.to_excel(f"res_{table_name}.xlsx", index=False)

            # print(f"file copied here: {os.path.abspath(file)}")
            # print(f"Contents of the table '{table_name}':")


        except Exception as e:
            print(f"Error: {e}")

        finally:
            # Closing the cursor and connection
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def get_category_name_id(self):

        try:
            connection = self.get_postgres_connection()
            cursor = connection.cursor()

            # Executing the SQL query to fetch data
            fetch_query = f"SELECT category_name, category_id FROM public.taxonomy"
            cursor.execute(fetch_query)

            # Fetching all rows from the executed query
            rows = cursor.fetchall()
            data = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
            category_name_with_id = data.set_index('category_name')['category_id'].to_dict()
            # print(category_name_with_id)

        except Exception as e:
            print(f"Error: {e}")

        finally:
            # Closing the cursor and connection
            if cursor:
                cursor.close()
            if connection:
                connection.close()

        return category_name_with_id

    def get_data_from_db_by_category(self, category_name):
        try:
            connection = self.get_postgres_connection()
            cursor = connection.cursor()

            # Fetching the category_id using the category_name
            category_id = self.get_category_name_id().get(category_name)
            if category_id is None:
                raise ValueError(f"Category '{category_name}' not found in the database.")
            print(f"Category ID for '{category_name}': {category_id}")

            # Executing the SQL query to fetch data
            fetch_query = f"select psm.category_id,p.* from public.price_point p join public.product_service_master psm on p.product_id = psm.product_id where psm.category_id = 14;"
            # fetch_query = QUERY
            cursor.execute(fetch_query)

            # Fetching all rows from the executed query
            rows = cursor.fetchall()
            data_by_category = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])


        except Exception as e:
            print(f"Error: {e}")

        finally:
            # Closing the cursor and connection
            if cursor:
                cursor.close()
            if connection:
                connection.close()
        return data_by_category

    def get_consolidated_flatfile(self):

        s3 = boto3.client('s3')
        try:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix_flatfile)
            if 'Contents' in response:
                files = [content['Key'] for content in response['Contents']]
                for file in files:

                    if f'{prefix_flatfile}{FLATFILE_NAME}' in file:
                        consolidatedfiles = file
                        print(f"Found consolidated file: {consolidatedfiles}")
                        # Download the consolidatedfile (example)
                        flatfilename = f'{self.test_directory}/{CATEGORY_NAME}_flatfiledata.xlsx'
                        s3.download_file(bucket_name, consolidatedfiles, flatfilename)

                    else:
                        print("No files found in the specified bucket and prefix.")
        except botocore.exceptions.NoCredentialsError:
            print("Credentials not available")
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise

        flatfilepath = os.path.realpath(flatfilename)
        return flatfilepath

    def get_category_db_data(self):

        self.get_category_name_id()
        category_data_from_db = self.get_data_from_db_by_category(CATEGORY_NAME)
        df_database = category_data_from_db
        category_dbname = f'{self.test_directory}/{CATEGORY_NAME}_{DB_TABLE}_databasedata.xlsx'
        with pd.ExcelWriter(category_dbname, engine='openpyxl') as writer:
            df_database.to_excel(writer, sheet_name="database", index=False)
        databasepath = os.path.realpath(category_dbname)
        return databasepath
