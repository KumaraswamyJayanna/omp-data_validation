import importlib.util
import logging
import os
import sys

import boto3
import pandas as pd
from botocore.exceptions import NoCredentialsError

# Add the parent directory to the sys.path to find the 'awsconfig' module
constants_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils', 'awsconfig.py'))
spec = importlib.util.spec_from_file_location("awsconfig", constants_path)
awsconfig = importlib.util.module_from_spec(spec)
spec.loader.exec_module(awsconfig)

config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config.py'))
config_spec = importlib.util.spec_from_file_location("config", config_path)
config = importlib.util.module_from_spec(config_spec)
config_spec.loader.exec_module(config)


class Lookupdata():


    def __init__(self):
        self.consolidated_lookup = []
        if not os.path.exists(awsconfig.directory_name):
            os.makedirs(awsconfig.directory_name)


    def download_lookups(self):
        s3 = boto3.client('s3')
        try:
            response = s3.list_objects_v2(Bucket=awsconfig.bucketname, Prefix=awsconfig.lookup_data_prefix)
            if 'Contents' in response:
                files = [content['Key'] for content in response['Contents']]
                # print(files)
                for file in files[1:]:
                    lookupfile_name = f'{awsconfig.directory_name}/{file.split("/")[-1]}'
                    s3.download_file(awsconfig.bucketname, file, lookupfile_name)
                else:
                    print("No files found in the specified bucket and prefix.")
        except NoCredentialsError:
            print("Error: No AWS credentials found.")

    def supplier_name_lookup(self, filename='Supplier_Alias_Name.csv'):
        #  suppliar id suppliar names lookup
        supplier_alias = pd.read_csv(f"/lookupdata/{filename}")
        supplier_lookup = supplier_alias.groupby('supplier_id')['alternative_name'].apply(list).reset_index()
        return supplier_lookup


    def client_alias_names_mapping(self, filename='Client_Alias_Name.csv'):
        # Client id and client alias name
        client_alias = pd.read_csv(f"lookupdata/{filename}")
        client_alias_lookup = client_alias.groupby('client_id')['alternative_name'].apply(list).reset_index()
        return client_alias_lookup


    def category_supplier_mapping(self, filename="category_suppliers_mapping.csv"):
        cat_supplier = pd.read_csv(f"lookupdata/{filename}")
        if 'category_id' in cat_supplier.columns:
            cat_supplier =cat_supplier[cat_supplier['column_name'] == config.CATEGORY_ID]
        category_supplier_lookup = cat_supplier.groupby('Supplier_ID')['Supplier_Name'].apply(list).reset_index()
        return category_supplier_lookup

    def supplier_normalization_lookup(self, filename="Supplier_Normalized_Original_Lkp.csv"):
        sup_normalization = pd.read_csv(f"lookupdata/{filename}")
        return sup_normalization

    def client_master_mapping(self, filename="Client_Master.csv"):
        client_master = pd.read_csv(f"lookupdata/{filename}")
        return client_master


    def normalization_lookup(self, filename="normalization_all_categories_lookup.csv"):
        noramilzation_lookupdata = pd.read_csv(f'lookupdata/{filename}')
        return noramilzation_lookupdata


    def consolidated_lookup_data(self, filename="lookup_file.xlsx"):
        lookupfile = f'{awsconfig.directory_name}/{filename}'
        normalization_lookup_data = self.normalization_lookup()
        supplier_lookup_data = self.supplier_normalization_lookup
        client_master_data = self.client_master_mapping()
        # lookups = [normalization_lookup_data, supplier_lookup_data, client_master_data]
        consolidated_df = pd.concat([client_master_data, normalization_lookup_data], axis=1)

        with pd.ExcelWriter(lookupfile, engine='openpyxl') as writer:
            consolidated_df.to_excel(writer, index=False)
        return os.path.abspath(lookupfile)

    def get_lookup_data(self):
        logging.info(f'Download the lookup data files from s3 bucket')
        self.download_lookups()
        logging.info(f'Get the consolidated lookupdata for verification')
        lookupfilepath = self.consolidated_lookup_data()
        return lookupfilepath

