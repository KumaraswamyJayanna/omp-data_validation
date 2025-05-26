import importlib.util
import os
from io import BytesIO

import boto3
import botocore
import pandas as pd
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

aws_config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils', 'awsconfig.py'))
spec = importlib.util.spec_from_file_location("awsconfig", aws_config_path)
awsconfig = importlib.util.module_from_spec(spec)
spec.loader.exec_module(awsconfig)


config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config.py'))
config_spec = importlib.util.spec_from_file_location("config", config_path)
config = importlib.util.module_from_spec(config_spec)
config_spec.loader.exec_module(config)

ground_truth_file_name = f'{awsconfig.groundtruth_files_path}cid-{config.CATEGORY_ID}_{config.CATEGORY_NAME}_GroundTruth.xlsx'


class S3utils:

    def __init__(self, categoryname):
        self.category_name = categoryname
        self.upload_path = f'lsi_poc/data_steward/validation_data/'
        # self.test_directory = "db_ff_test_directory"
        if not os.path.exists(awsconfig.test_directory):
            os.makedirs(awsconfig.test_directory)

    def download_file_from_s3object(self):
        s3 = boto3.client('s3')
        try:
            response = s3.list_objects_v2(Bucket=awsconfig.bucketname,
                                          Prefix=awsconfig.prefix_flatfile)
            if 'Contents' in response:
                files = [content['Key'] for content in response['Contents']]
                # print(files)
                for file in files:
                    if f'{awsconfig.prefix_flatfile}{config.FLATFILE_NAME}' in file:
                        consolidatedfiles = file
                        print(f"Found consolidated file: {consolidatedfiles}")
                        # Download the consolidatedfile (example)
                        flatfilename = f'{awsconfig.test_directory}/{config.CATEGORY_NAME}_flatfiledata.xlsx'
                        s3.download_file(awsconfig.bucketname,
                                         consolidatedfiles,
                                         flatfilename)
                    else:
                        print("No files found in the specified bucket and prefix.")
        except NoCredentialsError:
            print("Error: No AWS credentials found.")
        except PartialCredentialsError:
            print("Error: Incomplete AWS credentials.")
        except Exception as e:
            print(e)
        return flatfilename

    def upload_file_to_s3(self, file_path, destination_key):
        s3 = boto3.client('s3')
        try:
            s3.upload_file(file_path, awsconfig.bucketname, destination_key)
            print(f"File uploaded successfully to s3://{awsconfig.bucketname}/{destination_key}")
        except Exception as e:
            print(e)

    def read_uploaded_file_as_dataframe(self, s3_key):
        s3 = boto3.client('s3')
        try:
            obj = s3.get_object(Bucket=awsconfig.bucketname, Key=s3_key)
            df = pd.read_excel(BytesIO(obj['Body'].read()))
            print(f"File read successfully from s3://{awsconfig.bucketname}/{s3_key}")
            return df
        except Exception as e:
            print(e)
            return None

    def upload_report_file(self, file_path, report_key):
        self.upload_file_to_s3(file_path, report_key)

    def check_ground_truth_isexists(self)->bool:
        s3 = boto3.client('s3')
        try:
            response = s3.list_objects_v2(Bucket=awsconfig.bucketname,
                                          Prefix=awsconfig.groundtruth_files_path)
            if 'Contents' in response:
                files = [content['Key'] for content in response['Contents']]
                # print(files)
                print(f"Checking : {ground_truth_file_name}")
                if ground_truth_file_name in files:
                    groundtruth = f'{awsconfig.test_directory}/{config.CATEGORY_NAME}_groundtruth.xlsx'
                    print(f"Found Groundtruth File: {ground_truth_file_name}")
                    s3.download_file(awsconfig.bucketname,
                                     ground_truth_file_name,
                                     groundtruth)
                    gt_file_path = os.path.abspath(groundtruth)
                    return True

                else:
                    print("Groundtruth file not found")
                    return False
        finally:
            print("Trigerring the GT comparision")


# flatfilepath = "lsi_poc/data_steward/final_flat_files/"
# uploadpath = "lsi_poc/data_steward/validation_data/reports/"
# res= S3utils("Treasury&Banking")
