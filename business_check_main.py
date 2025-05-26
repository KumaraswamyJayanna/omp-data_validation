import logging as logger

import pandas as pd
from conditional_checks import ConditionalChecks
from config import CATEGORY_NAME
from main import runcomparision
from utils.lookup_data import Lookupdata
from utils.s3_utils import S3utils
from validate_general_checks import Report

# get the pipeline output file as testfile
testfile ='Test_Data/cid-85_wastemanagement_pipeline.xlsx'


class Runvalidationscript(S3utils, Lookupdata):
    def __init__(self):
        pass

    def business_checks(self):
        # pipeline_data_file = input("Enter the pipeline file path : ")
        pipeline_data_file = testfile
        lookup_file = "lookupdata/generic_lookup_file.xlsx"
        print("Executing Business level checks")
        conditional_lookup_file = "lookupdata/lookup_file.xlsx"
        # conditional_lookup_file = input("Enter the conditional lookup file : ")
        genearte_report =  Report(pipeline_data_file, lookup_file, CATEGORY_NAME)
        conditional_checks = ConditionalChecks(pipeline_data_file, conditional_lookup_file)

        conditional_checks.columns_to_lowercase()
        genearte_report.create_logger()
        logger.info(f'Create a logger report')
        logger.info(f'Find the missing Columns in the datafile')
        missing_columns = genearte_report.check_columns_missing()
        logger.info(f'Get the mandatory columns details')
        mandatory_columns = genearte_report.get_mandatory_columns()
        logger.info(f'Generate a report file')
        report_sheet = genearte_report.create_report_sheet()
        logger.info(f'Verify the fields which are all null values')
        # all_null_fields = genearte_report.verify_for_all_null_values()
        logger.info(f"Verify the mandatory column's/Fields are null")
        mandatory_null_fields = genearte_report.mandatory_columns_null_values(mandatory_columns)
        # Highlight the cells in light blue
        genearte_report.highlight_complete_column(report_sheet, columns=mandatory_null_fields, color="C8B6FC")
        logger.info(f"Verify the d-type of the fields")
        verify_dtype = genearte_report.verify_dtype()
        # highlight the cells in light green
        genearte_report.highlight_complete_column(report_sheet, columns=verify_dtype, color="CFE3D9")
        logger.info(f'Verify the conditional checks')
        logger.info(f"Verify the datasheet data contains the expected values")
        conditional_checks.verify_original_name_data(report_sheet)
        logger.info(f"Verifying supplier id and supplier_name_original are mapped correctly")
        conditional_checks.supplier_name_lookup(report_sheet)
        logger.info(f"Verifying client_id and client_name_original are mapped correctly")
        conditional_checks.client_alias_name_verify(report_sheet)
        logger.info(f'Verifying for the price dates')
        conditional_checks.verify_price_date(report_sheet)
        logger.info(f'Verify for the payment columns')
        conditional_checks.verify_payment_term(report_sheet)
        logger.info(f'Verifying for the negative values')
        conditional_checks.verify_for_non_negative(report_sheet)
        print(f"Reports Generated here {report_sheet}")
        logger.info("Execution completed")


    def run(self):
        self.get_lookup_data()
        print("verifying the groundtruth is exists")
        if self.check_ground_truth_isexists():
            logger.info("Executing comparision with Groundtruth")
            groundtruth_file = f'db_ff_test_directory/{CATEGORY_NAME}_groundtruth.xlsx'
            logger.info("Comparision with the groundtruth is in progress")
            # add the local directory path here of pipeline results
            # testfile = f'omp-dataValidation/lookupdata/'
            runcomparision.pipeline_vs_gtcomparision(testfile, groundtruth_file)
        else:
            logger.info("Verifying with the business checks")
            self.business_checks()
            print("verified the business level logics againest datafile")

# execute = Runvalidationscript()
# execute.run()
# # if we need to run business checks
# # execute.business_checks()
