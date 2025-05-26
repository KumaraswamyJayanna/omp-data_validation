# from config import GTPATH, OUTPUTPATH
from data_preprocess import DataCleaning
from generatereport import ExcelReport
from summary_levelreport import File_Report


class Runcomparision:
    def __init__(self):
        pass

    def pipeline_vs_gtcomparision(self, testfile, gtfile):
        data_preprocess = DataCleaning(testfile, gtfile)
        extra_columns = data_preprocess.check_column_difference()
        print(f"Extra columns : {extra_columns}")
        structured_baseline_df, structured_compare_df = data_preprocess.filter_columns()
        print("Create a validation report after pre-processing the Data")
        print("Get the mismatches column wise")
        out, preprocess_pipeline, preprocess_gt = data_preprocess.compare_and_highlight_excel()
        report_generator = ExcelReport(file1_path=preprocess_pipeline, file2_path=preprocess_gt)
        data_compared_report = report_generator.generate_report()
        print("Generating the Summary report")
        summary_report = File_Report(data_compared_report)
        summary_report.generate_report()
        print("Done")


runcomparision = Runcomparision()
# runcomparision.pipeline_vs_gtcomparision()