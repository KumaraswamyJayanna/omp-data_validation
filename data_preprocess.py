import os
import re
from datetime import datetime

import pandas as pd
from config import COLUMN_VALUE_TO_SORTBY, KEYS, OUTPUTFILE, VALIDATIONREPORT
from openpyxl import load_workbook


class ExcelCompare:

    def __init__(self, baseline_file, compare_file):
        self.baseline_file = baseline_file
        self.compare_file = compare_file
        self.baseline_df = pd.read_excel(baseline_file)
        self.compare_df = pd.read_excel(compare_file)
        self.missing_rows_df = pd.DataFrame()
        self.result_df = self.baseline_df.copy()
        self.mismatched_cells = {}
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        # self.varvalue = baseline_file.split("/")[-1].split(".")[0][0:14] + self.timestamp
        self.varvalue = "temp"
        self.reportname = VALIDATIONREPORT + "_result_" + self.varvalue + ".xlsx"

    def check_column_difference(self):
        unique = set(self.baseline_df.columns).symmetric_difference(self.compare_df.columns)
        return list(unique)

    def filter_columns(self):
        invalid_columns = self.check_column_difference()
        print(invalid_columns)
        invalid_columns.extend(["System_DateTime", "Key"])
        self.compare_df = self.compare_df.reindex(columns=self.baseline_df.columns)

        try:
            invalid_column_compare_df = [col for col in invalid_columns if col in self.compare_df.columns]
            filtered_compare_df = self.compare_df.drop(columns=invalid_column_compare_df, axis=1, errors='ignore')
            invalid_column_baseline_df = [col for col in invalid_columns if col in self.baseline_df.columns]
            filtered_baseline_df = self.baseline_df.drop(columns=invalid_column_baseline_df, axis=1, errors='ignore')
            return filtered_baseline_df, filtered_compare_df
        except KeyError:
            print("Error: One or more columns not found in dataFrame")
            # return self.baseline_df


class DataCleaning(ExcelCompare):

    def __init__(self, baseline_file, compare_file):
        super().__init__(baseline_file, compare_file)

    def reorder_columns_to_match(self):
        columns_df1 = self.baseline_df.columns
        columns_df2 = self.compare_df.columns

        if set(columns_df1) != set(columns_df2):
            print("Both Dataframes have similar column values")
            return False
        return True

    @staticmethod
    def convertdatetime(df, columnname):
        try:
            df[columnname] = pd.to_datetime(df[columnname])
            df[columnname] = df[columnname].dt.strftime('%Y-%m-%d')
        except Exception as e:
            print("failed to convert Date time")

    def remove_special_characters(self):
        def clean_string(value):
            if isinstance(value, str):
                value = re.sub(r"[^a-zA-Z0-9]", "", value)
            return value

        self.convertdatetime(self.baseline_df, "Price_Date")
        self.convertdatetime(self.compare_df, "Price_Date")
        structured_baseline_df, structured_compare_df = self.filter_columns()
        return structured_baseline_df, structured_compare_df

    def sort_dataframe_alphabetically(self):
        column_name = COLUMN_VALUE_TO_SORTBY
        clean_baseline_df, clean_compare_df = self.remove_special_characters()
        sorted_baseline_df = clean_baseline_df.sort_values(by=column_name, ascending=True)
        sorted_compare_df = clean_compare_df.sort_values(by=column_name, ascending=True)
        return sorted_baseline_df, sorted_compare_df

    # def check_row_wise_in_dataframe(self):
    #     df1, df2 = self.sort_dataframe_alphabetically(),
    #     is_present = pd.Series([False] * len(df1), index=df1.index)

    #     for i, row in df1.iterrows():
    #         if ((df2 == row).all(axis=1)).any():
    #             is_present[i] = True
    #     return is_present

    def find_mismatches(self):
        df1, df2 = self.sort_dataframe_alphabetically()
        mismatches = {}
        columns_to_compare = list(df1.columns)
        for column in columns_to_compare:
            merged_df = df1.merge(df2, suffixes=('_df1', '_df2'), how='outer')
            mismatch_column = f'{column}_Mismatch'
            merged_df[mismatch_column] = merged_df[f'{column}'] != merged_df[f'{column}']
            true_mismatches = merged_df[mismatch_column].sum()
            false_mismatches = (~merged_df[mismatch_column]).sum()
            true_value = round((true_mismatches / (true_mismatches + false_mismatches)) * 100, 2)
            mismatches[column] = 100 - float(true_value)
        return mismatches


    def create_excel_with_dataframes(self):
        filename = VALIDATIONREPORT +"/Output_for_manual_verify" + self.timestamp +".xlsx"
        if not filename.endswith('.xlsx'):
            raise ValueError("Invalid file extension. Please use '.xlsx' extension for the filename.")

        dataframes = list(self.sort_dataframe_alphabetically())
        sheet_name = ["pipeline_output", "GroundTruth_output"]
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            for df, sheet_name in zip(dataframes, sheet_name):
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        path = os.path.relpath(filename)
        print(f'Excel file has been created for manual validation: {path}')

        return path

    @staticmethod
    def generate_key_for_pseudo_column(df):
        new_col_name = "Pseudo_column"
        keys = KEYS
        columnkeys = ""
        for x in range(len(keys)):
            columnkeys += df[keys[x]].astype(str).replace(" ", "")
            # debug statements
            # print(type(columnkeys))
            # print(f"{columnkeys} type is {type(columnkeys)}")
        df[new_col_name] = columnkeys
        df[new_col_name] = df[new_col_name].str.lower().apply(lambda x: re.sub(r'[^A-Za-z0-9]+', '', x))
        df.insert(0, new_col_name, df.pop(new_col_name))
        return df


    def compare_and_highlight_excel(self):
        # Read the Excel file and sheets
        path = self.create_excel_with_dataframes()

        filepath = path
        print(path)
        sheetname = ["pipeline_output", "GroundTruth_output"]

        df1 = pd.read_excel(filepath, sheet_name=sheetname[0])
        df2 = pd.read_excel(filepath, sheet_name=sheetname[1])
        df1 = self.generate_key_for_pseudo_column(df1)
        # df1.fillna("NULL", inplace=True)
        df2 = self.generate_key_for_pseudo_column(df2)
        # df2.fillna("NULL", inplace=True)
        pipeline_data_processpath = VALIDATIONREPORT+"/pipeline"+self.reportname
        gt_data_processpath = VALIDATIONREPORT+"/groundtruth"+self.reportname
        df1.to_excel(pipeline_data_processpath, index=False, sheet_name="pipeline")
        df2.to_excel(gt_data_processpath, index=False, sheet_name="GT")

        # # Load the workbook and sheets using openpyxl
        workbook = load_workbook(filepath)

        # # Save the workbook with highlighted differences
        workbook.save(OUTPUTFILE)
        outputfilepath = os.path.relpath(OUTPUTFILE)
        print(os.path.relpath(pipeline_data_processpath))
        print(os.path.relpath(gt_data_processpath))
        print(f'Finalized output Excels file has created here: {outputfilepath}')
        # print("Execution Done")
        return outputfilepath, pipeline_data_processpath, gt_data_processpath


# res = DataCleaning(OUTPUTPATH, GTPATH)

