import datetime
import os
import re

import numpy as np
import pandas as pd
from openpyxl import Workbook, load_workbook

flatfile = 'Downloads/filtered_Linkedin_Upload_DB_uat.csv'
category_name = "linkedin"

class extracttestdata:
    def __init__(self, flatfile):
        self.flatfile = flatfile
        if self.flatfile.endswith('.xlsx'):
            self.df = pd.read_excel(self.flatfile)
        else:
            self.df = pd.read_csv(self.flatfile)


    def get_file_names(self):
        '''get the filenames from the report'''
        filenames = self.df['File_Name'].unique()
        return filenames

    def drop_zerovalues_in_unitprice(self):
        '''Drop rows with zero values in the "Unit Price" column'''
        self.df = self.df[self.df['Unit Price'] != 0]


    def filter_and_count(self):
        '''Filter the dataframe based on "Product_Service_SKU_Name_Normalized" and count repeated values.
        Add the count, minimum, maximum, and average unit price in separate columns.'''

        # Group by "Product_Service_SKU_Name_Normalized" and calculate required statistics
        grouped_data = self.df.groupby('Product_Service_SKU_Name_Normalized').agg(
            No_of_price_points=('Product_Service_SKU_Name_Normalized', lambda x: x.size - 1),
            Low_Price=('Unit Price', 'min'),
            # Max_Unit_Price=('Unit Price', 'max'),
            Percentile_25th=('Unit Price', lambda x: np.percentile(x, 25)),
            Avg_Price=('Unit Price', 'mean')
        ).reset_index()

        # Merge the grouped data back to the original dataframe
        merged_data = pd.merge(self.df, grouped_data, on='Product_Service_SKU_Name_Normalized', how='left')

        # Write the updated dataframe to a new Excel file
        # merged_data.to_excel('Filtered_Countstb1.xlsx', index=False)
        return merged_data


    def filter_and_write_in_singlesheet(self):
        filtereddata = self.filter_and_count()
        filtered_data = filtereddata[['Level 5 Category', 'Product_Service_SKU_Name_Original', 'Product_Service_SKU_Name_Normalized', 'UOM', 'Unit Price',
                'No_of_price_points', 'Low_Price', 'Percentile_25th', 'Avg_Price', 'File_Name']]
        filtered_data.to_excel(f'expected_testdata_fileview_{category_name}.xlsx', index=False)



    def filter_and_write(self, data):

        '''Filter data by filename and write to separate Excel sheets'''
        self.df = data
        with pd.ExcelWriter(f'Expected_data_{category_name}.xlsx', engine='openpyxl') as writer:
            for filename in self.get_file_names():
                filtered_data = self.df[self.df['File_Name'] == filename]
                filtered_data = filtered_data[['Level 5 Category', 'Product_Service_SKU_Name_Original', 'Product_Service_SKU_Name_Normalized', 'UOM', 'Unit Price',
                'No_of_price_points', 'Low_Price', 'Percentile_25th', 'Avg_Price']]
                filtered_data = filtered_data.sort_values(by=['Level 5 Category','Product_Service_SKU_Name_Normalized','Product_Service_SKU_Name_Original',])
                filtered_data.to_excel(writer, sheet_name=str(filename), index=False)


    def filter_unique_and_add_stats(self):
        '''Filter the dataframe based on unique "Product_Service_SKU_Name_Normalized" values and add count, min, and avg unit price as new columns.'''
        grouped_data = self.df.groupby('Product_Service_SKU_Name_Normalized').agg(
            No_of_price_points=('Product_Service_SKU_Name_Normalized', 'size'),
            Low_Price=('Unit Price', 'min'),
            Percentile_25th=('Unit Price', lambda x: np.percentile(x, 25)),
            Avg_Price=('Unit Price', 'mean')
        ).reset_index()

        # Merge the grouped data back to the original dataframe
        self.df = pd.merge(self.df, grouped_data, on='Product_Service_SKU_Name_Normalized', how='left')
        self.df = self.df.drop_duplicates(subset=['Product_Service_SKU_Name_Normalized'])
        category_analysis_stats = self.df[['Level 5 Category', 'Product_Service_SKU_Name_Normalized', 'UOM',
                'No_of_price_points', 'Low_Price', 'Percentile_25th', 'Avg_Price',]]
        category_analysis_stats = category_analysis_stats.sort_values(by=['Level 5 Category','Product_Service_SKU_Name_Normalized'])
        category_analysis_stats.to_excel(f'expecteddata_categorywise_{category_name}.xlsx', index=False)


if __name__ == '__main__':

    res= extracttestdata(flatfile)
    res.drop_zerovalues_in_unitprice()
    # uncomment the below line if you need the data in a separate sheet based on filename
    # res.filter_and_write(processeddata)
    res.filter_and_write_in_singlesheet()
    res.filter_unique_and_add_stats()
