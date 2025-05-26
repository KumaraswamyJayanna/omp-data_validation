"""Configuration file for the validation framework"""

# Enter the key values to generate a pseudo key
# while selecting the key columns please select the columns are text, avoid float dtype columns
KEYS = ['Product_Service_SKU_Name_Original','File_Name', 'UOM']

# in order to ease access of data sorting the values by column
COLUMN_VALUE_TO_SORTBY = "Product_Service_SKU_Name_Original"

# reports highlighted and summary stored in this path
REPORTPATH = "Reports"

# Intermediate data reports are stored here for debug purpose.
VALIDATIONREPORT = "ValidationData"

# Enter the Category Name
CATEGORY_NAME = "wastemanagement"
CATEGORY_ID = '85'
FLATFILE_NAME = "wastemanagement_flatfile.xlsx"

#stored the output pipeline and Gt output for manual verification
OUTPUTFILE = f'{REPORTPATH}/final_report_{CATEGORY_NAME}.xlsx'