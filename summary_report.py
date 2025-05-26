import os.path

import pandas as pd
from config import OUTPUTPATH, GTPATH, ACCURACYTHRESHOLD, REPORTPATH
from data_preprocess import res

import re

output_path = OUTPUTPATH
gt_path = GTPATH

output_file = REPORTPATH + '/summary_report.xlsx'
# Threshold for file-level accuracy (adjust as needed)
accuracy_threshold = ACCURACYTHRESHOLD

cols_for_primary_key = ['File_Name', 'Product_Service_SKU_Name_Original']


def clean_column(value):
    # Remove special characters and spaces, convert to lowercase
    return re.sub(r'[^a-zA-Z0-9]', '', value).lower()


def generate_primary_key(df, columns):
    # Ensure the columns exist in the DataFrame
    if not all(col in df.columns for col in columns):
        raise ValueError("Some columns in the list are not present in the DataFrame.")

    # Generate primary key by concatenating cleaned values row-wise
    primary_key = df[columns].apply(
        lambda row: ''.join(
            ''.join(filter(str.isalnum, str(row[col]))).lower()  # Remove non-alphanumeric characters and lowercase
            for col in columns
        ),
        axis=1
    )

    return primary_key


df_output = pd.read_excel(output_path)
df_gt = pd.read_excel(gt_path)

df_result = pd.DataFrame()
row_length, column_length = df_output.shape

files = df_gt['File_Name'].unique()
df_result['File_Name'] = files

df_result['Total Rows per File as per GT'] = df_result['File_Name'].map(df_gt.groupby('File_Name')['File_Name'].count())

df_gt['Product_Service_SKU_Name_Original'] = df_gt['Product_Service_SKU_Name_Original'].apply(clean_column)
df_output['Product_Service_SKU_Name_Original'] = df_output['Product_Service_SKU_Name_Original'].apply(clean_column)

df_output.insert(0, "Primary_key_output", generate_primary_key(df_output, cols_for_primary_key))
df_gt.insert(0, "Primary_key_gt", generate_primary_key(df_gt, cols_for_primary_key))

df_merged = df_gt.merge(df_output, left_on="Primary_key_gt", right_on="Primary_key_output", how="outer")

mapping_sku_absence_in_output = df_merged.groupby('File_Name_x')['Primary_key_output'].apply(
    lambda x: x.isnull().sum()).to_dict()
df_result['Missing Extractions (Complete Row)'] = df_result['File_Name'].map(mapping_sku_absence_in_output).fillna(
    0).astype(int)

mapping_extra_extraction = df_merged.groupby('File_Name_x')['Primary_key_gt'].apply(
    lambda x: x.isnull().sum()).to_dict()
df_result['Extra Extractions (Not Present in Ground Truth)'] = df_result['File_Name'].map(
    mapping_extra_extraction).fillna(0).astype(int)

df_result["Duplicates Extraction"] = df_merged.duplicated()
pipeline_duplicate_rows = df_merged.duplicated(keep=False)
true_duplicates = pipeline_duplicate_rows.sum()

mapping_incorrect_orginal_name_extraction = \
df_merged[df_merged['Product_Service_SKU_Name_Original_x'] != df_merged['Product_Service_SKU_Name_Original_y']].groupby(
    'File_Name_x')['File_Name_x'].count()
df_result['Product_Service_SKU_Name_Original'] = df_result['File_Name'].map(
    mapping_incorrect_orginal_name_extraction).fillna(0)

# take column and drop the columns not in cols to check
columns = df_output.columns
cols = ["Product_Service_SKU_Name_Normalized",
        "Level 5 Category",
        "UOM",
        "Product_Service_SKU_Name_Original",
        "Client_Spend",
        "Price_Date",
        "Quantity",
        "Total_Price",
        "Unit Price",
        "Price_Source",
        "Store_No",
        "Price_Factor",
        "Package_Price",
        "Currency_Code",
        "Exchange_Rate",
        "Price_Type",
        "Year",
        "System_DateTime",
        "Store_Days_Of_Service",
        "Billing_Frequency",
        "Payment_Term",
        "Contract_Duration",
        "QPU",
        "Level 3",
        "Level 4",
        "Product_Service_Item_Description",
        "Product_Service_SKU_Number",
        "Manufacturer_Name",
        "Manufacturer_Part_Number",
        "Manufacturer_Item_Description",
        "Package_Flag",
        "Product_Service_Type",
        "Universal_Product_Code",
        "Supplier_Address",
        "Supplier_Country",
        "Supplier_Address1",
        "Supplier_Address2",
        "Supplier_City",
        "Supplier_Postal_Code",
        "Supplier_State",
        "Client_Address",
        "Client_Address1",
        "Client_Address2",
        "Client_City", "Client_State",
        "Client_Country",
        "Client_Postal_Code",
        "Client_Industry_1",
        "Client_Industry 2",
        "Client_Industry 3",
        "Client_Revenue_Band",
        "Store_Address",
        "Store_City",
        "Store_Zip_Code",
        "Store_Zone",
        "Service_Frequency"]

cols_to_check = [
    'Product_Service_SKU_Name_Normalized',
    'Level 5 Category',
    'UOM',
    'Supplier_Name_Original',
    'Client_Name_Original',
    'Supplier_Name_Normalized',
    'Client_Normalized_ID',
    "Client_Spend",
    "Price_Date",
    "Quantity",
    "Total_Price",
    "Unit Price",
    "Price_Source",
    "Store_No",
    "Price_Factor",
    "Package_Price",
    "Currency_Code",
    "Exchange_Rate",
    "Price_Type",
    "Year",
    "System_DateTime",
    "Store_Days_Of_Service",
    "Billing_Frequency",
    "Payment_Term",
    "Contract_Duration",
    "QPU",
    "Level 3",
    "Level 4",
    "Product_Service_Item_Description",
    "Product_Service_SKU_Number",
    "Manufacturer_Name",
    "Manufacturer_Part_Number",
    "Manufacturer_Item_Description",
    "Package_Flag",
    "Product_Service_Type",
    "Universal_Product_Code",
    "Supplier_Address",
    "Supplier_Country",
    "Supplier_Address1",
    "Supplier_Address2",
    "Supplier_City",
    "Supplier_Postal_Code",
    "Supplier_State",
    "Client_Address",
    "Client_Address1",
    "Client_Address2",
    "Client_City",
    "Client_State",
    "Client_Country",
    "Client_Postal_Code",
    "Client_Industry_1",
    "Client_Industry 2",
    "Client_Industry 3",
    "Client_Revenue_Band",
    "Store_Address",
    "Store_City",
    "Store_Zip_Code",
    "Store_Zone",
    "Service_Frequency"

]

for i in range(len(cols)):
    # Check for mismatched values, excluding cases where both are NaN
    mismatched = df_merged[
        (df_merged[f'{cols_to_check[i]}_x'] != df_merged[f'{cols_to_check[i]}_y']) &
        ~(pd.isna(df_merged[f'{cols_to_check[i]}_x']) & pd.isna(df_merged[f'{cols_to_check[i]}_y']))
        ]

    # Group by 'File_Name_x' and count mismatches
    mappings = mismatched.groupby('File_Name_x').size().to_dict()

    # Map mismatches to the result DataFrame
    df_result[cols[i]] = df_result['File_Name'].map(mappings).fillna(0).astype(int)

# Calculate overall accuracy and affected files for each measure
results = []
measures = list(df_result.columns)[2:]



df_result_summary = pd.DataFrame(
    columns=["Iteration Number", "Issue Type", "Issue Level", "Overall Accuracy Percentage",
             "Number of Files affected", "Percentage_of_Missing", "Percentage of Incorrect"])

mismatches = res.find_mismatches()
mismatches["Duplicates Extraction"] = true_duplicates if true_duplicates >= accuracy_threshold else 0

print("Generating Summary report...")
# Calculate overall accuracy and affected files for each measure
for measure in measures:
    # iteration number has to handle in pipeline while naming the file incremental number in suffix
    file_level_issue = ["Missing Extractions (Complete Row)", "Extra Extractions (Not Present in Ground Truth)",
                        "Duplicates Extraction"]
    total_errors = df_result[measure].sum()
    total_rows = df_result["Total Rows per File as per GT"].sum()
    accuracy_percentage = round(100 - (total_errors / total_rows * 100), 2)
    affected_files = (df_result[measure] > 0) & (
    (100 - df_result[measure] / df_result["Total Rows per File as per GT"] * 100))
    num_affected_files = affected_files.sum()
    percentage_of_missing = (df_result[measure].isnull().sum() / len(df_result[measure]) * 100)
    percentage_of_incorrect = mismatches[measure] if measure in list(mismatches.keys()) else "-"

    # Append results to the DataFrame
    df_records_field_type = {
        "Iteration Number": "0.0",
        "Issue Type": measure,
        "Issue Level": "Field" if measure not in file_level_issue else "File",
        "Overall Accuracy Percentage": float(accuracy_percentage),
        "Number of Files affected (have file level accuracy < x%)": float(num_affected_files),
        "Perrcentage of missing": float(percentage_of_missing),
        "percentage_of_Incorrect_values": percentage_of_incorrect
    }

    df_result_summary.loc[measures.index(measure) + 1] = df_records_field_type.values()

with pd.ExcelWriter(output_file) as writer:
    df_result_summary.to_excel(writer, sheet_name='Category_Issue_Summary')
    df_result.to_excel(writer, sheet_name='File Level Accuracy', index=False)
    print(f"Generated reports are copied here: {os.path.abspath(output_file)}")
