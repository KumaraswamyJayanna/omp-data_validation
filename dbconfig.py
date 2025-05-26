""" Configuration sheet for database connection """

# Enter the Category Name
CATEGORY_NAME = "Treasury Services"
FLATFILE_NAME = "Treasury_&_Banking_Consolidated_flatfile.xlsx"
DB_TABLE = "price_point"

# PLease enter the QUERY
QUERY = f"select psm.category_id,p.* from public.price_point p join public.product_service_master psm on p.product_id = psm.product_id where psm.category_id = 14;"

# Enter the database connection details
DB_HOST= "deng-com.c3ywssa8qytw.us-east-1.rds.amazonaws.com"
DB_NAME= "DENGCOM"
DB_USER= "postgres"
DB_PASSWORD= "jYMQJP1bws9mk4uvwD"


# Enter the S3 bucket details
prefix = 'lsi_poc/outputs'
bucket_name = 'deng-us-east-1'
prefix_flatfile = "lsi_poc/data_steward/final_flat_files/"