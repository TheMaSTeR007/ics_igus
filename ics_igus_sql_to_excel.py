import pandas as pd
import pymysql

# import mysql.connector


# connection = mysql.connector.connect(host="localhost",user="root",password="actowiz")

# # Creating a connection to SQL Database
connection = pymysql.connect(host='localhost', user='root', database='igus_db', password='actowiz', charset='utf8mb4', autocommit=True)
# if connection.open:
#     print('Database connection Successful!')
# else:
#     print('Database connection Un-Successful.')
# cursor = connection.cursor()  # Creating a cursor to execute SQL Queries

select_query = '''SELECT * FROM igus_db.products_variants_links_page_table;'''  # Query that will retrieve all data from Database table

data_frame = pd.read_sql(sql=select_query, con=connection)  # Reading Data from Database table and converting into DataFrame

# data_frame.to_excel('ics_igus_products_variant_links.xlsx')  # Converting DataFrame into Excel file

df = data_frame
writer = pd.ExcelWriter(
            f"ics_igus_products_variant_links.xlsx_PART_.xlsx",
            engine='xlsxwriter',
            engine_kwargs={'options': {'strings_to_urls': False}}
        )
# writer.book.use_zip64()
df.to_excel(writer)
writer.close()
