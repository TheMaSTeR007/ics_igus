import gzip
import hashlib
import os.path
from sql_queries import categories_data_query
import pymysql
import requests


def req_sender(url: str, method: str, query_dict: dict = None, cookies: dict = None, headers: dict = None, params_dict: dict = None) -> bytes or None:
    # Send HTTP request
    print('sending request...')
    _response = requests.request(method=method, url=url, data=query_dict, cookies=cookies, headers=headers, allow_redirects=True, params=params_dict)
    print('response received...')

    response_url = _response.url
    page_hash = hashlib.sha256(string=response_url.encode(encoding='UTF-8', errors='backslashreplace')).hexdigest()

    # Check if response is successful
    if _response.status_code == 404:
        return 'ERROR 404'
    elif _response.status_code != 200:
        print(f"HTTP Status code: {_response.status_code}")  # Print status code if not 200
        return None
    print('Returning Response')
    return _response, page_hash, response_url  # Return the response if successful


def page_checker(url: str, method: str, directory_path: str, table_name: str, _id_: int, page_hash: str, query_dict: dict = None):
    file_path = os.path.join(directory_path, f"{page_hash}")  # Define file path
    if os.path.exists(file_path):  # Check if the file already exists
        print("File exists, reading it...")  # Notify that the file is being read
        print(f"Filename is {page_hash}")
        with gzip.open(filename=file_path, mode='rb') as file:
            file_text = file.read().decode(encoding='UTF-8', errors='backslashreplace')  # Read and decode file
        return file_text  # Return the content of the file
    else:  # If the file already do not exist
        print("File does not exist, Sending request & creating it...")  # Notify that a request will be sent
        output_req_sender = req_sender(url=url, method=method, query_dict=query_dict)
        _response = output_req_sender[0]  # Send the HTTP request
        page_hash = output_req_sender[1] + '.html.gz'  # Getting new Page Hash for url even it redirected or not
        response_url = output_req_sender[2]
        if _response is not None:
            print(f"Filename is {page_hash}")
            # Updating the Page Hash in sql table
            page_hash_update_query = f'''UPDATE `{table_name}` SET filename = '{page_hash}', response_url = '{response_url}'  WHERE id = {_id_};'''
            try:
                cursor.execute(page_hash_update_query)
            except Exception as error:
                print(error)
            file_path = os.path.join(directory_path, f"{page_hash}")  # Define file path
            with gzip.open(filename=file_path, mode='wb') as file:
                file.write(_response.content)  # Write response content if it is bytes
            return _response.text  # Return the response text


# Connecting to the Database
connection = pymysql.connect(host='localhost', user='root', database='igus_db', password='actowiz', charset='utf8mb4', autocommit=True)
if connection.open:
    print('Database connection Successful!')
else:
    print('Database connection Un-Successful.')
cursor = connection.cursor()

# Creating Tables in Database If Not Exists
try:
    cursor.execute(query=categories_data_query)
    print('Data fetched from table')
except Exception as e:
    print(e)

table_name = 'categories_data'

# Inserting categories links into Database table
insert_query = ''''''

# Retrieving link, id and filename for checking if file exists or not
select_query = f'''SELECT * FROM `{table_name}` WHERE category_status = 'Pending';'''
cursor.execute(select_query)
categories_data = cursor.fetchall()

for this_category_data in categories_data:
    index_id_ = this_category_data[0]  # index of category url fetched from Database Table
    print('Index id: ', index_id_)
    request_url = this_category_data[1]  # category url fetched from Database Table used for Requesting
    print('Request Url: ', request_url)
    filename_hash_ = this_category_data[2]  # Hash of url by which name file is saved fetched from Database Table
    response_url = this_category_data[3]  # Final Url of Category received in Response
    print('Response Url: ', response_url)
    category_page = page_checker(url=request_url, method="GET", directory_path=os.getcwd(), table_name=table_name, _id_=index_id_, page_hash=filename_hash_)
    print('Category page saved.')

    # Updating category_status to DONE
    update_query_category_status = f'''UPDATE `{table_name}` SET category_status = 'Done' WHERE id = {index_id_};'''
    cursor.execute(update_query_category_status)


# main_page_link_insert_query = f'''INSERT INTO `main_page_table` (main_link) VALUES ('{main_page_link}');'''
