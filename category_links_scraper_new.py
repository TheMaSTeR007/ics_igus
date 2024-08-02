import math
import pymysql
import requests, os, gzip, hashlib, json
from lxml import html
from sql_queries import products_links_query, categories_links_query, main_page_table_query, categories_data_query, products_browse_table_query


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


def ensure_dir_exists(dir_path: str):
    # Check if directory exists, if not, create it
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
        print(f'Directory {dir_path} Created')  # Print confirmation of directory creation


class Scraper:
    def __init__(self):
        # Connecting to the Database
        connection = pymysql.connect(host='localhost', user='root', database='igus_db', password='actowiz', charset='utf8mb4', autocommit=True)
        if connection.open:
            print('Database connection Successful!')
        else:
            print('Database connection Un-Successful.')
        self.cursor = connection.cursor()

        # Creating Table in Database If Not Exists
        try:
            self.cursor.execute(query=products_links_query)
        except Exception as e:
            print(e)
        try:
            self.cursor.execute(query=categories_links_query)
        except Exception as e:
            print(e)
        try:
            self.cursor.execute(query=main_page_table_query)
        except Exception as e:
            print(e)
        try:
            self.cursor.execute(query=categories_data_query)
        except Exception as e:
            print(e)
        try:
            self.cursor.execute(query=products_browse_table_query)
        except Exception as e:
            print(e)

        # Creating Saved Pages Directory for this Project if not Exists
        project_name = 'Ics_Igus'

        self.project_files_dir = f'C:\\Project Files\\{project_name}_Project_Files'
        ensure_dir_exists(dir_path=self.project_files_dir)

        self.main_page_url = 'https://www.igus.com/'

    def page_checker(self, url: str, method: str, directory_path: str, table_name: str, _id_: int, page_hash: str, query_dict: dict = None):
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
                    self.cursor.execute(page_hash_update_query)
                except Exception as error:
                    print(error)
                file_path = os.path.join(directory_path, f"{page_hash}")  # Define file path
                with gzip.open(filename=file_path, mode='wb') as file:
                    file.write(_response.content)  # Write response content if it is bytes
                return _response.text  # Return the response text

    def scrape(self):
        main_page_table = 'main_page_table'
        # Inserting Main Page Url into Database Table
        main_page_link_insert_query = f'''INSERT INTO `{main_page_table}` (main_page_link) VALUES ('{self.main_page_url}');'''
        try:
            self.cursor.execute(main_page_link_insert_query)
        except Exception as error:
            print('Main Page url INSERTING ERROR: ', error)

        # Retrieving Main Page Url from Database Table
        fetch_query = f'''SELECT * FROM `{main_page_table}` WHERE main_page_link_status = 'Pending';'''
        self.cursor.execute(fetch_query)
        main_page_link_data = self.cursor.fetchone()
        main_page_id_ = main_page_link_data[0]
        main_page_url = main_page_link_data[1]
        main_page_filename = main_page_link_data[2]

        # Requesting on Main Page for getting the Browse all categories links
        print('Main Page Url: ', main_page_url)
        main_page_text = self.page_checker(url=self.main_page_url, method='GET', directory_path=os.path.join(self.project_files_dir, 'Main_Pages'), page_hash=main_page_filename, table_name='main_page_table', _id_=main_page_id_)
        parsed_main_html = html.fromstring(main_page_text)  # Parsing the main page response text
        xpath_products_browse = '//a[@title="Browse all Products"]/@href'
        products_browse_link = self.main_page_url[:-1] + ' '.join(parsed_main_html.xpath(xpath_products_browse))

        products_browse_page_table = 'products_browse_page_table'
        # Inserting Categories Browse Page Url into Database Table
        products_browse_link_insert_query = f'''INSERT INTO `{products_browse_page_table}` (products_browse_link) VALUES ('{products_browse_link}');'''
        try:
            self.cursor.execute(products_browse_link_insert_query)
        except Exception as error:
            print('Products Browse Page url INSERTING ERROR: ', error)

        # Retrieving Categories Browse Page Url from Database Table
        fetch_query = f'''SELECT * FROM `{products_browse_page_table}` WHERE products_browse_link_status = 'Pending';'''
        self.cursor.execute(fetch_query)
        main_page_link_data = self.cursor.fetchone()
        products_browse_link_id_ = main_page_link_data[0]
        products_browse_link = main_page_link_data[1]
        products_browse_filename = main_page_link_data[2]

        # Requesting on Browse all categories link for getting all categories links
        print('Products Browse Url: ', products_browse_link)
        products_browse_page_text = self.page_checker(url=products_browse_link, method='GET', directory_path=os.path.join(self.project_files_dir, 'Products_Browse_Pages'), page_hash=products_browse_filename, table_name=products_browse_page_table, _id_=products_browse_link_id_)
        parsed_browser_page = html.fromstring(products_browse_page_text)
        xpath_category_links = "//a[@title='Browse the Shop']/@href"
        category_links = parsed_browser_page.xpath(xpath_category_links)

        categories_data_table_name = 'categories_data'
        # Inserting into Table for fetching in further code
        insert_query = f'''INSERT INTO `{categories_data_table_name}` (category_link)
                            VALUES (%s);'''
        try:
            self.cursor.executemany(insert_query, args=[(self.main_page_url[:-1] + link,) for link in category_links])
        except Exception as e:
            print(e)

        # Fetching Categories links whose status is "Pending" for faster execution
        fetch_query = f'''SELECT * FROM `{categories_data_table_name}` WHERE category_status = 'Pending';'''
        self.cursor.execute(fetch_query)
        categories_data = self.cursor.fetchall()

        # Iterating on each category links for getting their product's link
        for this_category_data in categories_data:
            category_index_id_ = this_category_data[0]  # index of category url fetched from Database Table
            print('Category Index id: ', category_index_id_)
            category_filename_hash_ = this_category_data[2]  # Hash of url by which name file is saved fetched from Database Table

            category_link_ = this_category_data[1]
            category_link = category_link_
            print('Category Url: ', category_link)
            # Sending Request on category pages for saving their pages and also getting redirected urls
            self.page_checker(url=category_link, method='GET', directory_path=os.path.join(self.project_files_dir, 'Categories_Pages'), page_hash=category_filename_hash_, table_name=categories_data_table_name, _id_=category_index_id_)
            print('Category page saved.')

            print('-'*100)


Scraper().scrape()
