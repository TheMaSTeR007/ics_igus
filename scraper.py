import math
import time

import pymysql
import requests, os, gzip, hashlib, json
from lxml import html
from sql_queries import products_links_query


def req_sender(url: str, method: str, query_dict: dict = None, cookies: dict = None, headers: dict = None) -> bytes or None:
    # Prepare headers for the HTTP request

    # Send HTTP request
    _response = requests.request(method=method, url=url, data=query_dict, cookies=cookies, headers=headers)
    time.sleep(5)
    # Check if response is successful
    if _response.status_code == 404:
        return 'ERROR 404'
    elif _response.status_code != 200:
        print(f"HTTP Status code: {_response.status_code}")  # Print status code if not 200
        return None
    return _response  # Return the response if successful


def ensure_dir_exists(dir_path: str):
    # Check if directory exists, if not, create it
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
        print(f'Directory {dir_path} Created')  # Print confirmation of directory creation


def page_checker(url: str, method: str, directory_path: str, query_dict: dict = None):
    # Create a unique hash for the URL to use as the filename
    page_hash = hashlib.sha256(string=url.encode(encoding='UTF-8', errors='backslashreplace')).hexdigest()
    ensure_dir_exists(dir_path=directory_path)  # Ensure the directory exists
    file_path = os.path.join(directory_path, f"{page_hash}.html.gz")  # Define file path
    if os.path.exists(file_path):  # Check if the file already exists
        print("File exists, reading it...")  # Notify that the file is being read
        print(f"Filename is {page_hash}")
        with gzip.open(filename=file_path, mode='rb') as file:
            file_text = file.read().decode(encoding='UTF-8', errors='backslashreplace')  # Read and decode file
        return file_text  # Return the content of the file
    else:
        print("File does not exist, Sending request & creating it...")  # Notify that a request will be sent
        _response = req_sender(url=url, method=method, query_dict=query_dict)  # Send the HTTP request
        if _response is not None:
            print(f"Filename is {page_hash}")
            with gzip.open(filename=file_path, mode='wb') as file:
                if isinstance(_response, str):
                    file.write(_response.encode())  # Write response if it is a string
                    return _response
                file.write(_response.content)  # Write response content if it is bytes
            return _response.text  # Return the response text


def page_checker_json(url: str, method: str, directory_path: str, cookies: dict = None, headers: dict = None, query_dict: dict = None):
    # Create a unique hash for the URL and data to use as the filename
    hash_input = url + json.dumps(query_dict, sort_keys=True)  # Combine URL and data for hashing
    page_hash = hashlib.sha256(hash_input.encode('UTF-8')).hexdigest()
    ensure_dir_exists(dir_path=directory_path)  # Ensure the directory exists
    file_path = os.path.join(directory_path, f"{page_hash}.json")  # Define file path

    if os.path.exists(file_path):  # Check if the file already exists
        print("File exists, reading it...")  # Notify that the file is being read
        print(f"Filename is {page_hash}")
        with open(file_path, 'r', encoding='UTF-8') as file:
            file_text = file.read()  # Read the file
        return json.loads(file_text)  # Return the content as a dictionary

    else:
        print("File does not exist, Sending request & creating it...")  # Notify that a request will be sent
        _response = req_sender(url=url, method=method, query_dict=query_dict, cookies=cookies, headers=headers)  # Send the GET request
        if _response is not None:
            response_json = _response.json()  # Get the JSON response
            print(f"Filename is {page_hash}")
            with open(file_path, 'w', encoding='UTF-8') as file:
                json.dump(response_json, file, ensure_ascii=False, indent=4)  # Write JSON response to file
            return response_json  # Return the JSON response as a dictionary


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

        # Creating Saved Pages Directory if not Exists
        project_name = 'Ics_Igus'

        self.project_files_dir = f'C:\\Project Files\\{project_name}_Project_Files'
        ensure_dir_exists(dir_path=self.project_files_dir)

        self.main_page_url = 'https://www.igus.com/'

    def scrape(self):
        # Requesting on Main Page for getting the Browse all categories links
        print('Main Page Url: ', self.main_page_url)
        main_page_text = page_checker(url=self.main_page_url, method='GET', directory_path=os.path.join(self.project_files_dir, 'Main_Page'))
        parsed_main_html = html.fromstring(main_page_text)  # Parsing the main page response text
        xpath_products_browse = '//a[@title="Browse all Products"]/@href'
        products_browse_link = self.main_page_url[:-1] + ' '.join(parsed_main_html.xpath(xpath_products_browse))
        # Requesting on Browse all categories link for getting all categories links
        print('Products Browse Url: ', products_browse_link)
        products_browse_page_text = page_checker(url=products_browse_link, method='GET', directory_path=os.path.join(self.project_files_dir, 'Products_Browse_Page'))
        parsed_browser_page = html.fromstring(products_browse_page_text)
        xpath_category_links = '//a[@title="Browse the Shop"]/@href'
        category_links = parsed_browser_page.xpath(xpath_category_links)

        # Iterating on each category links for getting their product's link
        for category_link in category_links:
            category_link = self.main_page_url[:-1] + category_link
            print('Category Url: ', category_link)
            category_page_text = page_checker(url=category_link, method='GET', directory_path=os.path.join(self.project_files_dir, 'Categories_Page'))
            parsed_category_page = html.fromstring(category_page_text)
            xpath_products_count = '//p[contains(text(), "Number of products")]/text() | //div[contains(text(), "Number of products")]//text()'
            products_count = ' '.join(parsed_category_page.xpath(xpath_products_count))
            if products_count:
                print('Products count: ', products_count)
            # if products_count:
            #     page_count = math.ceil(products_count / 15)
            #     print('Pages Count: ', page_count)
            #     for page_no in range(1, page_count + 1):
            #         print('On Page: ', page_no)
            #         category_link += f'/{page_no}'
            #         category_page = page_checker(url=category_link, method='GET', directory_path=os.path.join(self.project_files_dir, 'Products_Page'))
            #         parsed_category_page = html.fromstring(category_page)
            #         xpath_products_link = '//div[@class="btn__text" and text() = "Shop now"]/../@href'
            #         products_links_list = parsed_category_page.xpath(xpath_products_link)
            #         print('No of Proucts: ', len(products_links_list))
            #
            #         #  Iterating on each product's links
            #         for product_link in products_links_list:
            #             product_link = self.main_page_url[:-1] + product_link
            #             print('Product Link: ', product_link)
            #             product_page = page_checker(url=product_link, method='GET', directory_path=os.path.join(self.project_files_dir, 'Products_Page'))
            #             parsed_product_page = html.fromstring(product_page)


                # print('+'*50)

            print('-'*100)



Scraper().scrape()