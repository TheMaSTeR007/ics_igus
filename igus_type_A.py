import math
import pymysql
import requests, os, gzip, hashlib, json
from lxml import html
from sql_queries import products_links_query, categories_links_query


def req_sender(url: str, method: str, query_dict: dict = None, cookies: dict = None, headers: dict = None, params_dict: dict = None) -> bytes or None:
    # Send HTTP request
    _response = requests.request(method=method, url=url, data=query_dict, cookies=cookies, headers=headers, allow_redirects=True, params=params_dict)

    # Checking if the request is Redirected or not
    if _response.history:
        location = _response.history[-1].headers.get('location')
        if location:
            redirected_url = 'https://www.igus.com' + location if not location.startswith('https://www.igus.com') else location
            print('red', redirected_url)
            print('Redirected to: ', redirected_url)
        else:
            redirected_url = 'https://www.igus.com' + _response.history[-1].url
            print('blue', redirected_url)
            print('Redirected to: ', redirected_url)
        _response = requests.request(method=method, url=redirected_url, data=query_dict, cookies=cookies, headers=headers, allow_redirects=True, params=params_dict)

        # Check if response is successful
        if _response.status_code == 404:
            return 'ERROR 404'
        elif _response.status_code != 200:
            print(f"HTTP Status code: {_response.status_code}")  # Print status code if not 200
            return None
        print('Returning Redirected Response')
        return _response, redirected_url  # Return the response if successful

    # Check if response is successful
    if _response.status_code == 404:
        return 'ERROR 404'
    elif _response.status_code != 200:
        print(f"HTTP Status code: {_response.status_code}")  # Print status code if not 200
        return None
    print('Returning Direct Response')
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


def page_retriever_main():
    main_page_url = 'https://www.igus.com/'
    # Requesting on Main Page for getting the Browse all categories links
    print('Main Page Url: ', main_page_url)
    main_page_text = page_checker(url=main_page_url, method='GET', directory_path=os.path.join(project_files_dir, 'Main_Pages'))
    parsed_main_html = html.fromstring(main_page_text)  # Parsing the main page response text
    xpath_products_browse = '//a[@title="Browse all Products"]/@href'
    products_browse_link = main_page_url[:-1] + ' '.join(parsed_main_html.xpath(xpath_products_browse))
    print(products_browse_link)


# Creating Saved Pages Directory for this Project if not Exists
project_name = 'Ics_Igus'

project_files_dir = f'C:\\Project Files\\{project_name}_Project_Files'
ensure_dir_exists(dir_path=project_files_dir)


class Scraper:
    def __init__(self):
        # Connecting to the Database
        connection = pymysql.connect(host='localhost', user='root', database='igus_db', password='actowiz', charset='utf8mb4', autocommit=True)
        if connection.open:
            print('Database connection Successful!')
        else:
            print('Database connection Un-Successful.')
        self.cursor = connection.cursor()

        # Creating Tables in Database If Not Exists
        try:
            self.cursor.execute(query=products_links_query)
        except Exception as e:
            print(e)
        try:
            self.cursor.execute(query=categories_links_query)
        except Exception as e:
            print(e)

    def scrape(self):
        # Requesting on Main Page for getting the Browse all categories links
        print('Main Page Url: ', self.main_page_url)
        main_page_text = page_checker(url=self.main_page_url, method='GET', directory_path=os.path.join(self.project_files_dir, 'Main_Pages'))
        parsed_main_html = html.fromstring(main_page_text)  # Parsing the main page response text
        xpath_products_browse = '//a[@title="Browse all Products"]/@href'
        products_browse_link = self.main_page_url[:-1] + ' '.join(parsed_main_html.xpath(xpath_products_browse))
        print(products_browse_link)
        # Requesting on Browse all categories link for getting all categories links
        print('Products Browse Url: ', products_browse_link)
        products_browse_page_text = page_checker(url=products_browse_link, method='GET', directory_path=os.path.join(self.project_files_dir, 'Products_Browse_Pages'))
        parsed_browser_page = html.fromstring(products_browse_page_text)
        xpath_category_links = "//a[@title='Browse the Shop']/@href"

        category_links = parsed_browser_page.xpath(xpath_category_links)

        # Inserting into Table for fetching in further code
        insert_query = f'''INSERT INTO `categories_links` (category_link)
                            VALUES (%s);'''
        try:
            self.cursor.executemany(insert_query, args=[(link,) for link in category_links])
        except Exception as e:
            print(e)

        # Fetching Categories links whose status is "Pending" for faster execution
        fetch_query = '''SELECT * FROM `categories_links` WHERE category_status = 'Pending';'''
        self.cursor.execute(fetch_query)
        categories_data = self.cursor.fetchall()

        # Iterating on each category links for getting their product's link
        for category_data in categories_data:
            cat_id = category_data[0]
            category_link_ = category_data[1]
            category_link = self.main_page_url[:-1] + category_link_
            print('Category Url: ', category_link)
            category_page_text = page_checker(url=category_link, method='GET', directory_path=os.path.join(self.project_files_dir, 'Categories_Pages'))
            parsed_category_page = html.fromstring(category_page_text)

            # If data is available from html page, applying xpath to retrieve it
            xpath_products_count = '//p[contains(text(), "Number of products")]/text() | //div[contains(text(), "Number of products")]//text()'
            products_count = ' '.join(parsed_category_page.xpath(xpath_products_count))

            # If data is not available from html page retrieving it from script tag to retrieve product's count to retrieve data
            xpath_script_next_data = '//script[@id="__NEXT_DATA__"]/text()'
            script_next_data = ' '.join(parsed_category_page.xpath(xpath_script_next_data))

            # Retrieving Category id for sending request on
            script_text_catID = ' '.join(parsed_category_page.xpath('//script[contains(text(), "_filter.ServiceUrl")]/text()'))
            start_index_catId = script_text_catID.find('_filter.ServiceUrl = ') + len('_filter.ServiceUrl = ')
            end_index_catId = script_text_catID.find('";', start_index_catId)
            category_id = script_text_catID[start_index_catId:end_index_catId + 1].strip()
            category_id = category_id[-7:-1]
