import pymysql
import requests, os, gzip, hashlib, json
from lxml import html
from sql_queries import products_links_query, categories_links_query


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


def req_sender_redirected(url: str, method: str, query_dict: dict = None, cookies: dict = None, headers: dict = None) -> bytes or None:
    # Prepare headers for the HTTP request

    redirected_url = 'https://www.igus.com/' + '/'.join(url.split('/')[-3:-1])

    # Send HTTP request
    _response = requests.request(method=method, url=redirected_url, data=query_dict, cookies=cookies, headers=headers, allow_redirects=True)

    # Checking if the request is Redirected or not
    if _response.history:
        location = _response.history[-1].headers.get('location')[1:]
        if location:
            print(location)
            redirected_url = url.replace('/'.join(url.split('/')[-3:-1]), location) if not location.startswith('https://www.igus.com') else url.replace('/'.join(url.split('/')[-3:-1]), location.replace('https://www.igus.com', ''))
            print('red', redirected_url)
            print('Redirected to: ', redirected_url)
        else:
            redirected_url = url.replace('/'.join(url.split('/')[-3:-1]), _response.history[-1].url)
            print('blue', redirected_url)
            print('Redirected to: ', redirected_url)
        _response = requests.request(method=method, url=redirected_url, data=query_dict, cookies=cookies, headers=headers, allow_redirects=True)

    # Check if response is successful
    if _response.status_code == 404:
        return 'ERROR 404'
    elif _response.status_code != 200:
        print(f"HTTP Status code: {_response.status_code}")  # Print status code if not 200
        return None
    print('Returning Response')
    return _response  # Return the response if successful


def ensure_dir_exists(dir_path: str):
    # Check if directory exists, if not, create it
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
        print(f'Directory {dir_path} Created')  # Print confirmation of directory creation


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
        _response = req_sender_redirected(url=url, method=method, query_dict=query_dict, cookies=cookies, headers=headers)  # Send the GET request
        if _response is not None:
            response_json = _response.json()  # Get the JSON response
            print(f"Filename is {page_hash}")
            with open(file_path, 'w', encoding='UTF-8') as file:
                json.dump(response_json, file, ensure_ascii=False, indent=4)  # Write JSON response to file
            return response_json  # Return the JSON response as a dictionary


def page_checker_json_new(url: str, method: str, directory_path: str, cookies: dict = None, headers: dict = None, query_dict: dict = None, params_dict: dict = None):
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
        _response = req_sender(url=url, method=method, query_dict=query_dict, cookies=cookies, headers=headers, params_dict=query_dict)  # Send the GET request
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
        try:
            self.cursor.execute(query=categories_links_query)
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
        # Requesting on Main Page for getting the Browse all categories links
        print('Main Page Url: ', self.main_page_url)
        main_page_text = self.page_checker(url=self.main_page_url, method='GET', directory_path=os.path.join(self.project_files_dir, 'Main_Pages'))
        parsed_main_html = html.fromstring(main_page_text)  # Parsing the main page response text
        xpath_products_browse = '//a[@title="Browse all Products"]/@href'
        products_browse_link = self.main_page_url[:-1] + ' '.join(parsed_main_html.xpath(xpath_products_browse))
        print(products_browse_link)
        # Requesting on Browse all categories link for getting all categories links
        print('Products Browse Url: ', products_browse_link)
        products_browse_page_text = self.page_checker(url=products_browse_link, method='GET', directory_path=os.path.join(self.project_files_dir, 'Products_Browse_Pages'), )
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


Scraper().scrape()
