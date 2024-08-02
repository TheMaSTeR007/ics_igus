import re
import time
from sys import argv
from bs4 import BeautifulSoup
import pymysql
import requests, os, gzip, hashlib, json
from lxml import html
from sql_queries import products_links_query, categories_links_query, category_page_table_query, products_links_page_table_query, products_variants_links_page_table_query

start = argv[1]
end = argv[2]


def req_sender_old(url: str, method: str, query_dict: dict = None, cookies: dict = None, headers: dict = None) -> bytes or None:
    # Prepare headers for the HTTP request

    # Send HTTP request
    _response = requests.request(method=method, url=url, data=query_dict, cookies=cookies, headers=headers)
    # Check if response is successful
    if _response.status_code != 200:
        print(f"HTTP Status code: {_response.status_code}")  # Print status code if not 200
        return None
    return _response  # Return the response if successful


def page_checker_old(url: str, method: str, directory_path: str, query_dict: dict = None):
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
        _response = req_sender_old(url=url, method=method, query_dict=query_dict)  # Send the HTTP request
        if _response is not None:
            print(f"Filename is {page_hash}")
            with gzip.open(filename=file_path, mode='wb') as file:
                if isinstance(_response, str):
                    file.write(_response.encode())  # Write response if it is a string
                    return _response
                file.write(_response.content)  # Write response content if it is bytes
            return _response.text  # Return the response text


def req_sender(url: str, method: str, query_dict: dict = None, cookies: dict = None, headers: dict = None, params_dict: dict = None, timeout: int = 30) -> bytes or None:
    # Send HTTP request
    print('sending request...')
    _response = requests.request(method=method, url=url, data=query_dict, cookies=cookies, headers=headers, allow_redirects=True, params=params_dict, timeout=timeout)
    # Making some time delay to let the page download completely
    time.sleep(5)

    # Sending request again after 5 seconds delay if it gets HTTP Status code like 500
    while _response.status_code >= 500:
        print('HTTP Status code:', _response.status_code, 'Sending Request again after 10 Seconds')
        time.sleep(10)
        _response = requests.request(method=method, url=url, data=query_dict, cookies=cookies, headers=headers, allow_redirects=True, params=params_dict, timeout=timeout)
        # Making some time delay to let the page download completely
        time.sleep(5)
    print('response received...')

    response_url = _response.url
    page_hash = hashlib.sha256(response_url.encode(encoding='UTF-8', errors='backslashreplace')).hexdigest()
    print(page_hash)

    response_status_code = _response.status_code
    # Check if response is successful
    if _response.status_code == 404:
        print(f"HTTP Status code: {response_status_code}")  # Print status code if not 200
        with open('404_links.txt', 'a') as file:
            file.write(url + ' -/- ' + response_url + '\n')
        return _response, page_hash, response_url, response_status_code
    elif _response.status_code != 200:
        print(f"HTTP Status code: {response_status_code}")  # Print status code if not 200
        return _response, page_hash, response_url, response_status_code
    elif _response.status_code == 200:
        with open('200_links.txt', 'a') as file:
            file.write(url + ' -/- ' + response_url + '\n')
        print(f"HTTP Status code: {response_status_code}")  # Print status code if not 200
        print('Returning Response')
        return _response, page_hash, response_url, response_status_code  # Return the response if successful
    else:
        with open('other_links.txt', 'a') as file:
            file.write(url + ' -/- ' + response_url + '\n')
        return _response, page_hash, response_url, response_status_code


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
            self.cursor.execute(query=products_variants_links_page_table_query)
        except Exception as e:
            print(e)

        # Creating Saved Pages Directory for this Project if not Exists
        project_name = 'Ics_Igus'

        self.project_files_dir = f'C:\\Project Files\\{project_name}_Project_Files'
        ensure_dir_exists(dir_path=self.project_files_dir)

        self.main_page_url = 'https://www.igus.com/'

    def page_checker(self, url: str, method: str, directory_path: str, table_name: str, _id_: int, page_hash: str, query_dict: dict = None):
        ensure_dir_exists(directory_path)
        file_path = os.path.join(directory_path, f"{page_hash}")  # Define file path
        if os.path.exists(file_path):  # Check if the file already exists
            print("File exists, reading it...")  # Notify that the file is being read
            print(f"Filename is {page_hash}")
            with gzip.open(filename=file_path, mode='rb') as file:
                file_text = file.read().decode(encoding='UTF-8', errors='backslashreplace')  # Read and decode file
            return file_text  # Return the content of the file
        else:  # If the file does not exist
            print("File does not exist, Sending request & creating it...")  # Notify that a request will be sent
            output_req_sender = req_sender(url=url, method=method, query_dict=query_dict)
            print(output_req_sender)
            _response = output_req_sender[0]  # Send the HTTP request
            print(output_req_sender[1])
            page_hash = output_req_sender[1] + '.html.gz'  # Getting new Page Hash for url even it redirected or not
            response_url = output_req_sender[2]
            response_status_code = output_req_sender[3]
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
                    response_text = _response.text if not isinstance(_response, str) else 'HTTP STATUS CODE ERROR ' + response_status_code
                    response_content = _response.content if not isinstance(_response, str) else 'HTTP STATUS CODE ERROR ' + response_status_code
                    file.write(response_content)  # Write response content if it is bytes
                return response_text  # Return the response text

    def scrape(self):
        products_links_page_table_name = 'products_links_page_table'
        # Fetching Products Page links whose status is "Pending" for faster execution
        category_page_fetch_query = f'''SELECT * FROM `{products_links_page_table_name}` WHERE id between {start} and {end} and  product_page_link_status = 'Pending';'''
        self.cursor.execute(category_page_fetch_query)
        products_page_data = self.cursor.fetchall()
        for product_data in products_page_data:
            product_index_id = product_data[0]
            print('Product index in db table', product_index_id)
            product_link = product_data[1]
            print('Product Link:', product_link)
            product_page_hash = product_data[2]
            product_response_url = product_data[3]
            print('Product Response URL:', product_response_url)
            product_category_page_url = product_data[4]
            print('Product category page url:', product_category_page_url)
            product_category_url = product_data[5]
            print('product category url:', product_category_url)

            products_links_page_table = 'products_links_page_table'
            product_page_text = self.page_checker(url=product_link, method='GET', directory_path=os.path.join(self.project_files_dir, 'Product_Pages_Uniques'), table_name=products_links_page_table, page_hash=product_page_hash, _id_=product_index_id)
            parsed_product_page = html.fromstring(product_page_text)

            xpath_script_tag = '//script[@id="__NEXT_DATA__"]/text()'

            soup = BeautifulSoup(product_page_text, 'lxml')

            script_tag = soup.find('script', attrs={'id': '__NEXT_DATA__'})
            print('Has script tag')

            xpath_variant_info_link = '//script[contains(@src, "USen.js")]/@src'
            variant_info_link = ' '.join(parsed_product_page.xpath(xpath_variant_info_link))
            if '/info/' not in product_link and '/iProSvc/' in product_link and product_link:

                # If the item no. is present in script tag
                if script_tag:
                    print('Has variant info...')
                    json_data = dict()
                    try:
                        json_data = json.loads(script_tag.text)
                    except Exception as e:
                        print('ERROR_QUOTA:', e)
                        with open('error_quota.txt', 'w') as file:
                            file.write(script_tag)
                    variants_list = json_data.get('props').get('pageProps').get('allProducts')
                    if variants_list:
                        print('Number of Variants:', len(variants_list))
                        for this_variant in variants_list:
                            item_no = this_variant.get('id_us').replace('L', '')
                            print('Item No: ', item_no)
                            series_text = product_response_url.split('/')[4].split('?')[0]
                            product_variant_url = f'https://www.igus.com/product/{series_text}' + f'?artNr={item_no}'
                            print('Product Variant URL case1: ', product_variant_url)

                            # Inserting Product Variant Urls into DB Table
                            product_url_insert_query = f'''INSERT INTO `products_variants_links_page_table` (product_variant_link, product_main_link, category_page_url, category_url) VALUES ('{product_variant_url}', '{product_link}', '{product_category_page_url}', '{product_category_url}');'''
                            try:
                                self.cursor.execute(product_url_insert_query)
                            except Exception as e:
                                print(e)
                    else:
                        product_variant_url = product_link
                        print('Product Variant URL /igus.com/ case4: ', product_variant_url)
                        # Inserting Product Variant Urls into DB Table
                        product_url_insert_query = f'''INSERT INTO `products_variants_links_page_table` (product_variant_link, product_main_link, category_page_url, category_url) VALUES ('{product_variant_url}', '{product_link}', '{product_category_page_url}', '{product_category_url}');'''
                        try:
                            self.cursor.execute(product_url_insert_query)
                        except Exception as e:
                            print(e)

                # if html text deos not contains required variant information, sending request on a link which contains the variant infromation
                elif variant_info_link:
                    print('Does not have variant info...')
                    variant_info_link_ = 'https://www.igus.com/iPro/' + variant_info_link
                    print('Variant info link:', variant_info_link_)
                    variant_info_link_text = page_checker_old(url=variant_info_link_, method='GET', directory_path=os.path.join(self.project_files_dir, 'Variant_info_js_Pages'))

                    # Define the regular expression pattern to capture the values
                    pattern = r'SE\.KE\.KEVA\.ARTY\.AR\[\d+\]\.ArtNr\s*=\s*"([^"]+)"'
                    # Find all matches
                    matches = re.findall(pattern, variant_info_link_text)

                    for match in matches:
                        base_link = 'https://www.igus.com/iPro/iPro_01_0044_0001_USen.htm?ArtNr=ARTICLENO'
                        product_variant_url = base_link.replace('ARTICLENO', match)
                        print('Product Variant URL /iPro/iPro case5: ', product_variant_url)
                        # Inserting Product Variant Urls into DB Table
                        product_url_insert_query = f'''INSERT INTO `products_variants_links_page_table` (product_variant_link, product_main_link, category_page_url, category_url) VALUES ('{product_variant_url}', '{product_link}', '{product_category_page_url}', '{product_category_url}');'''
                        try:
                            self.cursor.execute(product_url_insert_query)
                        except Exception as e:
                            print(e)

                else:
                    print('404 links')
                    product_variant_url = product_link
                    print('Product Variant URL /igus.com/ case4: ', product_variant_url)
                    # Inserting Product Variant Urls into DB Table
                    product_url_insert_query = f'''INSERT INTO `products_variants_links_page_table` (product_variant_link, product_main_link, category_page_url, category_url) VALUES ('{product_variant_url}', '{product_link}', '{product_category_page_url}', '{product_category_url}');'''
                    try:
                        self.cursor.execute(product_url_insert_query)
                    except Exception as e:
                        print(e)

            elif '/info/' not in product_link and '/product/' in product_link:
                print('ArticleData in script ')
                xpath_script_content = "//script[contains(., 'MMA.Settings.LocalArticleData')]/text()"
                script_content = parsed_product_page.xpath(xpath_script_content)
                print(script_content)
                script_text = ' '.join(script_content)
                # Getting Start and End index of that variable to get its value
                start_index = script_text.find('MMA.Settings.LocalArticleData = ') + len('MMA.Settings.LocalArticleData = ')
                end_index = script_text.find('};', start_index)
                local_article_data = script_text[start_index:end_index + 1].strip()

                if local_article_data:
                    local_article_data_dict = json.loads(local_article_data)
                    article_no_list = local_article_data_dict.get('ProductList')[0].get('Data').get('Articles')
                    for article_no in article_no_list:
                        print('ARTICLE')
                        part_no = article_no.get('FilterAttributes').get('PFA_1_AN')
                        print('Part no: ', part_no)
                        article_index = product_link.find('?')
                        product_variant_url = product_link[:article_index] + f'?artNr={part_no}'
                        print('Product Variant URL case2: ', product_variant_url)

                        # Inserting Product Variant Urls into DB Table
                        product_url_insert_query = f'''INSERT INTO `products_variants_links_page_table` (product_variant_link, product_main_link, category_page_url, category_url) VALUES ('{product_variant_url}', '{product_link}', '{product_category_page_url}', '{product_category_url}');'''
                        try:
                            self.cursor.execute(product_url_insert_query)
                        except Exception as e:
                            print(e)
                else:
                    json_data = dict()
                    try:
                        json_data = json.loads(script_tag.text)
                    except Exception as e:
                        print('ERROR_QUOTA:', e)
                        with open('error_quota.txt', 'w') as file:
                            file.write(script_tag)
                    variants_list = json_data.get('props').get('pageProps').get('allProducts')
                    if variants_list:
                        print('Number of Variants:', len(variants_list))
                        for this_variant in variants_list:
                            item_no = this_variant.get('id_us').replace('L', '')
                            print('Item No: ', item_no)
                            product_variant_url = f'https://www.igus.com/product/' + item_no
                            print('Product Variant URL case1: ', product_variant_url)

                            # Inserting Product Variant Urls into DB Table
                            product_url_insert_query = f'''INSERT INTO `products_variants_links_page_table` (product_variant_link, product_main_link, category_page_url, category_url) VALUES ('{product_variant_url}', '{product_link}', '{product_category_page_url}', '{product_category_url}');'''
                            try:
                                self.cursor.execute(product_url_insert_query)
                            except Exception as e:
                                print(e)

            elif '/info/' in product_link:
                product_variant_url = product_link
                print('Product Variant URL /info/ case3: ', product_variant_url)
                # Inserting Product Variant Urls into DB Table
                product_url_insert_query = f'''INSERT INTO `products_variants_links_page_table` (product_variant_link, product_main_link, category_page_url, category_url) VALUES ('{product_variant_url}', '{product_link}', '{product_category_page_url}', '{product_category_url}');'''
                try:
                    self.cursor.execute(product_url_insert_query)
                except Exception as e:
                    print(e)

            else:
                print('Exceptional link:', product_link)
                with open('exceptional_links.txt', 'a') as file:
                    file.write(product_link + '\n')

            print('=' * 25)
            update_query = f'''UPDATE `{products_links_page_table}` SET product_page_link_status = 'Done' WHERE id = {product_index_id}'''
            self.cursor.execute(update_query)

        print('-' * 100)


Scraper().scrape()
