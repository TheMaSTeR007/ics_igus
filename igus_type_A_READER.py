import math
import time

import pymysql
import requests, os, gzip, hashlib, json
from lxml import html
from sql_queries import products_links_query, categories_links_query, category_page_table_query, products_links_page_table_query


def req_sender(url: str, method: str, query_dict: dict = None, cookies: dict = None, headers: dict = None, params_dict: dict = None) -> bytes or None:
    # Send HTTP request
    print('sending request...')
    _response = requests.request(method=method, url=url, data=query_dict, cookies=cookies, headers=headers, allow_redirects=True, params=params_dict)
    # Sending request again after 5 seconds delay if it gets HTTP Status code like 500
    if _response.status_code >= 500:
        print('HTTP Status code:', _response.status_code, 'Sending Request again after 5 Seconds')
        time.sleep(5)
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
        _response = req_sender(url=url, method=method, query_dict=query_dict, cookies=cookies, headers=headers, params_dict=params_dict)  # Send the GET request
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
        try:
            self.cursor.execute(query=category_page_table_query)
        except Exception as e:
            print(e)
        try:
            self.cursor.execute(query=products_links_page_table_query)
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
        else:  # If the file does not exist
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
        category_page_table_name = 'category_page_table'
        # Fetching Categories Page links whose status is "Pending" for faster execution
        category_page_fetch_query = f'''SELECT * FROM `{category_page_table_name}` WHERE category_page_link_status = 'Pending';'''
        self.cursor.execute(category_page_fetch_query)
        categories_page_data = self.cursor.fetchall()

        # Iterating on each category links for getting their product's link
        for this_category_page_data in categories_page_data:
            category_page_index_id_ = this_category_page_data[0]  # index of category url fetched from Database Table
            print('Category Index id: ', category_page_index_id_)
            category_page_filename_hash_ = this_category_page_data[2]  # Hash of url by which name file is saved fetched from Database Table
            category_page_link_raw = this_category_page_data[1]
            category_page_link = category_page_link_raw
            print('Category Page Url: ', category_page_link)
            category_page_request_url = this_category_page_data[1]  # category url fetched from Database Table used for Requesting
            print('Category Page Request Url: ', category_page_request_url)
            category_page_response_url = this_category_page_data[3]  # Final Url of Category received in Response
            print('Category Page Response Url: ', category_page_response_url)
            category_main_response_url = this_category_page_data[4]
            print('Working on Category: ', category_main_response_url, ' ...')
            print('Working on Category Page: ', category_page_link, ' ...')

            # Checking if Category page exists, else sending request and saving it
            category_page_text = self.page_checker(url=category_page_link, method='GET', directory_path=os.path.join(self.project_files_dir, 'Categories_Pages_Paginated'), page_hash=category_page_filename_hash_, table_name=category_page_table_name, _id_=category_page_index_id_)
            parsed_category_page = html.fromstring(category_page_text)

            # If Products links are available in HTML, xpath for getting product links from href will be applied
            xpath_products_link = '//div[@class="btn__text" and text() = "Shop now"]/../@href'
            products_links_list = parsed_category_page.xpath(xpath_products_link)

            # Else if Products links are available in SCRIPT tag, xpath for getting links from script tag will be applied
            # xpath_script_next_data = '//script[@id="__NEXT_DATA__"]/text()'
            # script_next_data = ' '.join(parsed_category_page.xpath(xpath_script_next_data))

            if products_links_list:
                print('No of Products:', len(products_links_list) if products_links_list else 0)
                print('PRODUCTS LINKS IN HTML PAGE')
                for product_link_ in products_links_list:
                    # Concatenating main url with relative product link
                    product_link = 'https://www.igus.com' + product_link_
                    print('Product Link:', product_link)

                    products_links_page_table = 'products_links_page_table'
                    # INSERTING PRODUCTS LINKS IN DB TABLE
                    product_link_insert_query = f'''INSERT INTO {products_links_page_table}
                                                    (product_page_link, category_page_url, category_url)
                                                    VALUES ('{product_link}', '{category_page_response_url}', '{category_main_response_url}');'''
                    try:
                        self.cursor.execute(product_link_insert_query)
                    except Exception as error:
                        print('Products_links_page_table ERROR:', error)
                    print('='*25)

            # Product's link slug available in json, which is stored in saved category page text
            else:
                print('SCRIPT NEXT DATA...')
                script_next_data = json.loads(category_page_text)
                print('Redirected Section...')
                # products_count = script_next_data.get('pageProps').get('dehydratedState').get('queries')[1].get('state').get('data').get('total')
                # print('Total Products in redirected Category: ', products_count)
                # category_code = script_next_data.get('buildId')
                # print('Category Code:', category_code)
                products_links_list = script_next_data.get('pageProps').get('dehydratedState').get('queries')[1].get('state').get('data').get('products')
                for product_data in products_links_list:
                    product_slug = product_data.get('slug')
                    product_link = 'https://www.igus.com/product/' + product_slug
                    print('Product Link:', product_link)

                    products_links_page_table = 'products_links_page_table'
                    # INSERTING PRODUCTS LINKS IN DB TABLE
                    product_link_insert_query = f'''INSERT INTO {products_links_page_table}
                                                    (product_page_link, category_page_url, category_url)
                                                    VALUES ('{product_link}', '{category_page_response_url}', '{category_main_response_url}');'''
                    try:
                        self.cursor.execute(product_link_insert_query)
                    except Exception as error:
                        print('Products_links_page_table ERROR:', error)
                print('='*25)

            print('+' * 50)

                    # INSERTING CATEGORY LINKS WITH PAGE NO INTO TABLE
                    # category_page_table_name = 'category_page_table'
                    #
                    # category_page_insert_query = f'''INSERT INTO {category_page_table_name} (category_page_link, category_url) VALUES (%s, %s);'''
                    # try:
                    #     self.cursor.execute(category_page_insert_query, args=(category_link_page_no, category_link_raw))
                    # except Exception as error:
                    #     print('Category Page Link Error: ', error)
                    # category_page = page_checker_json_new(url=category_link, method='GET', directory_path=os.path.join(self.project_files_dir, 'Products_Pages_Json'))
                    #
                    # category_page = json.loads(category_page) if isinstance(category_page, str) else category_page
                    # print(category_page.get('pageProps').get('dehydratedState').get('queries')[1].get('state').get('data').get('products'))
                    # # pageProps.dehydratedState.queries[1].state.data.products
                    # print('PRODUCTS LINKS IN JSON DATA')



            # checking if product url is available without request and also if the url is being redirected or not from /iProSvc/ to final product url
            #     if '/info/' not in product_link and '/iProSvc/' in product_link and product_link:
            #         print('Product link -iProSvc- : ', product_link)
            #         product_page = self.page_checker(url=product_link, method='GET', directory_path=os.path.join(self.project_files_dir, 'Product_Variant_Pages'))
            # parsed_product_page = html.fromstring(product_page)
            # # product_id = product_link[-14:-10]
            # xpath_script_tag = '//script[@id="__NEXT_DATA__"]/text()'
            # script_tag = ' '.join(parsed_product_page.xpath(xpath_script_tag))
            # try:
            #     json_data = json.loads(script_tag)
            #     variants_list = json_data.get('props').get('pageProps').get('allProducts')
            #     for this_variant in variants_list:
            #         item_no = this_variant.get('id_us')
            #         print('Item No: ', item_no)
            #         if 48 <= ord(item_no[0]) <= 57 and 48 <= ord(item_no[1]) <= 57 and 48 <= ord(item_no[2]) <= 57 and 48 <= ord(item_no[3]) <= 57:
            #             series_text = item_no[0:4]
            #         elif 48 <= ord(item_no[0]) <= 57 and 65 <= ord(item_no[1]) <= 90 and ord(item_no[2]) == 45 and 48 <= ord(item_no[3]) <= 57 and 48 <= ord(item_no[44]) <= 57:
            #             series_text = item_no[0:5]
            #         elif 48 <= ord(item_no[0]) <= 57 and 48 <= ord(item_no[1]) <= 57:
            #             series_text = item_no[0:2]
            #
            #         product_variant_url = f'https://www.igus.com/product/series-{series_text}' + f'?artnr={item_no}'
            #         print('Product Variant URL: ', product_variant_url)
            #
            #         # Inserting Product Variant Urls into DB Table
            #         product_url_insert_query = f'''INSERT INTO `products_links` (product_link, category_link, page_no) VALUES ('{product_variant_url}', '{category_link}', {page_no});'''
            #         try:
            #             self.cursor.execute(product_url_insert_query)
            #         except Exception as e:
            #             print(e)
            # except Exception as e:
            #     print('??ERROR?? Product link -iProSvc- : ', product_link)
            #     ignored_list += [product_link]
            #     with open('ignored_links.txt', 'a+') as file:
            #         file.write(product_link + ' -=- ')
            #     print(e)

            # checking if product url is available without request and also if url is not being redirected
            # elif '/info/' not in product_link and '/product/' in product_link:
            #     print('Product  link -product- : ', product_link)
            #     product_page = self.page_checker(url=product_link, method='GET', directory_path=os.path.join(self.project_files_dir, 'Product_Variant_Pages'))
            #     parsed_product_page = html.fromstring(product_page)

            # xpath_script_content = "//script[contains(., 'MMA.Settings.LocalArticleData')]/text()"
            # script_content = parsed_product_page.xpath(xpath_script_content)
            #
            # if script_content:
            #     script_text = ' '.join(script_content)
            #     # Getting Start and End index of that variable to get its value
            #     start_index = script_text.find('MMA.Settings.LocalArticleData = ') + len('MMA.Settings.LocalArticleData = ')
            #     end_index = script_text.find('};', start_index)
            #     local_article_data = script_text[start_index:end_index + 1].strip()
            #     local_article_data_dict = json.loads(local_article_data)
            #     article_no_list = local_article_data_dict.get('ProductList')[0].get('Data').get('Articles')
            #     for article_no in article_no_list:
            #         print('ARTICLE')
            #         part_no = article_no.get('FilterAttributes').get('PFA_1_AN')
            #         print('Part no: ', part_no)
            #         article_index = product_link.find('?')
            #         product_variant_url = product_link[:article_index] + f'?artNr={part_no}'
            #         print('Product Variant URL: ', product_variant_url)
            #
            #         # Inserting Product Variant Urls into DB Table
            #         product_url_insert_query = f'''INSERT INTO `products_links` (product_link, category_link, page_no) VALUES ('{product_variant_url}', '{category_link}', {page_no});'''
            #         try:
            #             self.cursor.execute(product_url_insert_query)
            #         except Exception as e:
            #             print(e)
            # print('=' * 25)

                        # print('Ignored List: x', ignored_list)
                    # Resetting the category link as its concatenating irrelevant '/products/' string
                    # category_link = category_link_raw
                    # print('+' * 50)

            # As pre-assembled-cable-carriers does not have products on its page, ignoring it manually
            # elif script_next_data:
            #     print('REAMAINING', category_response_url)
            #     print('Redirected Section...')
            #     script_next_data = json.loads(script_next_data)
            #     products_count = script_next_data.get('props').get('pageProps').get('dehydratedState').get('queries')[1].get('state').get('data').get('total')
            #     print('Total Products in redirected Category: ', products_count)
            #     category_code = script_next_data.get('buildId')
            #     print('Category Code: ', category_code)
            #
            #     products_links_list = script_next_data.get('props').get('pageProps').get('dehydratedState').get('queries')[1].get('state').get('data').get('products')
            #
            #     page_count = math.ceil(products_count / len(products_links_list))
            #     print('Pages Count: ', page_count)
            #     for page_no in range(1, page_count + 1):
            #         print('On Page: ', page_no, ' of Category: ', category_response_url)
            #
            #         # Joining /category_code/ and page_no to retrieve products links on current category page
            #         category_link_page_no = f'https://www.igus.com/_next/data/{category_code}/en-US/{'/'.join(category_response_url.split('/')[-2:])}/{page_no}.json'
            #         print('_+_EXCEPTOR_+_')
            #         with open('exceptor.txt', 'a+') as file:
            #             file.write(category_link_page_no + ' -=- ')
            #         print(category_link_page_no)
            #
            #         # INSERTING CATEGORY LINKS WITH PAGE NO INTO TABLE
            #         category_page_table_name = 'category_page_table'
            #
            #         category_page_insert_query = f'''INSERT INTO {category_page_table_name} (category_page_link, category_url) VALUES (%s, %s);'''
            #         try:
            #             self.cursor.execute(category_page_insert_query, args=(category_link_page_no, category_link_raw))
            #         except Exception as error:
            #             print('Category Page Link Error: ', error)
            #         # category_page = page_checker_json_new(url=category_link, method='GET', directory_path=os.path.join(self.project_files_dir, 'Products_Pages_Json'))
            #         #
            #         # category_page = json.loads(category_page) if isinstance(category_page, str) else category_page
            #         # print(category_page.get('pageProps').get('dehydratedState').get('queries')[1].get('state').get('data').get('products'))
            #         # # pageProps.dehydratedState.queries[1].state.data.products
            #         # print('PRODUCTS LINKS IN JSON DATA')
            #
            #         # Resetting the category link as its concatenating irrelevant '/products/' string
            #         category_link = self.main_page_url[:-1] + category_link_raw

            # update_query = f'''UPDATE `categories_data` SET category_status = 'Done' WHERE id = {category_index_id_}'''
            # self.cursor.execute(update_query)

Scraper().scrape()
