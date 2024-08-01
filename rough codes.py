# # If data is available from html page, applying xpath to retrieve it
# xpath_products_count = '//p[contains(text(), "Number of products")]/text() | //div[contains(text(), "Number of products")]//text()'
# products_count = ' '.join(parsed_category_page.xpath(xpath_products_count))
# # Removing irrelevant characters from string, getting integer page number
# products_count = products_count.replace('Number of products', '').replace(':', '')
# print('PRODUCTS COUNT :', products_count)
#
# # If data is not available from html page retrieving it from script tag to retrieve product's count to retrieve data
# xpath_script_next_data = '//script[@id="__NEXT_DATA__"]/text()'
# script_next_data = ' '.join(parsed_category_page.xpath(xpath_script_next_data))
# print('SCRIPT DATA:', script_next_data)
#
# Retrieving Category id for sending request on
# script_text_catID = ' '.join(parsed_category_page.xpath('//script[contains(text(), "_filter.ServiceUrl")]/text()'))
# start_index_catId = script_text_catID.find('_filter.ServiceUrl = ') + len('_filter.ServiceUrl = ')
# end_index_catId = script_text_catID.find('";', start_index_catId)
# category_id = script_text_catID[start_index_catId:end_index_catId + 1].strip()[-7:-1]
# print('CATEGORY ID: ', category_id)

# # If a category page contains Category id in its html text, its also having product's count on its 1st page
# if category_id:
#     print('Category Id: ', category_id)
#     products_count = int(products_count)
#     print('Total Products in current Category: ', products_count)
#     xpath_products_links = '//div[contains(text(), "Shop now")]/../@href'
#     products_links_list = parsed_category_page.xpath(xpath_products_links)
#     print('Products on this Page: ', len(products_links_list))
#     pages_count = math.ceil(products_count / len(products_links_list))
#     print('Total Pages Count: ', pages_count)
#     for page_no in range(1, pages_count + 1):
#         print('On Page: ', page_no)
#         # Joining /products/ and category id of category to
#         category_link = '/'.join(category_link.split('/')[:-1]) + '/products/' + category_id
#         category_link_page_no = category_link + f'?sort=3&inch=false&page={page_no}'
#         print('new category link with page no: ', category_link_page_no)
#
#         # INSERTING CATEGORY LINKS WITH PAGE NO INTO TABLE
#         category_page_table_name = 'category_page_table'
#
#         category_page_insert_query = f'''INSERT INTO {category_page_table_name} (category_page_link, category_url) VALUES (%s, %s);'''
#         try:
#             self.cursor.execute(category_page_insert_query, args=(category_link_page_no, category_link_raw))
#         except Exception as error:
#             print('Category Page Link Error: ', error)


