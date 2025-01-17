categories_links_query = '''CREATE TABLE IF NOT EXISTS categories_links (
                            iD INT AUTO_INCREMENT PRIMARY KEY,
                            category_link VARCHAR(255) UNIQUE,
                            category_status VARCHAR(255) DEFAULT 'Pending'
                            );'''

products_links_query = '''CREATE TABLE IF NOT EXISTS products_links (
                            iD INT AUTO_INCREMENT PRIMARY KEY,
                            product_link VARCHAR(255) UNIQUE,
                            category_link VARCHAR(255),
                            page_no INT
                            );'''

categories_data_query = '''CREATE TABLE IF NOT EXISTS categories_data (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            category_link VARCHAR(255) UNIQUE,
                            filename VARCHAR(255) UNIQUE,
                            response_url VARCHAR(255) UNIQUE,
                            category_status VARCHAR(255) DEFAULT 'Pending'
                            );'''

main_page_table_query = '''CREATE TABLE IF NOT EXISTS main_page_table (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            main_page_link VARCHAR(255) UNIQUE,
                            filename VARCHAR(255) DEFAULT 'NO_FILE',
                            response_url VARCHAR(255) UNIQUE,
                            main_page_link_status VARCHAR(255) DEFAULT 'Pending'
                            );'''

products_browse_table_query = '''CREATE TABLE IF NOT EXISTS products_browse_page_table (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            products_browse_link VARCHAR(255) UNIQUE,
                            filename VARCHAR(255) DEFAULT 'NO_FILE',
                            response_url VARCHAR(255) UNIQUE,
                            products_browse_link_status VARCHAR(255) DEFAULT 'Pending'
                            );'''

category_page_table_query = '''CREATE TABLE IF NOT EXISTS category_page_table(
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            category_page_link VARCHAR(255) UNIQUE,
                            filename VARCHAR(255) DEFAULT 'NO_FILE',
                            response_url VARCHAR(255) UNIQUE,
                            category_url VARCHAR(255),
                            category_page_link_status VARCHAR(255) DEFAULT 'Pending'
                            );'''

products_links_page_table_query = '''CREATE TABLE IF NOT EXISTS products_links_page_table(
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            product_page_link VARCHAR(255) UNIQUE,
                            product_filename VARCHAR(255) DEFAULT 'NO_FILE',
                            response_url VARCHAR(255) UNIQUE,
                            category_page_url VARCHAR(255),
                            category_url VARCHAR(255),
                            product_page_link_status VARCHAR(255) DEFAULT 'Pending'
                            );'''

products_variants_links_page_table_query = '''CREATE TABLE IF NOT EXISTS products_variants_links_page_table(
                                                id INT AUTO_INCREMENT PRIMARY KEY,
                                                product_variant_link VARCHAR(255) UNIQUE,
                                                product_main_link VARCHAR(255),
                                                category_page_url VARCHAR(255),
                                                category_url VARCHAR(255)
                                                );'''