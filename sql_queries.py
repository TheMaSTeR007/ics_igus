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
