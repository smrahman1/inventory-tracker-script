
from bs4 import BeautifulSoup
import requests

from dotenv import load_dotenv
import os

import psycopg2
import psycopg2.extras


load_dotenv()
login_url = 'https://balloons.online/ca/customer/account/login/'
post_url = 'https://balloons.online/ca/customer/account/loginPost/'
history_url = 'https://balloons.online/ca/sales/order/history/'

login_data = {
    'login[username]': os.getenv('BALLOONS_ONLINE_EMAIL'),
    'login[password]': os.getenv('BALLOONS_ONLINE_PASSWORD'),
    'persistent_remember_me': 'on',
}

conn = psycopg2.connect(os.getenv('COCKROACH_DB_CONNECTION_STRING'))
cur = conn.cursor()

def getToken(s):
    page = s.get(login_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    token = soup.find('input', attrs={'name':'form_key', 'type':'hidden'}).get('value')
    return token

def login(s):
    s.post(post_url, data=login_data)
    r = s.get(history_url)
    return BeautifulSoup(r.content, 'html.parser')
    
def getPages(historyPage):
    pages = 0
    toolbar = historyPage.find('span', attrs={'class':'toolbar-number'})
    if not toolbar:
        return pages
    string = toolbar.text
    split = string.split('of')
    items = int(split[1].strip().split(' ')[0])
    
    if items % 10 == 0:
        pages = items // 10
    else:
        pages = items // 10 + 1

    return pages

def getOrdersPerPage(s, page, orders):
    r = s.get(history_url+'?p='+str(page))
    orderPage = BeautifulSoup(r.content, 'html.parser')
    table = orderPage.find('table', attrs={'class':'data table table-order-items history'})
    rows = table.find_all('tr')

    parseOrders(rows, orders)

def parseOrders(rows, orders):
    count = 0
    for order in rows:
        # Ignore first two rows
        if count != 1:
            count += 1
            continue
        order_id = str(order.find('td', attrs={'data-th':'Order #'}).text)
        order_price = order.find('span', attrs={'class':'price'}).text
        status = order.find('td', attrs={'data-th':'Status'}).text 
        link = order.find('a').get('href')
        orders.append({'order_id': order_id, 'order_price': order_price[1:], 'status': status, 'link': link})

def getProducts(s, order, products):
    link = order['link']
    r = s.get(link)
    soup = BeautifulSoup(r.content, 'html.parser')
    table = soup.find('table', attrs={'id':'my-orders-table'})
    rows = table.find_all('tbody')
    order['product_count'] = len(rows)
    parseProducts(rows, products, order)

def parseProducts(rows, products, order):
    for row in rows:
        items = row.find_all('tr')
        for product in items:
            title = product.find('td', attrs={'data-th':'Product Name'}).text.strip()
            if len(title) >= 3 and title[2] == '\"':
                size = int(title[0:2])
                title = title[3:].strip()
            elif title.find('\"') != -1:
                pos = title.find('\"')
                size = int(title[pos-2:pos])
                title = title[:pos-2].strip() + title[pos+1:].strip()
            else:
                size = None
            sku = product.find('td', attrs={'data-th':'SKU'}).text.strip()
            unit_price = product.find('td', attrs={'data-th':'Price'}).text.strip().split(' ')[1]
            unit_price = unit_price[1:] if unit_price[0] == '$' else unit_price
            unit_price = float(unit_price) * 1.13
            qty = product.find('td', attrs={'data-th':'Qty'}).text.strip()
            split = qty.split(' ')
            ordered = split[1]
            shipped = split[3]
            total_price = product.find('td', attrs={'data-th':'Subtotal'}).text.strip().split(' ')[1]
            products.append({'order_id': order['order_id'], 'title': title, 'size': size, 'sku': sku, 'unit_price': unit_price, 'ordered': ordered, 'shipped':shipped, 'total_price': total_price[1:], 'order': order})

def removeCompletedOrders(orders):
    cur.execute("SELECT order_id FROM complete_orders")
    complete_orders = cur.fetchall()
    for order in orders:
        if order['order_id'] in complete_orders:
            orders.remove(order)
  
def insertProducts(products):
    cur.execute("SELECT sku FROM incomplete_orders")
    incomplete_orders = cur.fetchall()
    cur.execute("SELECT order_id FROM complete_orders")
    complete_orders = cur.fetchall()
    complete_orders = [order[0] for order in complete_orders]
    cur.execute("SELECT order_id FROM product_count_per_order")
    product_count_per_order = cur.fetchall()
    product_count_per_order = [order[0] for order in product_count_per_order]

    for product in products:
        if product['sku'] in incomplete_orders:
            if int(product['shipped']) == int(product['ordered']):
                cur.execute("DELETE FROM incomplete_orders WHERE sku = %s", (product['sku'],))
                if product['order_id'] not in product_count_per_order:
                    cur.execute("INSERT INTO product_count_per_order (order_id, completed) VALUES (%s, %s)", (product['order_id'], 0))
                cur.execute("SELECT completed FROM product_count_per_order WHERE order_id = %s", (product['order_id'],))
                completed = cur.fetchone()[0]
                if completed + 1 == product['order']['product_count']:
                    cur.execute("INSERT INTO complete_orders (order_id) VALUES (%s)", (product['order_id'],))
                    cur.execute("DELETE FROM product_count_per_order WHERE order_id = %s", (product['order_id'],))
                else:
                  cur.execute("UPDATE product_count_per_order SET completed = %s WHERE order_id = %s", (completed + 1, product['order_id'],))
            else:
              cur.execute("UPDATE incomplete_orders SET shipped = %s WHERE sku = %s", (product['shipped'], product['sku']))
        elif product['order_id'] not in complete_orders:
            if int(product['shipped']) == int(product['ordered']):
                if product['order_id'] not in product_count_per_order:
                    cur.execute("INSERT INTO product_count_per_order (order_id, completed) VALUES (%s, %s)", (product['order_id'], 0))
                cur.execute("SELECT completed FROM product_count_per_order WHERE order_id = %s", (product['order_id'],))
                completed = cur.fetchone()[0]
                if completed + 1 == product['order']['product_count']:
                    cur.execute("INSERT INTO complete_orders (order_id) VALUES (%s)", (product['order_id'],))
                    cur.execute("DELETE FROM product_count_per_order WHERE order_id = %s", (product['order_id'],))
                else:
                  cur.execute("UPDATE product_count_per_order SET completed = %s WHERE order_id = %s", (completed + 1, product['order_id'],))
            else:
              cur.execute("INSERT INTO incomplete_orders (order_id, title, size, sku, unit_price, ordered, shipped, total_price) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (product['order_id'], product['title'], product['size'], product['sku'], product['unit_price'], product['ordered'], product['shipped'], product['total_price']))
        else:
            continue
        cur.execute("SELECT title FROM inventory",)
        inventory = cur.fetchall()
        if product['title'] in inventory:
            cur.execute("SELECT quantity FROM inventory WHERE title = %s", (product['title'],))
            quantity = cur.fetchone()
            cur.execute("SELECT shipped FROM incomplete_orders WHERE title = %s", (product['title'],))
            shipped = cur.fetchall()
            diff = int(product['shipped']) - shipped
            cur.execute("UPDATE inventory SET quantity = %s WHERE title = %s", (quantity[0] + diff, product['title']))
        else:
            cur.execute("INSERT INTO inventory (title, size, quantity, unit_price) VALUES (%s, %s, %s, %s)", (product['title'], product['size'], product['shipped'], product['unit_price']))
        conn.commit()

def main():
    with requests.session() as s:
        
        login_data['form_key'] = getToken(s)

        historyPage = login(s)

        pages = getPages(historyPage)
        
        if not pages:
            print('No orders')
            return 1

        products = []
        orders = []

        for page in range(1, pages+1):
            getOrdersPerPage(s, page, orders)
            
        removeCompletedOrders(orders)
            

        for order in orders:
            getProducts(s, order, products)
    insertProducts(products)

    conn.close()
    return 0

if __name__ == '__main__':
  main()