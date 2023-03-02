from bs4 import BeautifulSoup
import requests

from dotenv import load_dotenv
import os

import psycopg2
import psycopg2.extras


load_dotenv()
login_url = 'https://www.bargainballoons.ca/custarea/Login.asp'
history_url = 'https://www.bargainballoons.ca/custarea/OrderHistory.asp'

login_data = {
    'LoginID': os.getenv('BARGAIN_BALLOONS_EMAIL'),
    'Password': os.getenv('BARGAIN_BALLOONS_PASSWORD'),
    'Action': 'signin',
}

conn = psycopg2.connect(os.getenv('COCKROACH_DB_CONNECTION_STRING'))
cur = conn.cursor()

def login(s):
    s.post(login_url, data=login_data)

def getOrders(s, orders):
    r = s.get(history_url)
    orderPage = BeautifulSoup(r.content, 'html.parser')
    table = orderPage.find('table', attrs={'id':'Table1'})
    rows = table.find_all('tr')[1:]
    for row in rows:
        cols = row.find_all('td')
        order_id = cols[1].text
        order_price = cols[2].text[1:] if cols[2].text[0] == '$' else cols[2].text # remove $ if it exists
        link = cols[4].find('a')['href'] 
        status = cols[5].text

        orders.append({'order_id': order_id, 'order_price': order_price, 'status': status, 'link': link})

def removeCompletedOrders(orders):
    cur.execute("SELECT order_id FROM complete_orders")
    complete_orders = cur.fetchall()
    for order in orders:
        if order['order_id'] in complete_orders:
            orders.remove(order)

def getProducts(s, order, products):
    link = order['link']
    r = s.get(link)
    soup = BeautifulSoup(r.content, 'html.parser')
    table = soup.find('table', attrs={'cellpadding':'3'})
    rows = table.find_all('tr')
    
    for row in rows:
        cols = row.find_all('td')
        # If none of the td column have a class of styTabBackColor, then skip it (it's not a product)
        if not any([col.has_attr('class') and 'styTabBackColor' in col['class'] for col in cols]):
            continue
        # Get all cols that have a class of styTabBackColor
        cols = [col for col in cols if col.has_attr('class') and 'styTabBackColor' in col['class']]
        if len(cols) < 5:
            continue
        size = None
        if (cols[2].text.find('\"') != -1):
            title = cols[2].text[cols[2].text.find('\"')+1:]
            size = cols[2].text[:cols[2].text.find('\"')]
        else:
            title = cols[2].text
        title = title.strip()
        
        product = {
            'order_id': order['order_id'],
            'title': title,
            'sku': cols[1].text,
            'ordered': cols[3].text,
            'shipped': cols[3].text,
            'unit_price': float(cols[4].text.strip()[1:]) * 1.13, # remove $ if it exists
            'total_price': order['order_price'], 
            'size': size,
            'order': order
        }
        products.append(product)


        

    order['product_count'] = len(rows)
  
def insertProducts(products):
    cur.execute("SELECT order_id FROM complete_orders")
    complete_orders = cur.fetchall()
    complete_orders = [order[0] for order in complete_orders]
    visited = []

    
    for product in products:
        if product['order_id'] in complete_orders:
            continue
        if product['order_id'] not in visited:
            cur.execute('INSERT INTO complete_orders (order_id) VALUES (%s)', (product['order_id'],))
            visited.append(product['order_id'])
        cur.execute("SELECT title FROM inventory",)
        inventory = cur.fetchall()
        if product['title'] in inventory:
            cur.execute("SELECT quantity FROM inventory WHERE title = %s", (product['title'],))
            quantity = cur.fetchone()
            cur.execute("UPDATE inventory SET quantity = %s WHERE title = %s", (quantity[0] + int(product['shipped']), product['title']))
        else:
            cur.execute("INSERT INTO inventory (title, size, quantity, unit_price) VALUES (%s, %s, %s, %s)", (product['title'], product['size'], product['shipped'], product['unit_price']))
        conn.commit()


def main():
    orders = []
    products = []
    with requests.session() as s:
        login(s)
        getOrders(s, orders)
        removeCompletedOrders(orders)
        # orders = [{'order_id': '1443469', 'order_price': '8.99', 'status': 'Approved', 'link': 'https://www.bargainballoons.ca/custareaadmin/50Finish.asp?PackSlip=1&psCurr=CAN&OrderID=1443469&Password=JMISEPFMGDGU2WTU&shipInstr=no', 'product_count': 14}, {'order_id': '1442571', 'order_price': '231.98', 'status': 'Approved', 'link': 'https://www.bargainballoons.ca/custareaadmin/50Finish.asp?PackSlip=1&psCurr=CAN&OrderID=1442571&Password=7Q4423IRUM3MUBD4&shipInstr=no', 'product_count': 30}, {'order_id': '1411701', 'order_price': '91.27', 'status': 'Approved', 'link': 'https://www.bargainballoons.ca/custareaadmin/50Finish.asp?PackSlip=1&psCurr=CAN&OrderID=1411701&Password=0XKHO024SYP1TRT8&shipInstr=no', 'product_count': 17}, {'order_id': '1408680', 'order_price': '347.83', 'status': 'Approved', 'link': 'https://www.bargainballoons.ca/custareaadmin/50Finish.asp?PackSlip=1&psCurr=CAN&OrderID=1408680&Password=K6VG5DRNCSPBVUTF&shipInstr=no', 'product_count': 30}, {'order_id': '1366694', 'order_price': '248.63', 'status': 'Approved', 'link': 'https://www.bargainballoons.ca/custareaadmin/50Finish.asp?PackSlip=1&psCurr=CAN&OrderID=1366694&Password=SY4SXOP4ZTSM73I4&shipInstr=no', 'product_count': 30}, {'order_id': '1345841', 'order_price': '409.66', 'status': 'Approved', 'link': 'https://www.bargainballoons.ca/custareaadmin/50Finish.asp?PackSlip=1&psCurr=CAN&OrderID=1345841&Password=DKJ1LJTSUJT61E81&shipInstr=no', 'product_count': 34}, {'order_id': '1280688', 'order_price': '18.89', 'status': 'Approved', 'link': 'https://www.bargainballoons.ca/custareaadmin/50Finish.asp?PackSlip=1&psCurr=CAN&OrderID=1280688&Password=2DKR97ZSE9J83C3F&shipInstr=no', 'product_count': 15}, {'order_id': '1279077', 'order_price': '338.18', 'status': 'Approved', 'link': 'https://www.bargainballoons.ca/custareaadmin/50Finish.asp?PackSlip=1&psCurr=CAN&OrderID=1279077&Password=L52RNMT2BFDRFGES&shipInstr=no', 'product_count': 35}, {'order_id': '1229114', 'order_price': '219.07', 'status': 'Approved', 'link': 'https://www.bargainballoons.ca/custareaadmin/50Finish.asp?PackSlip=1&psCurr=CAN&OrderID=1229114&Password=63NSO9FZ0WQD3FDB&shipInstr=no', 'product_count': 60}]
        
        for order in orders:
            getProducts(s, order, products)

        # print(products)
        insertProducts(products)

    conn.close()
    return 0


if __name__ == '__main__':
  main()