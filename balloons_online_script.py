
from bs4 import BeautifulSoup
import requests

from dotenv import load_dotenv
import os

load_dotenv()

login_url = 'https://balloons.online/ca/customer/account/login/'
post_url = 'https://balloons.online/ca/customer/account/loginPost/'
history_url = 'https://balloons.online/ca/sales/order/history/'

login_data = {
    'login[username]': os.getenv('EMAIL'),
    'login[password]': os.getenv('PASSWORD'),
    'persistent_remember_me': 'on',
}
       
with requests.session() as s:
    # Login page to get the token
    page = s.get(login_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    token = soup.find('input', attrs={'name':'form_key', 'type':'hidden'}).get('value')
    login_data['form_key'] = token

    # Post the login data and go to the history page
    s.post(post_url, data=login_data)
    r = s.get(history_url)
    soup = BeautifulSoup(r.content, 'html.parser')

    # Write code to Get the number of pages using class toolbar-number and getting the last two words and taking the first word of those two since it is the number of total items
    # Then divide by 10 to get the number of pages
    toolbar = soup.find('span', attrs={'class':'toolbar-number'})
    if not toolbar:
        print('No orders')
        exit()
    string = toolbar.text
    split = string.split('of')
    items = int(split[1].strip().split(' ')[0])
    
    if items % 10 == 0:
        pages = items // 10
    else:
        pages = items // 10 + 1

    products = []
    orders = []

    for page in range(1, pages+1):
      r = s.get(history_url+'?p='+str(page))
      soup = BeautifulSoup(r.content, 'html.parser')
      # Parse the order data from the history page 
      # Get table 
      table = soup.find('table', attrs={'class':'data table table-order-items history'})
      rows = table.find_all('tr')

      count = 0
      for order in rows:
          # Ignore first two rows
          if count != 1:
              count += 1
              continue
          order_id = order.find('td', attrs={'data-th':'Order #'}).text 
          order_price = order.find('span', attrs={'class':'price'}).text
          status = order.find('td', attrs={'data-th':'Status'}).text 
          link = order.find('a').get('href')
          orders.append({'order_id': order_id, 'order_price': order_price, 'status': status, 'link': link})
      
      # Get the products from each order
      for order in orders:
          link = order['link']
          r = s.get(link)
          soup = BeautifulSoup(r.content, 'html.parser')
          table = soup.find('table', attrs={'id':'my-orders-table'})
          rows = table.find_all('tbody')
          for row in rows:
              items = row.find_all('tr')
              for product in items:
                  title = product.find('td', attrs={'data-th':'Product Name'}).text.strip()
                  sku = product.find('td', attrs={'data-th':'SKU'}).text.strip()
                  unit_price = product.find('td', attrs={'data-th':'Price'}).text.strip().split(' ')[1]
                  qty = product.find('td', attrs={'data-th':'Qty'}).text.strip()
                  total_price = product.find('td', attrs={'data-th':'Subtotal'}).text.strip().split(' ')[1]
                  print(total_price)
                  products.append({'title': title, 'sku': sku, 'unit_price': unit_price, 'qty': qty, 'total_price': total_price})



    # f = open('orders.txt', 'w')
    # f.write(str(order_id))
    # f.write(str(soup))
    
