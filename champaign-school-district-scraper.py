import requests
from bs4 import BeautifulSoup
import pdfplumber
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, PrimaryKeyConstraint
from sqlalchemy import insert

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'}
session = requests.Session()
url = 'https://www.champaignschools.org/documents/departments/finance/check-register-archive/369828'
response = session.get(url, headers=headers)
soup = BeautifulSoup(response.text, 'html.parser')
links = []
for year in soup.select('.col a'):
  new_url = 'https://www.champaignschools.org' + year['href']
  new_response = session.get(new_url, headers=headers)
  new_soup = BeautifulSoup(new_response.text, 'html.parser')
  for pdf in new_soup.select('.col a'):
    links.append(pdf['href'])

for link in links:
  file = requests.get(link)
  with open('./report.pdf', 'wb') as f:
    f.write(file.content)
  pdf = pdfplumber.open('./report.pdf')
  page_number = -1
  for page in pdf.pages:
    page_number = page_number + 1
    print(f"{page_number} {link}")
    text = page.extract_text()
    lines = text.split("\n")
    record_number = 0
    for line in lines[7:]:
      if not line.startswith("TOTAL"):
        try:
          record_number = record_number + 1
          items = line.split(" ")
          cash_acct = items[0]
          check_no = items[1]
          if items[2] == 'V':
            v = True
            issue_dt = items[3]
            vendor_no = items[4]
          else:
            v = False
            issue_dt = items[2]
            vendor_no = items[3]
          amount = items[-1]
          sales_tax = items[-2]
          if items[2] == 'V':
            items = items[5:-2]
          else:
            items = items[4:-2]

          index_budget = 0
          found = False
          while index_budget < len(items)-1 and not found:
            item = items[index_budget]
            if len(item) >= 12 and len(item) <= 16:
              if item[0] in '0123456789' and item[1] in '0123456789' and '−' not in item and '/' not in item:
                found = True
            if not found:
              index_budget = index_budget + 1
          if not found:
            index_budget = 0
            found = False
            while index_budget < len(items)-1 and not found:
              item = items[index_budget]
              if len(item) == 2:
                if item[0] in '0123456789' and item[1] in '0123456789':
                  found = True
              if not found:
                index_budget = index_budget + 1

          if not found:
            raise Exception("The line is not well-formed.")

          budget = items[index_budget]
          vendor_name = " ".join(items[:index_budget])
          account_no = items[index_budget+1]
          description = " ".join(items[index_budget+2:])
          link_name = link
          page_no = page_number
          #print(f"{cash_acct} {check_no} {v} {issue_dt} {vendor_no} {amount} {sales_tax} {budget} {vendor_name} {account_no} '{description}'")
        except Exception as e:
          print(line.split(" "))
          raise e

engine = create_engine('sqlite:///drive/MyDrive/champaign_school_district.db')
metadata = MetaData()
district_table = Table('district', metadata,
                    Column('cash_acct', String),
                    Column('check_no', String),
                    Column('issue_dt', String),
                    Column('vendor_no', String),
                    Column('vendor_name', String),
                    Column('budget', String),
                    Column('account_no', String),
                    Column('description', String),
                    Column('sales_tax', String),
                    Column('amount', Integer),
                    Column('link_name', String),
                    Column('page_no', Integer),
                    Column('record_number', Integer),
                    PrimaryKeyConstraint('link_name', 'page_no', 'record_number'))
metadata.create_all(engine)

with engine.connect() as connection:
    data = {
        'cash_acct': 'cash_acct',
        'check_no': 'check_no',
        'issue_dt': 'issue_dt',
        'vendor_no': 'vendor_no',
        'vendor_name': 'vendor_name',
        'budget': 'budget',
        'account_no': 'account_no',
        'description': 'description',
        'sales_tax': 'sales_tax',
        'amount': 'amount',
        'link_name': 'link_name',
        'page_no': page_no,
        'record_number': record_number
    }
    stmt = insert(district_table).values(data)
    connection.execute(stmt)