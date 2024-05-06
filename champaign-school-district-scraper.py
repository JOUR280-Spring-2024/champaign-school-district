import requests
from bs4 import BeautifulSoup
import pdfplumber
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Boolean, Float
from sqlalchemy.dialects.sqlite import insert

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 '
                  'Safari/537.36'}
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

engine = create_engine('sqlite:///school_finances.sqlite')
metadata = MetaData()
metadata.create_all(engine)

finances = Table('finances', metadata,
                 Column('cash_account', String),
                 Column('check_number', String),
                 Column('v_field', Boolean),
                 Column('issue_date', String),
                 Column('vendor_number', String),
                 Column('vendor_name', String),
                 Column('budget_code', String),
                 Column('account_number', String),
                 Column('description', String),
                 Column('amount', Float),
                 Column('sales_tax', Float),
                 Column('page_number', Integer, primary_key=True),
                 Column('row', Integer, primary_key=True),
                 Column('link', String, primary_key=True))

metadata.create_all(engine)

link_number = 0
for link in links:
    link_number = link_number + 1
    print(f"{link_number}/{len(links)}: {link}")
    file = requests.get(link)
    with open('./report.pdf', 'wb') as f:
        f.write(file.content)
    with engine.connect() as connection:
        with pdfplumber.open('./report.pdf') as pdf:
            page_number = 0
            for page in pdf.pages:
                page_number = page_number + 1
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
                            issue_dt = "20" + issue_dt[6:] + "-" + issue_dt[:2] + "-" + issue_dt[3:5]
                            amount = float(items[-1].replace(",", "").replace("−", "-"))
                            sales_tax = float(items[-2].replace(",", "").replace("−", "-"))
                            if items[2] == 'V':
                                items = items[5:-2]
                            else:
                                items = items[4:-2]

                            index_budget = 0
                            found = False
                            while index_budget < len(items) - 1 and not found:
                                item = items[index_budget]
                                if (12 <= len(item) <= 16 and item[0] in '0123456789' and item[1] in '0123456789' and
                                        '−' not in item and '/' not in item):
                                    found = True
                                if not found:
                                    index_budget = index_budget + 1
                            if not found:
                                index_budget = 0
                                found = False
                                while index_budget < len(items) - 1 and not found:
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
                            account_no = items[index_budget + 1]
                            description = " ".join(items[index_budget + 2:])
                            insert_query = insert(finances).values(
                                cash_account=cash_acct,
                                check_number=check_no,
                                v_field=v,
                                issue_date=issue_dt,
                                vendor_number=vendor_no,
                                vendor_name=vendor_name,
                                budget_code=budget,
                                account_number=account_no,
                                description=description,
                                amount=amount,
                                sales_tax=sales_tax,
                                page_number=page_number,
                                link=link,
                                row=record_number
                            )
                            do_nothing_statement = insert_query.on_conflict_do_nothing(
                                index_elements=['link', 'page_number', 'row'])
                            connection.execute(do_nothing_statement)
                        except Exception as e:
                            print(f"Error in page {page_number} and row {record_number}.")
                            print("Record:")
                            print(line.split(" "))
                            raise e
        connection.commit()
