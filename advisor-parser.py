import requests
import csv
from bs4 import BeautifulSoup

def get_stock_prices(tickers):
    trendDetails = {}

    for ticker in tickers:
        trendDetails[ticker]=get_stock_info(ticker)
    return trendDetails

def get_stock_info(symbol):
    base_url = "https://www.advisorkhoj.com/mutual-funds-research/"
    url = f"{base_url}{symbol}"

    response = requests.get(url)

    if response.status_code == 200:
        # Parse the HTML content or use BeautifulSoup to extract information
        # For example, you can use BeautifulSoup to extract the stock price
        # (similar to the previous examples)

        soup = BeautifulSoup(response.text, 'html.parser')

        valueDict = {}
        mapping = {'Category: ':'Category', 
                   'TER:':'TER', 
                   'Asset Class: ':'Asset Class', 
                   'Standard Deviation':'Standard Deviation', 
                   'Alpha':'Alpha', 
                   'Beta':'Beta',
                   'Sharpe Ratio':'Sharpe Ratio'}

        mkt_cap_dist_types = ['Small Cap', 'Others', 'Large Cap', 'Mid Cap']

        valueDict = {}

        for mkt_cap_div in soup.find_all('div', class_='flex-div'):
          div = mkt_cap_div.find_next('p', class_='font12 text-left')
          if div:
            category = div.text.strip()
            if category in mkt_cap_dist_types:
              percentage = div.find_next('div', {'style': 'width:70%'}).div['title']
              valueDict[category]=percentage


        tables = soup.find_all('table', class_='sch_over_table')
        for index, table in enumerate(tables, start=1):
          rows = table.find_all('tr')
          for row in rows:
            subrow = row.find('td').text.strip()
            for key, value in mapping.items():
              if key in subrow:
                valueDict[value]=subrow.replace(key, '').strip()

        tables = soup.find_all('table', class_='adv-table table table-striped')
        for index, table in enumerate(tables, start=1):
          rows = table.find_all('tr')
          for row in rows:
            subrow = row.find('td').text.strip()
            for key, value in mapping.items():
              if key in subrow:
                valueDict[value]=row.find_next('td', {'class': 'text-center'}).text.strip()

        # ra_tab1_div = soup.find("div", {"id": "ra_tab1"})
        # print(ra_tab1_div.prettify())
        # table_within_ra_tab1 = ra_tab1_div.find('table', {'class': 'adv-table table'})

        # print(table_within_ra_tab1)


        return valueDict

    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")


def export_to_file(data):
  # Specify the CSV file path
  csv_file_path = '/Users/shrasrir/Downloads/output.csv'

  # Define the order of columns
  column_order = ['Fund', 'Category', 'Asset Class', 'TER', 'Alpha', 'Beta', 'Standard Deviation',
                  'Sharpe Ratio', 'Small Cap', 'Mid Cap', 'Large Cap', 'Others']

  rows = []
  for fund, fund_data in data.items():
      fund_data['Fund']=fund
      rows.append([fund_data.get(column, '') for column in column_order])

  # Write to CSV
  with open(csv_file_path, 'w', newline='') as csv_file:
      writer = csv.writer(csv_file)
      writer.writerow(column_order)  # Writing header
      writer.writerows(rows)  # Writing data rows


# Example usage:
tickers = [
           'Nippon-India-Large-Cap-Fund-Growth-Plan-Growth-Option',
           'Tata-Large-Cap-Fund-Direct-Plan-Growth-Option',
           'SBI-Blue-Chip-Fund-Regular-Plan-Growth',
           'Kotak-Bluechip-Fund-Growth',
           'ICICI-Prudential-Bluechip-Fund-Growth',
           'HDFC-Top-100-Fund-Growth-Option-Regular-Plan',
           'Canara-Robeco-Blue-Chip-Equity-Fund-Regular-Plan-Growth-Option',
           'Aditya-Birla-Sun-Life-Frontline-Equity-Fund-Growth',
           'Edelweiss-Mid-Cap-Fund-Regular-Plan-Growth-Option'
]

extracted_data = get_stock_prices(tickers)
# print(extracted_data)

export_to_file(extracted_data)