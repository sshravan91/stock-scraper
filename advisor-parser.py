import csv
import re
import requests
import yaml
from bs4 import BeautifulSoup
from datetime import datetime

def get_stock_prices(tickers):
    trendDetails = list()
    for count in range(len(tickers)):
      ticker=tickers[count]
      # for ticker in tickers:
      print(count+1, ticker)  
      trendDetails.append(get_stock_info(ticker))
    return trendDetails

def get_stock_info(symbol):
    
    base_url = "https://www.advisorkhoj.com/mutual-funds-research/"
    url = f"{base_url}{symbol}"

    # print(url)
    response = requests.get(url)
    # to dump html for testing purposes
    # f = open("response-dump.html", "a")
    # f.write(response.text)
    # f.close()

    if response.status_code == 200:
        # Parse the HTML content or use BeautifulSoup to extract information
        # For example, you can use BeautifulSoup to extract the stock price
        # (similar to the previous examples)

        valueDict = {}
        valueDict['Fund']=symbol
        
        cagr_mapping = {'scheme_inception_returns':'CAGR Since Inception',
                        'scheme_1yr_returns':'1 Year CAGR',
                        'scheme_3yr_returns':'3 Years CAGR',
                        'scheme_5yr_returns':'5 Years CAGR',
                        'scheme_10yr_returns':'10 Years CAGR'}

        # We extract CAGR using regex. This is a simpler brute approach.
        # We do this because currently there is a java script executed to populate table.
        # We can consider using a headless browser automation tool like Selenium, which can load the web page, execute JavaScript, and retrieve the updated content
        for key, index in cagr_mapping.items():
          value=extract_using_regex(response.text, key)
          if value:
            valueDict[index]=value

        soup = BeautifulSoup(response.text, 'html.parser')

        context_mapping = {'Category: ':'Category', 
                   'TER:':'TER', 
                   'Asset Class: ':'Asset Class', 
                   'Standard Deviation':'Standard Deviation', 
                   'Alpha':'Alpha', 
                   'Beta':'Beta',
                   'Sharpe Ratio':'Sharpe Ratio'}

        # retrieve market cap distributions
        mkt_cap_dist_types = ['Small Cap', 'Others', 'Large Cap', 'Mid Cap']
        for mkt_cap_div in soup.find_all('div', class_='flex-div'):
          div = mkt_cap_div.find_next('p', class_='font12 text-left')
          if div:
            category = div.text.strip()
            if category in mkt_cap_dist_types:
              percentage = div.find_next('div', {'style': 'width:70%'}).div['title']
              valueDict[category]=percentage

        # extract-1
        sch_over_table_keys = {'Category: ', 'Asset Class: ', 'TER:'}
        tables = soup.find_all('table', class_='sch_over_table')
        for index, table in enumerate(tables, start=1):
          rows = table.find_all('tr')
          for row in rows:
            subrow = row.find('td').text.strip()
            for key in sch_over_table_keys:
              if key in subrow:
                if key=='TER:':
                  valueDict[context_mapping[key]]=subrow.replace(key, '').strip().split(" As on ")[0]
                else:
                  valueDict[context_mapping[key]]=subrow.replace(key, '').strip()

        # extract-metrics
        adv_table_keys = {'Standard Deviation', 'Sharpe Ratio', 'Alpha', 'Beta'}
        tables = soup.find_all('table', class_='adv-table table table-striped')
        for index, table in enumerate(tables, start=1):
          rows = table.find_all('tr')
          for row in rows:
            subrow = row.find('td')
            if subrow:
              cleanedsubrow=subrow.text.strip()
              for key in adv_table_keys:
                if key in subrow:
                  valueDict[context_mapping[key]]=row.find_next('td', {'class': 'text-center'}).text.strip()

        # alternative to cagr extraction
        # ra_tab1_div = soup.find("div", {"id": "ra_tab1"})
        # print(ra_tab1_div.prettify())
        # table_within_ra_tab1 = ra_tab1_div.find('table', {'class': 'adv-table table'})

        # print(table_within_ra_tab1)

        return valueDict

    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")

# extract CAGR using regex
def extract_using_regex(input_string, key):
  match = re.search(f"{key}='([-+]?\d*\.\d+|\d+)'", input_string)
  if match:
      return match.group(1)
  else:
    print("no match")
    return None

# exporting data to file
def export_to_file(data):
  # Specify the CSV file path
  timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
  csv_file_path = f"fund-stats_{timestamp}.csv"

  # Define the order of columns
  column_order = ['Fund', 
                  'Category', 
                  'Asset Class', 
                  'TER', 
                  'CAGR Since Inception', 
                  '1 Year CAGR', 
                  '3 Years CAGR', 
                  '5 Years CAGR', 
                  '10 Years CAGR',
                  'Alpha', 
                  'Beta', 
                  'Standard Deviation',
                  'Sharpe Ratio', 
                  'Small Cap', 
                  'Mid Cap', 
                  'Large Cap', 
                  'Others']

  fundsByType={}
  for fund_data in data:
      category=fund_data.get('Category')
      fundStats=[]
      if category in fundsByType:
         fundStats=fundsByType.get(category)
      fundStats.append([fund_data.get(column, '') for column in column_order])
      fundsByType[category]=fundStats

  # Write to CSV
  with open(csv_file_path, 'w', newline='') as csv_file:
      writer = csv.writer(csv_file)
      writer.writerow(column_order)  # Writing header
      for category, fundStats in fundsByType.items():
         writer.writerow([category])
         writer.writerows(fundStats)  # Writing data rows


# extract funds from yaml file with the following format
# funds:
# - Nippon-India-Large-Cap-Fund-Growth-Plan-Growth-Option
# - Tata-Large-Cap-Fund-Direct-Plan-Growth-Option
def extract_funds_from_yaml():
  with open('fundlist.yaml', 'r') as file:
      data = yaml.safe_load(file)

  return data['funds']


if __name__ == "__main__":
  
  # extract fund names
  funds = extract_funds_from_yaml()

  # extract data for funds
  extracted_data = get_stock_prices(funds)
  # print(extracted_data)

  # export to file
  export_to_file(extracted_data)
