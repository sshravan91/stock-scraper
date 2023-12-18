import concurrent.futures
import csv
import re
import requests
import yaml
from bs4 import BeautifulSoup
from datetime import datetime

def get_stock_prices(tickers):
    trendDetails = list()
    futures=list()

    # Number of processes in the process pool
    num_processes = 8  # You can adjust this based on your system resources

    # Using ProcessPoolExecutor for parallel execution
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_processes) as executor:
      # Submit tasks and get Future objects
      for count in range(len(tickers)):
        ticker=tickers[count]
        print(count+1, ticker)
        futures.append(executor.submit(get_stock_info, ticker))

      # Wait for all futures to complete
      concurrent.futures.wait(futures)

      # Retrieve results from completed futures
      for future in futures:
        valueDict=future.result()
        if bool(valueDict):
          trendDetails.append(valueDict)

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
                   'Launch Date:': 'Launch Date',
                   #  'Asset Class: ':'Asset Class', 
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
        sch_over_table_keys = {'Category: ',
                               #  'Asset Class: ',
                               'TER:',
                               'Launch Date:'}
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
        print("Finished parsing " + url)

        if bool(valueDict):
          valueDict['Fund']=symbol
          return valueDict
        else:
           print(f"{symbol} has no data")

    else:
        print(f"Failed for {url}")

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
  # print(data)
  # Specify the CSV file path
  timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
  csv_file_path = f"fund-stats_{timestamp}.csv"

  # Define the order of columns
  column_order = ['Fund', 
                  'Category', 
                  # 'Asset Class', 
                  'Launch Date',
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


  category_order=['Equity: Large Cap',
                  'Equity: Focused',
                  'Equity: Large and Mid Cap',
                  'Equity: Mid Cap',
                  'Equity: Small Cap',
                  'Equity: Contra',
                  'Equity: Value',
                  'Equity: Flexi Cap',
                  'Equity: Multi Cap',
                  'Hybrid: Multi Asset Allocation',
                  'Hybrid: Dynamic Asset Allocation',
                  'Hybrid: Aggressive',
                  'Hybrid: Conservative',
                  'Equity: ELSS',
                  'Hybrid: Arbitrage', 
                  'Hybrid: Equity Savings', 
                  'Equity: Sectoral-Pharma and Healthcare', 
                  'Equity: Sectoral-FMCG',
                  'Equity: Thematic-Consumption',
                  'Equity: Thematic-ESG',
                  'Equity: Thematic-Others',
                  'Equity: Sectoral-Infrastructure',
                  'Equity: Dividend Yield', 
                  'Equity: Thematic-Manufacturing', 
                  'Equity: Sectoral-Banking and Financial Services', 
                  'Equity: Thematic-PSU', 
                  'Equity: Thematic-MNC', 
                  'Equity: Thematic-International']

  fundsByType={}
  for fund_data in data:
      if 'Category' in fund_data:
        category=fund_data.get('Category')
        fundStats=[]
        if category in fundsByType:
          fundStats=fundsByType.get(category)
        fundStats.append([fund_data.get(column, '') for column in column_order])
        fundsByType[category]=fundStats
      else:
         print("No category for fund " + fund_data + " hence skipping.")

  # Write to CSV
  with open(csv_file_path, 'w', newline='') as csv_file:
      writer = csv.writer(csv_file)
      writer.writerow(column_order)  # Writing header

      for category in category_order:
        if category in fundsByType.keys():
          writer.writerow([category])
          writer.writerows(fundsByType[category])


# extract funds from yaml file with the following format
# funds:
# - Nippon-India-Large-Cap-Fund-Growth-Plan-Growth-Option
# - Tata-Large-Cap-Fund-Direct-Plan-Growth-Option
def extract_funds_from_yaml():
  with open('fundslist.yaml', 'r') as file:
      data = yaml.safe_load(file)

  return data['funds']


if __name__ == "__main__":
  
  # extract fund names
  funds = extract_funds_from_yaml()

  # extract data for funds
  extracted_data = get_stock_prices(funds)
  # print(extracted_data)

  data_sorted_by_alpha=sorted(extracted_data, key=lambda x: (print(x) or float(x['Alpha'])) if x['Alpha'] and x['Alpha'] != '-' else float('-inf'), reverse=True)

  # export to file
  export_to_file(data_sorted_by_alpha)
