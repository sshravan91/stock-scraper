import concurrent.futures
import csv
import re
import requests
import yaml
import html
from datetime import datetime
from bs4 import BeautifulSoup

def get_stock_prices(tickers):
    trendDetails = list()
    futures=list()
    print(tickers)
      
    # Number of processes in the process pool
    num_processes = 100  # You can adjust this based on your system resources
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
     
    base_url = "https://www.screener.in/company/"
    url = f"{base_url}{symbol}/#peers"
  
    print(url)
    response = requests.get(url)
    # to dump html for testing purposes
    # f = open("response-dump.html", "a")
    # f.write(response.text)
    # f.close()

    fields = ['Market Cap', 'Current Price', 'Book Value', 'Face Value', 'Stock P/E', 'ROE', 'DMA 50', '3 Years', '5 Years', '10 Years', 'TTM']  
    if response.status_code == 200:
        # Parse the HTML content or use BeautifulSoup to extract information
        # For example, you can use BeautifulSoup to extract the stock price
        # (similar to the previous examples)

        valueDict = {}

        soup = BeautifulSoup(response.text, 'html.parser')
        print(soup)

        # Find all <li> tags under #top-ratios
        ratios = soup.select('#top-ratios li')
        #print(ratios)

        market_cap = None

        extracted_data = {}
        extracted_data['ticker']=symbol

        for ratio in ratios:
          name_tag = ratio.find('span', class_='name')
          value_tag = ratio.find('span', class_='nowrap value')
          if name_tag and value_tag:
            name = name_tag.get_text(strip=True)
            if name in fields:
                number_tag = value_tag.find('span', class_='number')
                number = number_tag.get_text(strip=True) if number_tag else ''
                suffix = value_tag.get_text(strip=True).split(number, 1)[-1].strip()
                extracted_data[name] = f"{number} {suffix}".strip()

        for table in soup.select('table.ranges-table'):
            header = table.find('th').text.strip()  # e.g. "Compounded Sales Growth, Stock Price CAGR ..."
            if header == 'Stock Price CAGR':

              for row in table.find_all('tr')[1:]:  # Skip the header row
                cols = row.find_all('td')
                if len(cols) == 2:
                    label = cols[0].text.strip().replace(':', '')  # e.g. "5 Years"
                    value = cols[1].text.strip()  # e.g. "10%"
                    if label in fields:
                        extracted_data[label] = value     

        if bool(extracted_data):
          return extracted_data
        else:
           print(f"\033[91m{symbol} has no data\033[0m")


# extract tickers from yaml file with the following format
# tickers:
# - TCS
def extract_data_from_yaml(property):
  with open('tickers.yaml', 'r') as file:
      data = yaml.safe_load(file)

  return data[property]

def export_to_file(data):
  # print(data)
  # Specify the CSV file path
  timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
  csv_file_path = f"stock-stats_{timestamp}.csv"

  # Define the order of columns
  column_order = ['Ticker',
                  'Market Cap', 
                  'Current Price', 
                  'Book Value', 
                  'Face Value', 
                  'Stock P/E', 
                  'ROE']

  formatted_data = []
  for entry in data:
    formatted_entry = {
        'Ticker': entry.get('ticker'),
        'Market Cap': entry.get('Market Cap'),
        'Current Price': entry.get('Current Price'),
        'Book Value': entry.get('Book Value'),
        'Face Value': entry.get('Face Value'),
        'Stock P/E': entry.get('Stock P/E'),
        'ROE': entry.get('ROE'),
        '3 Years': entry.get('3 Years'),
        '5 Years': entry.get('5 Years'),
        '10 Years': entry.get('10 Years'),
        'TTM': entry.get('TTM')
    }
    formatted_data.append(formatted_entry)

  # Write to CSV with column order
  with open(csv_file_path, 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=column_order)
    writer.writeheader()
    writer.writerows(formatted_data)

if __name__ == "__main__":
    
  # extract ticker names
  tickers = extract_data_from_yaml('tickers')

  # extract data for funds
  extracted_data = get_stock_prices(tickers)
  print(extracted_data)
  #export_to_file(extracted_data)
  print(extracted_data)
