import concurrent.futures
import csv
import re
import requests
import yaml
import html
import time
from selenium import webdriver
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
    url = f"{base_url}{symbol}/consolidated"
  
    print(url)
    
    # to dump html for testing purposes
    # f = open("response-dump.html", "a")
    # f.write(response.text)
    # f.close()


    # Setup Selenium with headless Chrome
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)

    # Go to the page
    driver.get(url)
    time.sleep(1)  # Wait for JS to load

    # Get page source *after* JavaScript runs
    html = driver.page_source
    driver.quit()

    fields = ['Market Cap', 'Current Price', 'Book Value', 'Face Value', 'Stock P/E', 'ROE', 'DMA 50']  
    #if html.status_code == 200:
        # Parse the HTML content or use BeautifulSoup to extract information
        # For example, you can use BeautifulSoup to extract the stock price
        # (similar to the previous examples)
    sampleFn(html)
        

def sampleFn(html):
        fields = ['Market Cap', 'Current Price', 'Book Value', 'Face Value', 'Stock P/E', 'ROE', 'DMA 50']  
        valueDict = {}

        soup = BeautifulSoup(html, 'html.parser')
        print(soup)

        # Find all <li> tags under #top-ratios
        ratios = soup.select('#top-ratios li')
        #print(ratios)

        market_cap = None

        extracted_data = {}

        for ratio in ratios:
          name_tag = ratio.find('span', class_='name')
          value_tag = ratio.find('span', class_='nowrap value')
          if name_tag and value_tag:
            name = name_tag.get_text(strip=True)
            print(name)
            if name in fields:
                number_tag = value_tag.find('span', class_='number')
                number = number_tag.get_text(strip=True) if number_tag else ''
                suffix = value_tag.get_text(strip=True).split(number, 1)[-1].strip()
                extracted_data[name] = f"{number} {suffix}".strip()

        print(extracted_data)

# extract tickers from yaml file with the following format
# tickers:
# - TCS
def extract_data_from_yaml(property):
  with open('tickers.yaml', 'r') as file:
      data = yaml.safe_load(file)

  return data[property]


if __name__ == "__main__":
    
  # extract ticker names
  tickers = extract_data_from_yaml('tickers')

  # extract data for funds
  extracted_data = get_stock_prices(tickers)
  print(extracted_data)

