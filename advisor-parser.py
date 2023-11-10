import requests
import csv
import re
from bs4 import BeautifulSoup

def get_stock_prices(tickers):
    trendDetails = {}
    for count in range(len(tickers)):
      ticker=tickers[count]
      # for ticker in tickers:
      print(count+1, ticker)  
      trendDetails[ticker]=get_stock_info(ticker)
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
  csv_file_path = 'output.csv'

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
           #large-cap
           'Nippon-India-Large-Cap-Fund-Growth-Plan-Growth-Option',
           'Tata-Large-Cap-Fund-Direct-Plan-Growth-Option',
           'SBI-Blue-Chip-Fund-Regular-Plan-Growth',
           'quant-Large-Cap-Fund-Growth-Option-Regular-Plan',
           'Mirae-Asset-Large-Cap-Fund-Growth-Plan',
           'Kotak-Bluechip-Fund-Growth',
           'ICICI-Prudential-Bluechip-Fund-Growth',
           'HDFC-Top-100-Fund-Growth-Option-Regular-Plan',
           'Canara-Robeco-Blue-Chip-Equity-Fund-Regular-Plan-Growth-Option',
           'Aditya-Birla-Sun-Life-Frontline-Equity-Fund-Growth',

           #focused-cap
           'HDFC-Focused-30-Fund-GROWTH-PLAN',
           'Franklin-India-Focused-Equity-Fund-Growth-Plan',
           'Nippon-India-Focused-Equity-Fund-Growth-Plan-Growth-Option',
           'SBI-Focused-Equity-Fund-Regular-Plan-Growth',
           'Mirae-Asset-Focused-Fund-Regular-Plan-Growth',
           'Quant-Focused-Fund-Growth-Option-Regular-Plan',
           'ICICI-Prudential-Focused-Equity-Fund-Growth',

           #large-mid-cap
           'HDFC-Large-and-Mid-Cap-Fund-Growth-Option',
           'ICICI-Prudential-Large-Mid-Cap-Fund-Growth',
           'Motilal-Oswal-Large-and-Midcap-Fund-Regular-Plan-Growth',
           'UTI-Large-Mid-Cap-Fund-Regular-Plan-Growth-Option',
           'Axis-Growth-Opportunities-Fund-Regular-Plan-Growth',
           'BANDHAN-Core-Equity-Fund-Regular-Plan-Growth',
           'Mahindra-Manulife-Large-Mid-Cap-Fund-Regular-Plan-Growth',
           'Mirae-Asset-Emerging-Bluechip-Fund-Regular-Plan-Growth-Option',
           'Tata-Large-Mid-Cap-Fund-Regular-Plan-Growth-Option',
           'SBI-Large-MIDCap-Fund-Regular-Plan-Growth',
           'Nippon-India-Vision-Fund-Growth-Plan-Growth-Option',
           'Canara-Robeco-Emerging-Equities-Regular-Plan-Growth-Option',
           'Quant-Large-Mid-Cap-Fund-Growth-Option',

           #mid-cap
           'Quant-Mid-Cap-Fund-Growth-Option-Regular-Plan',
           'Motilal-Oswal-Midcap-Fund-Regular-Plan-Growth-Option',
           'HDFC-Mid-Cap-Opportunities-Fund-Growth-Plan',
           'SBI-Magnum-Midcap-Fund-Regular-Plan-Growth',
           'Nippon-India-Growth-Fund-Growth-Plan-Growth-Option',
           'BARODA-BNP-PARIBAS-Mid-Cap-Fund-Regular-Plan-Growth-Option',
           'PGIM-India-Midcap-Opportunities-Fund-Regular-Plan-Growth-Option',
           'Axis-Midcap-Fund-Regular-Plan-Growth',
           'WhiteOak-Capital-Mid-Cap-Fund-Regular-Plan-Growth',
           'Edelweiss-Mid-Cap-Fund-Regular-Plan-Growth-Option',

           #small-cap
           'Quant-Small-Cap-Fund-Growth-Regular-Plan',
           'Nippon-India-Small-Cap-Fund-Growth-Plan-Growth-Option',
           'HSBC-Small-Cap-Fund-Regular-Growth',
           'Kotak-Small-Cap-Fund-Growth',
           'Franklin-India-Smaller-Companies-Fund-Growth',
           'Axis-Small-Cap-Fund-Regular-Plan-Growth',
           'HDFC-Small-Cap-Fund-Growth-Option',
           'ICICI-Prudential-Smallcap-Fund-Growth',
           'Canara-Robeco-Small-Cap-Fund-Regular-Plan-Growth-Option',
           'UTI-Small-Cap-Fund-Regular-Plan-Growth-Option',
           'Invesco-India-Smallcap-Fund-Regular-Plan-Growth',
           'Tata-Small-Cap-Fund-Regular-Plan-Growth',
          
           #flexi-cap
           'quant-Flexi-Cap-Fund-Growth-Option-Regular-Plan',
           'Parag-Parikh-Flexi-Cap-Fund-Regular-Plan-Growth',
           'HDFC-Flexi-Cap-Fund-Growth-Plan',
           'JM-Flexicap-Fund-Regular-Growth-option',
           'PGIM-India-Flexi-Cap-Fund-Regular-Plan-Growth-Option',
           'Kotak-Flexicap-Fund-Growth',
           'Motilal-Oswal-Flexi-Cap-Fund-Regular-Plan-Growth-Option',
           'Samco-Flexi-Cap-Fund-Regular-Plan-Growth-Option',
           'Franklin-India-Flexi-Cap-Fund-Growth',

           #multi-cap
           'Nippon-India-Multi-Cap-Fund-Growth-Plan-Growth-Option',
           #quant activ
           'Mahindra-Manulife-Multi-Cap-Fund-Regular-Plan-Growth',
           'ICICI-Prudential-Multicap-Fund-Growth',
           'Baroda-BNP-Paribas-MULTI-CAP-FUND-Regular-Plan-Growth-Option',
           'Sundaram-Multi-Cap-Fund-Formerly-Known-as-Principal-Multi-Cap-Growth-Fund-Growth-Option',

           #multi-asset
           'Nippon-India-Multi-Asset-Fund-Regular-Plan-Growth-Option',
           'Quant-Multi-Asset-Fund-GROWTH-OPTION-Regular-Plan',
           'Tata-Multi-Asset-Opportunities-Fund-Regular-Plan-Growth',
           'Nippon-India-Multi-Asset-Fund-Regular-Plan-Growth-Option',
           'HDFC-Multi-Asset-Fund-Growth-Option',

           #contra
           'SBI-Contra-Fund-Regular-Plan-Growth',
           'Kotak-India-EQ-Contra-Fund-Growth',
           'Invesco-India-Contra-Fund-Growth'
]

extracted_data = get_stock_prices(tickers)
# print(extracted_data)

export_to_file(extracted_data)
