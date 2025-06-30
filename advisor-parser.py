import concurrent.futures
import csv
import requests
import yaml
from bs4 import BeautifulSoup
from datetime import datetime
from collections import defaultdict
import threading
import re

funds_with_no_data = []

def get_stock_prices(tickers):
    trendDetails = list()
    futures = list()
    symbols_no_data = []

    # Number of processes in the process pool
    num_processes = 100  # You can adjust this based on your system resources

    # Use ThreadPoolExecutor for I/O-bound web requests
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(16, len(tickers))) as executor:
        for count, ticker in enumerate(tickers):
            print(count + 1, ticker)
            futures.append(executor.submit(get_stock_info, ticker))

        for future in concurrent.futures.as_completed(futures):
            has_data, value = future.result()
            if has_data:
                trendDetails.append(value)
            else:
                symbols_no_data.append(value)

    # Update the global funds_with_no_data
    global funds_with_no_data
    funds_with_no_data = symbols_no_data

    return trendDetails

def get_stock_info(symbol):
    base_url = "https://www.advisorkhoj.com/mutual-funds-research/"
    url = f"{base_url}{symbol}"

    response = requests.get(url)

    if response.status_code == 200:
        valueDict = {}

        cagr_mapping = {'scheme_inception_returns': 'CAGR Since Inception',
                        'scheme_1yr_returns': '1 Year CAGR',
                        'scheme_3yr_returns': '3 Years CAGR',
                        'scheme_5yr_returns': '5 Years CAGR',
                        'scheme_10yr_returns': '10 Years CAGR',
                        'category_1yr_returns': '1 Year Baseline CAGR',
                        'category_3yr_returns': '3 Years Baseline CAGR',
                        'category_5yr_returns': '5 Years Baseline CAGR',
                        'category_10yr_returns': '10 Years Baseline CAGR',
                        'scheme_nav': 'NAV',
                        'benchmark': 'Benchmark Type'}

        for key, index in cagr_mapping.items():
            value = extract_using_regex(response.text, key)
            if value:
                valueDict[index] = value

        soup = BeautifulSoup(response.text, 'html.parser')

        context_mapping = {'Category: ': 'Category',
                           'TER:': 'TER',
                           'Total Assets:': 'Total Assets (in Cr)',
                           'Launch Date:': 'Launch Date',
                           'Turn over:': 'Turn over (%)',
                           'Standard Deviation': 'Standard Deviation',
                           'Alpha': 'Alpha',
                           'Beta': 'Beta',
                           'Sharpe Ratio': 'Sharpe Ratio'}

        # retrieve market cap distributions
        mkt_cap_dist_types = ['Small Cap', 'Others', 'Large Cap', 'Mid Cap']
        for mkt_cap_div in soup.find_all('div', class_='flex-div'):
            div = mkt_cap_div.find_next('p', class_='font12 text-left')
            if div:
                category = div.text.strip()
                if category in mkt_cap_dist_types:
                    percentage = div.find_next('div', {'style': 'width:70%'}).div['title']
                    valueDict[category] = percentage

        sch_over_table_keys = {'Category: ',
                               'TER:',
                               'Turn over:',
                               'Total Assets:',
                               'Launch Date:'}
        tables = soup.find_all('table', class_='sch_over_table')
        for index, table in enumerate(tables, start=1):
            rows = table.find_all('tr')
            for row in rows:
                subrow = row.find('td').text.strip()
                for key in sch_over_table_keys:
                    if key in subrow:
                        if key == 'TER:':
                            valueDict[context_mapping[key]] = subrow.replace(key, '').strip().split(" As on ")[0]
                        elif key == 'Total Assets:':
                            valueDict[context_mapping[key]] = subrow.replace(key, '').strip().split(" Cr As on ")[0]
                        elif key == 'Turn over:':
                            result = subrow.replace(key, '').strip().split("|")[0]
                            valueDict[context_mapping[key]] = result.strip() if result else result
                        else:
                            valueDict[context_mapping[key]] = subrow.replace(key, '').strip()

        adv_table_keys = {'Standard Deviation', 'Sharpe Ratio', 'Alpha', 'Beta'}
        tables = soup.find_all('table', class_='adv-table table table-striped')
        for index, table in enumerate(tables, start=1):
            rows = table.find_all('tr')
            for row in rows:
                subrow = row.find('td')
                if subrow:
                    cleanedsubrow = subrow.text.strip()
                    for key in adv_table_keys:
                        if key in subrow:
                            valueDict[context_mapping[key]] = row.find_next('td', {'class': 'text-center'}).text.strip()

        print("Finished parsing " + url)

        if bool(valueDict):
            valueDict['Fund'] = symbol
            return (True, valueDict)
        else:
            print(f"\033[91m{symbol} has no data\033[0m")
            return (False, symbol)

    else:
        print(f"\033Failed for {url}\033[0m")
        return (False, symbol)

def extract_using_regex(input_string, key):
    match = re.search(f"{key}='(.*?)'", input_string)
    if match:
        return match.group(1)
    else:
        return None

def export_to_file(data):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    csv_file_path = f"fund-stats_{timestamp}.csv"

    column_order = ['Fund',
                   'Category',
                   'Launch Date',
                   'Total Assets (in Cr)',
                   'TER',
                   'Turn over (%)',
                   'CAGR Since Inception',
                   '1 Year CAGR',
                   '1 Year Baseline CAGR',
                   '3 Years CAGR',
                   '3 Years Baseline CAGR',
                   '5 Years CAGR',
                   '5 Years Baseline CAGR',
                   '10 Years CAGR',
                   '10 Years Baseline CAGR',
                   'Benchmark Type',
                   'NAV',
                   'Alpha',
                   'Beta',
                   'Standard Deviation',
                   'Sharpe Ratio',
                   'Small Cap',
                   'Mid Cap',
                   'Large Cap',
                   'Others']

    category_order = extract_data_from_yaml('categories')

    fundsByType = {}
    for fund_data in data:
        if 'Category' in fund_data:
            category = fund_data.get('Category')
            fundStats = []
            if category in fundsByType:
                fundStats = fundsByType.get(category)
            fundStats.append([fund_data.get(column, '') for column in column_order])
            fundsByType[category] = fundStats
        else:
            print("No category for fund " + str(fund_data) + " hence skipping.")

    with open(csv_file_path, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(column_order)

        for category in category_order:
            if category in fundsByType.keys():
                writer.writerow([category])
                writer.writerows(fundsByType[category])

def extract_data_from_yaml(property):
    with open('fundslist.yaml', 'r') as file:
        data = yaml.safe_load(file)

    return data[property]

if __name__ == "__main__":
    # extract fund names
    funds = extract_data_from_yaml('funds')

    # extract data for funds
    extracted_data = get_stock_prices(funds)

    data_sorted_by_alpha = sorted(extracted_data, key=lambda x: (print(x) or float(x['Alpha'])) if x['Alpha'] and x['Alpha'] != '-' else float('-inf'), reverse=True)
    print(f"\033[91m{len(funds_with_no_data)} funds have no data. These are, {funds_with_no_data}.\033[0m")

    # export to file
    export_to_file(data_sorted_by_alpha)
