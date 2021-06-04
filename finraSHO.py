import calendar
import json
import requests
import pandas as pd
import yfinance as yf
from pandas_datareader import data as pdr
import datetime
import time
from io import StringIO

filename = "SHO.csv"

symbols = [ "GME", "AMC", "KOSS", "NAKD", "BBBY", "NOK", "GM", "AAPL", "TSLA", "MSFT", "SPY", "SPXS" ]
years = [2020,2021]
fields = ["tradeReportDate", "securitiesInformationProcessorSymbolIdentifier", "shortParQuantity","shortExemptParQuantity","totalParQuantity","marketCode","reportingFacilityCode"]

def timing():
    start_time = time.time()
    return lambda x: print("[{:.2f}s] {}".format(time.time() - start_time, x))

t = timing()

def finra(fields,symbols):

    finraUrl = "https://api.finra.org/data/group/OTCMarket/name/regShoDaily"

    finraData = pd.DataFrame(columns = fields)

    for year in years:
        for month in range(1, 12):
            startDate = str(year) + "-" + str(month).zfill(2) + "-" + "01"
            endDate = str(year) + "-" + str(month).zfill(2) + "-" + str(calendar._monthlen(year, month))

            tempQuery = {
            "fields":fields,  
            "limit" : 50000,
            "domainFilters": [{
            "fieldName" : "securitiesInformationProcessorSymbolIdentifier",
            "values" : symbols
            }],
            "dateRangeFilters": [ {

                "startDate" : startDate,

                "endDate" : endDate,

                "fieldName" : "tradeReportDate"

            } ]
            }

            query = json.dumps(tempQuery)

            r = requests.post(finraUrl, query)
            temp = pd.read_csv(StringIO(r.text))
            finraData = finraData.append(temp, ignore_index="True")
    t("Loaded {} rows of SHO data from FINRA".format(len(finraData)))
    return finraData

def price(finra):
    yf.pdr_override()
    df = finra

    df = df.drop(columns=["shortParQuantity","shortExemptParQuantity","totalParQuantity","marketCode","reportingFacilityCode"])
    df = df.sort_values(by=['securitiesInformationProcessorSymbolIdentifier','tradeReportDate'])
    df = df.drop_duplicates()
    symbols = df["securitiesInformationProcessorSymbolIdentifier"].drop_duplicates()

    priceColumns = ['securitiesInformationProcessorSymbolIdentifier','tradeReportDate','High', 'Low', 'Open', 'Close', 'Volume']
    priceData = pd.DataFrame(columns = priceColumns)
    for symbol in symbols:
            curr = df[df["securitiesInformationProcessorSymbolIdentifier"].isin([symbol])]
            minDate = datetime.date.fromisoformat(curr['tradeReportDate'].min())
            maxDate = datetime.date.fromisoformat(curr['tradeReportDate'].max())
            maxDate = datetime.date.fromisoformat(curr['tradeReportDate'].max()) + datetime.timedelta((5-maxDate.weekday()) % 7 ) #yahoo finance ignores the endDate in the result set.
            dataSet = pdr.get_data_yahoo(symbol,minDate,maxDate)
            dataSet['securitiesInformationProcessorSymbolIdentifier'] = symbol
            dataSet['tradeReportDate'] = pd.to_datetime(dataSet.index)
            dataSet['tradeReportDate'] = dataSet['tradeReportDate'].dt.strftime('%Y-%m-%d')
            dataSet = dataSet.drop(columns=["Adj Close"])
            priceData = priceData.append(dataSet, ignore_index="True")

    priceData = priceData.rename(columns={"High": "YF_High", "Low": "YF_Low", "Open": "YF_Open", "Close": "YF_Close", "Volume": "YF_Volume"})
    t("Loaded {} rows of pricing data from Yahoo Finance".format(len(priceData)))
    return priceData

print("Beginning processing run")
finra = finra(fields,symbols)
price = price(finra)

join = finra.merge(price, left_on=['securitiesInformationProcessorSymbolIdentifier','tradeReportDate'], right_on=['securitiesInformationProcessorSymbolIdentifier','tradeReportDate'])
join = join.sort_values(by=['securitiesInformationProcessorSymbolIdentifier','tradeReportDate'])

t("Merged {} rows".format(len(join)))

join.to_csv(filename,mode='w',index=False)
t("Exported {} rows".format(len(join)))
