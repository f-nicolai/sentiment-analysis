from re import IGNORECASE, escape
from pandas import Series, read_csv
from pathlib import Path

from utils.tickers_terminology import *

companies_names = read_csv(Path(__file__).parent/'data/US_companies_and_tickers.csv',sep=';')
companies_names_transco = dict(zip(companies_names['short_name'].str.lower().str.replace('\\',''),companies_names['symbol']))

def detect_tickers(text:Series) -> Series:
    tickers = text.str \
        .findall(r'\b([A-Z]{1,5}|' + '|'.join(indices) + r')\b') \
        .apply(lambda tickers: sorted([x for x in set(tickers) if x.replace('$', '').upper() in list(US_tickers) + list(indices) and x not in blacklist]))

    companies = text.str.lower().str.findall(r'\b('+'|'.join(list(companies_names['short_name'])) + r')\b', flags=IGNORECASE) \
        .apply(lambda company: sorted([companies_names_transco[x.lower()] for x in set(company) if companies_names_transco[x.lower()] in US_tickers]))

    return (tickers+companies).apply(lambda l : '|'.join(sorted(set(l))))