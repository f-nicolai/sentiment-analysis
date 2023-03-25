from re import IGNORECASE
from pandas import Series

from utils.tickers_terminology import *


def detect_tickers(text:Series) -> Series:
    return text.str \
        .findall(r'\b([A-Z]{1,5})\b', flags=IGNORECASE) \
        .apply(lambda text: '|'.join(sorted([x for x in set(text) if x.replace('$', '') in US_tickers and x not in blacklist])))
