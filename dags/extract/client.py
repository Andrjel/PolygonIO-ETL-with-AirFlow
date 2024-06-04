import dataclasses
import requests


def fetch_data_decorator(func):
    """
    Decorator to fetch data from the API.

    :param func: The API function being decorated.
    :return: The wrapper function.
    :raises: Exception if the limit is reached.
    """
    def wrapper(self, *args):
        result = []
        result.append({"name": func.__name__})
        response = func(self, *args)
        if response.get("next_url", None):
            result.extend(self.fetch_data(response.get("next_url")))
        return result

    return wrapper


@dataclasses.dataclass
class ClientSync:
    """
    Polygon.io API client
    """
    _api_key = 'BH1TqYfUvL6xV1YWnpVdxiXYYZsikHM8'
    _endpoint = 'https://api.polygon.io'

    @fetch_data_decorator
    def get_all_tickers(self, ticker=None, market="stocks", active=True, limit=1000, sort="ticker", order="asc"):
        """
        Fetches all active tickers from the Polygon.io API
        :return: list.
        """
        return requests.get(
            f"{self._endpoint}/v3/reference/tickers",
            params={
                "ticker": ticker,
                "market": market,
                "active": active,
                "limit": limit,
                "sort": sort,
                "order": order,
                "apiKey": self._api_key
            }
        ).json()

    @fetch_data_decorator
    def get_all_ticker_types(self, asset_class=None, locale=None):
        """
        Fetches all ticker types from the Polygon.io API
        :return: list.
        """
        return requests.get(
            f"{self._endpoint}/v3/reference/tickers/types",
            params={
                "asset_class": asset_class,
                "locale": locale,
                "apiKey": self._api_key
            }
        ).json()

    @fetch_data_decorator
    def get_stock_dividend(self, ticker):
        """
        Get dividend information for certain stock
        :param ticker: Company ticker
        :return: list
        """
        return requests.get(
            f"{self._endpoint}/v3/reference/dividends",
            params={
                "ticker": ticker,
                "apiKey": self._api_key
            }
        ).json()

    @fetch_data_decorator
    def get_aggregate_bars(self, ticker, multiplier, timespan, date_from, date_to, sort="asc", limit=5000):
        """
        Get aggregate bars for a stock over a given date range in custom time window sizes.
        For example, if timespan = ‘minute’ and multiplier = ‘5’ then 5-minute bars will be returned.
        :param ticker: Specify a case-sensitive ticker symbol.
        :param multiplier: The size of the timespan multiplier.
        :param timespan: The size of the time window.
        :param date_from: YYYY-MM-DD or a millisecond timestamp
        :param date_to: YYYY-MM-DD or a millisecond timestamp
        :param sort: asc/desc
        :param limit: Max 50000 and Default 5000
        :return:
        """
        return requests.get(
            f"{self._endpoint}/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{date_from}/{date_to}",
            params={
                "sort": sort,
                "limit": limit,
                "apiKey": self._api_key
            }
        ).json()

    @fetch_data_decorator
    def get_grouped_daily(self, date):
        """
        Get the daily open, high, low, and close (OHLC) for the entire stocks/equities markets.
        :param date: The beginning date for the aggregate window.
        :return: list
        """
        return requests.get(
            f"{self._endpoint}/v2/aggs/grouped/locale/us/market/stocks/{date}",
            params={
                "apiKey": self._api_key
            }
        ).json()

    def fetch_data(self, url):
        """
        Support method to fetch data from the API
        :param url: next URL to fetch data from (next page)
        :return: list
        """
        result = []
        response = requests.get(url, params={"apiKey": self._api_key})
        result.extend(response.json().get("results", {}))
        if response.json().get("next_url", None):
            result.extend(self.fetch_data(response.json().get("next_url")))
        return result
    
