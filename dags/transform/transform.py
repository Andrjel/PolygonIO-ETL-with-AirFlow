import pandas as pd
import numpy as np


class Transformers:
    def __init__(self) -> None:
        pass

    def transform_tickers_response(self, response: dict) -> pd.DataFrame:
        raw_data = pd.json_normalize(response)
        raw_data.drop(["cik", "share_class_figi", "market"], axis=1, inplace=True)
        raw_data["composite_figi"] = raw_data["composite_figi"].replace(np.nan, "")
        raw_data["currency_name"] = raw_data["currency_name"].str.upper()
        raw_data["locale"] = raw_data["locale"].str.upper()
        raw_data["last_updated_utc"] = pd.to_datetime(raw_data["last_updated_utc"])
        raw_data["active"] = raw_data["active"].astype(int)
        return raw_data        

    def transform_ticker_types_response(self, response: dict) -> pd.DataFrame:
        raw_data = pd.json_normalize(response)
        raw_data.drop(["locale"], axis=1, inplace=True)
        raw_data["code"] = raw_data["code"].str.upper()
        return raw_data
