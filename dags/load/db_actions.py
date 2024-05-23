from .db_connection import DbConnection
import pandas as pd


class Insert:
    """
    Class to insert data into the database
    """
    def __init__(self):
        self.db = DbConnection()

    def insert_tiker_types(self, tiker_types: pd.DataFrame) -> None:
        """
        Insert the tiker types into the database
        :param tiker_types: DataFrame with the ticker types
        """
        query = """
                MERGE INTO StockTypes AS target
                USING (VALUES (?, ?, ?)) AS source (Code, AssetClass, Description)
                ON target.Code = source.Code
                WHEN NOT MATCHED THEN
                    INSERT (Code, AssetClass, Description)
                    VALUES (source.Code, source.AssetClass, source.Description);
                """
        for _, row in tiker_types.iterrows():
            self.db.execute_query(query, False, row["code"], row["asset_class"], row["description"])
        print("Tiker types inserted successfully")
    
    def insert_tickers(self, tickers: pd.DataFrame) -> None:
        """
        Insert the tickers into the database
        :param tickers: DataFrame with the tickers
        """
        for locale in tickers["locale"].unique():
            self.update_locale(locale)
        
        for currency in tickers["currency_name"].unique():
            self.update_currency(currency)
        
        for exchange in tickers["primary_exchange"].unique():
            self.update_stock_exchange(exchange, tickers[tickers["primary_exchange"] == exchange]["locale"].iloc[0])
        
        query = """
                MERGE INTO Stock AS target
                USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source (Ticker, StockTypeCode, StockExchangeCode, CurrencyCode, LocaleCode, FIGI, Name, IsActive, LastUpdate)
                ON target.Ticker = source.Ticker AND target.LastUpdate = source.LastUpdate
                WHEN NOT MATCHED THEN
                    INSERT (Ticker, StockTypeCode, StockExchangeCode, CurrencyCode, LocaleCode, FIGI, Name, IsActive, LastUpdate)
                    VALUES (source.Ticker, source.StockTypeCode, source.StockExchangeCode, source.CurrencyCode, source.LocaleCode, source.FIGI, source.Name, source.IsActive, source.LastUpdate);
                """
        for _, row in tickers.iterrows():
            self.db.execute_query(query, False, row["ticker"], row["type"], row["primary_exchange"], row["currency_name"], row["locale"], row["composite_figi"], row["name"], row["active"], row["last_updated_utc"])
        print("Tickers inserted successfully")
    
    def update_locale(self, locale: str, name: str = "") -> None:
        """
        Update the locale table
        :param locale: The locale to update
        :param name: The name of the locale - optional
        """
        query = """
                IF NOT EXISTS (SELECT 1 FROM Locales WHERE Code = ?)
                BEGIN
                    INSERT INTO Locales (Code, Name)
                    VALUES (?, ?)
                END
                """
        self.db.execute_query(query, False, locale, locale, name)
        print(f"Locale {locale} inserted successfully")
    
    def update_stock_exchange(self, exchange: str, locale: str) -> None:
        """
        Update the stock exchange table
        :param exchange: The exchange to update
        :param locale: The locale of the exchange
        """
        query = """
                IF NOT EXISTS (SELECT 1 FROM StockExchange WHERE Code = ?)
                BEGIN
                    INSERT INTO StockExchange (Code, LocaleCode)
                    VALUES (?, ?)
                END
                """
        self.db.execute_query(query, False, exchange, exchange, locale)
        print(f"Stock exchange {exchange} inserted successfully")
    
    def update_currency(self, currency: str, name:str = "") -> None:
        """
        Update the currency table
        :param currency: The currency to update
        :param name: The name of the currency - optional
        """
        query = """
                IF NOT EXISTS (SELECT 1 FROM Currency WHERE Code = ?)
                BEGIN
                    INSERT INTO Currency (Code, Name)
                    VALUES (?, ?)
                END 
                """
        self.db.execute_query(query, False, currency, currency, name)
        print(f"Currency {currency} inserted successfully")