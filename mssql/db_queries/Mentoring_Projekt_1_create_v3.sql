USE master;
GO

IF NOT EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'Stocks')
BEGIN
    CREATE DATABASE Stocks;
END
GO

USE Stocks;
GO
-- Created by Vertabelo (http://vertabelo.com)
-- Last modification date: 2024-04-24 19:41:53.802

-- tables
-- Table: Currency
CREATE TABLE "Currency" (
    "Code" nvarchar(3)  NOT NULL,
    "Name" nvarchar(100)  NOT NULL,
    CONSTRAINT "Currency_pk" PRIMARY KEY  ("Code")
);

-- Table: DailyQuote
CREATE TABLE "DailyQuote" (
    "Id" int  NOT NULL IDENTITY,
    "CloseValue" decimal(10,2)  NOT NULL,
    "OpenValue" decimal(10,2)  NOT NULL,
    "Volume" decimal(18,2)  NOT NULL,
    "High" decimal(18,2)  NOT NULL,
    "Low" decimal(10,2)  NOT NULL,
    "Timestamp" datetime  NOT NULL,
    "Stock_Ticker" nvarchar(10)  NOT NULL,
    CONSTRAINT "DailyQuote_pk" PRIMARY KEY  ("Id")
);

-- Table: Locales
CREATE TABLE "Locales" (
    "Code" nvarchar(10)  NOT NULL,
    "Name" nvarchar(100)  NOT NULL,
    CONSTRAINT "Locales_pk" PRIMARY KEY  ("Code")
);

-- Table: Stock
CREATE TABLE "Stock" (
    "Ticker" nvarchar(10)  NOT NULL,
    "StockTypeCode" varchar(10)  NOT NULL,
    "StockExchangeId" varchar(10)  NOT NULL,
    "CurrencyCode" nvarchar(3)  NOT NULL,
    "LocaleCode" nvarchar(10)  NOT NULL,
    "FIGI" nvarchar(12)  NOT NULL,
    "Name" ntext  NOT NULL,
    "IsActive" int  NOT NULL,
    "LastUpdate" datetime  NOT NULL,
    CONSTRAINT "Stock_pk" PRIMARY KEY  ("Ticker")
);

-- Table: StockTypes
CREATE TABLE "StockTypes" (
    "Code" varchar(10)  NOT NULL,
    "AssetClass" varchar(50)  NOT NULL,
    "Description" text  NOT NULL,
    CONSTRAINT "StockTypes_pk" PRIMARY KEY  ("Code")
);

-- Table: Stock_Exchange
CREATE TABLE "Stock_Exchange" (
    "StockExchangeId" varchar(10)  NOT NULL,
    "Locales_Code" nvarchar(10)  NOT NULL,
    CONSTRAINT "Stock_Exchange_pk" PRIMARY KEY  ("StockExchangeId")
);

-- foreign keys
-- Reference: DailyQuote_Stock (table: DailyQuote)
ALTER TABLE "DailyQuote" ADD CONSTRAINT "DailyQuote_Stock"
    FOREIGN KEY ("Stock_Ticker")
    REFERENCES "Stock" ("Ticker");

-- Reference: Stock_Exchange_Locales (table: Stock_Exchange)
ALTER TABLE "Stock_Exchange" ADD CONSTRAINT "Stock_Exchange_Locales"
    FOREIGN KEY ("Locales_Code")
    REFERENCES "Locales" ("Code");

-- Reference: Stock_Information_Currency (table: Stock)
ALTER TABLE "Stock" ADD CONSTRAINT "Stock_Information_Currency"
    FOREIGN KEY ("CurrencyCode")
    REFERENCES "Currency" ("Code");

-- Reference: Stock_Locales (table: Stock)
ALTER TABLE "Stock" ADD CONSTRAINT "Stock_Locales"
    FOREIGN KEY ("LocaleCode")
    REFERENCES "Locales" ("Code");

-- Reference: Stock_Stock_Exchange (table: Stock)
ALTER TABLE "Stock" ADD CONSTRAINT "Stock_Stock_Exchange"
    FOREIGN KEY ("StockExchangeId")
    REFERENCES "Stock_Exchange" ("StockExchangeId");

-- Reference: Stock_Stock_Type (table: Stock)
ALTER TABLE "Stock" ADD CONSTRAINT "Stock_Stock_Type"
    FOREIGN KEY ("StockTypeCode")
    REFERENCES "StockTypes" ("Code");

-- End of file.
