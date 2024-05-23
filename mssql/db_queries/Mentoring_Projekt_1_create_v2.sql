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
-- Last modification date: 2024-04-15 17:26:52.673

-- tables
-- Table: Country
CREATE TABLE "Country" (
    "Code" nvarchar(3)  NOT NULL,
    "Name" nvarchar(100)  NOT NULL,
    CONSTRAINT "Country_pk" PRIMARY KEY  ("Code")
);

-- Table: Currency
CREATE TABLE "Currency" (
    "Code" nvarchar(3)  NOT NULL,
    "Name" nvarchar(100)  NOT NULL,
    "IsRelevant" tinyint  NOT NULL,
    CONSTRAINT "Currency_pk" PRIMARY KEY  ("Code")
);

-- Table: Currency_Quotes
CREATE TABLE "Currency_Quotes" (
    "Id" int  NOT NULL IDENTITY,
    "CurrencyCode" nvarchar(3)  NOT NULL,
    "Timestamp" datetime  NOT NULL,
    "Rate" decimal(10,2)  NOT NULL,
    CONSTRAINT "Currency_Quotes_pk" PRIMARY KEY  ("Id")
);

-- Table: Quotes
CREATE TABLE "Quotes" (
    "Id" int  NOT NULL IDENTITY,
    "StockId" int  NOT NULL,
    "Bid" decimal(10,2)  NOT NULL,
    "Ask" decimal(10,2)  NOT NULL,
    "CloseValue" decimal(10,2)  NOT NULL,
    "OpenValue" decimal(10,2)  NOT NULL,
    "Volume" decimal(18,2)  NOT NULL,
    "Capitalization" decimal(18,2)  NOT NULL,
    "LastYearMin" decimal(10,2)  NOT NULL,
    "LastYearMax" decimal(10,2)  NOT NULL,
    "Timestamp" datetime  NOT NULL,
    CONSTRAINT "Quotes_pk" PRIMARY KEY  ("Id")
);

-- Table: Stock
CREATE TABLE "Stock" (
    "Id" int  NOT NULL IDENTITY,
    "StockTypeCode" varchar(3)  NOT NULL,
    "StockExchangeId" int  NOT NULL,
    "CountryCode" nvarchar(3)  NOT NULL,
    "CurrencyCode" nvarchar(3)  NOT NULL,
    "Name" ntext  NOT NULL,
    "Ticker" nvarchar(10)  NOT NULL,
    "FIGI" nvarchar(12)  NOT NULL,
    "IsActive" int  NOT NULL,
    "LastUpdate" datetime  NOT NULL,
    CONSTRAINT "Stock_pk" PRIMARY KEY  ("Id")
);

-- Table: StockTypes
CREATE TABLE "StockTypes" (
    "Code" varchar(3)  NOT NULL,
    "StockClass" varchar(50)  NOT NULL,
    "Description" text  NOT NULL,
    CONSTRAINT "StockTypes_pk" PRIMARY KEY  ("Code")
);

-- Table: Stock_Exchange
CREATE TABLE "Stock_Exchange" (
    "Id" int  NOT NULL IDENTITY,
    "CountryCode" nvarchar(3)  NOT NULL,
    "City" varchar(100)  NOT NULL,
    "Ticker" varchar(10)  NOT NULL,
    CONSTRAINT "Stock_Exchange_pk" PRIMARY KEY  ("Id")
);

-- foreign keys
-- Reference: Currency_Quotes_Currency (table: Currency_Quotes)
ALTER TABLE "Currency_Quotes" ADD CONSTRAINT "Currency_Quotes_Currency"
    FOREIGN KEY ("CurrencyCode")
    REFERENCES "Currency" ("Code");

-- Reference: Quotes_Stock (table: Quotes)
ALTER TABLE "Quotes" ADD CONSTRAINT "Quotes_Stock"
    FOREIGN KEY ("StockId")
    REFERENCES "Stock" ("Id");

-- Reference: Stock_Exchange_Country (table: Stock_Exchange)
ALTER TABLE "Stock_Exchange" ADD CONSTRAINT "Stock_Exchange_Country"
    FOREIGN KEY ("CountryCode")
    REFERENCES "Country" ("Code");

-- Reference: Stock_Information_Country (table: Stock)
ALTER TABLE "Stock" ADD CONSTRAINT "Stock_Information_Country"
    FOREIGN KEY ("CountryCode")
    REFERENCES "Country" ("Code");

-- Reference: Stock_Information_Currency (table: Stock)
ALTER TABLE "Stock" ADD CONSTRAINT "Stock_Information_Currency"
    FOREIGN KEY ("CurrencyCode")
    REFERENCES "Currency" ("Code");

-- Reference: Stock_Stock_Exchange (table: Stock)
ALTER TABLE "Stock" ADD CONSTRAINT "Stock_Stock_Exchange"
    FOREIGN KEY ("StockExchangeId")
    REFERENCES "Stock_Exchange" ("Id");

-- Reference: Stock_Stock_Type (table: Stock)
ALTER TABLE "Stock" ADD CONSTRAINT "Stock_Stock_Type"
    FOREIGN KEY ("StockTypeCode")
    REFERENCES "StockTypes" ("Code");

-- End of file.

