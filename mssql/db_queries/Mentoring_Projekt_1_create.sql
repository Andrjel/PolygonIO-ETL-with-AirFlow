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
-- Last modification date: 2024-03-15 17:35:55.691

-- tables
-- Table: Country
CREATE TABLE Country (
    Code nvarchar(3)  NOT NULL,
    Name nvarchar(100)  NOT NULL,
    CONSTRAINT Country_pk PRIMARY KEY  (Code)
);

-- Table: Currency
CREATE TABLE Currency (
    Code nvarchar(3)  NOT NULL,
    Name nvarchar(100)  NOT NULL,
    Is_Relevant tinyint  NOT NULL,
    CONSTRAINT Currency_pk PRIMARY KEY  (Code)
);

-- Table: Currency_Quotes
CREATE TABLE Currency_Quotes (
    Id int  NOT NULL,
    Currency_Code nvarchar(3)  NOT NULL,
    Timestamp datetime  NOT NULL,
    Rate decimal(10,2)  NOT NULL,
    CONSTRAINT Currency_Quotes_pk PRIMARY KEY  (Id)
);

-- Table: Quotes
CREATE TABLE Quotes (
    Id int  NOT NULL,
    Stock_Id int  NOT NULL,
    Bid decimal(10,2)  NOT NULL,
    Ask decimal(10,2)  NOT NULL,
    Prevoius_Close decimal(10,2)  NOT NULL,
    [Open] decimal(10,2)  NOT NULL,
    Volume int  NOT NULL,
    Capitalization decimal(18,2)  NOT NULL,
    Last_Year_Min decimal(10,2)  NOT NULL,
    Last_Year_Max decimal(10,2)  NOT NULL,
    CONSTRAINT Quotes_pk PRIMARY KEY  (Id)
);

-- Table: Stock
CREATE TABLE Stock (
    Id int  NOT NULL,
    Stock_Type_Id int  NOT NULL,
    Stock_Exchange_Id int  NOT NULL,
    Country_Code nvarchar(3)  NOT NULL,
    Currency_Code nvarchar(3)  NOT NULL,
    Name ntext  NOT NULL,
    Ticker nvarchar(10)  NOT NULL,
    ISIN nvarchar(12)  NOT NULL,
    Description ntext  NOT NULL,
    CONSTRAINT Stock_pk PRIMARY KEY  (Id)
);

-- Table: Stock_Exchange
CREATE TABLE Stock_Exchange (
    Id int  NOT NULL,
    Country_Code nvarchar(3)  NOT NULL,
    City varchar(100)  NOT NULL,
    Ticker varchar(10)  NOT NULL,
    CONSTRAINT Stock_Exchange_pk PRIMARY KEY  (Id)
);

-- Table: Stock_Type
CREATE TABLE Stock_Type (
    Id int  NOT NULL,
    Stock_Type varchar(50)  NOT NULL,
    CONSTRAINT If_StochType_In_Certain_Types CHECK (Stock_Type IN ('Akcja', 'Obligacja', 'ETF')),
    CONSTRAINT Stock_Type_pk PRIMARY KEY  (Id)
);

-- foreign keys
-- Reference: Currency_Quotes_Currency (table: Currency_Quotes)
ALTER TABLE Currency_Quotes ADD CONSTRAINT Currency_Quotes_Currency
    FOREIGN KEY (Currency_Code)
    REFERENCES Currency (Code);

-- Reference: Quotes_Stock (table: Quotes)
ALTER TABLE Quotes ADD CONSTRAINT Quotes_Stock
    FOREIGN KEY (Stock_Id)
    REFERENCES Stock (Id);

-- Reference: Stock_Exchange_Country (table: Stock_Exchange)
ALTER TABLE Stock_Exchange ADD CONSTRAINT Stock_Exchange_Country
    FOREIGN KEY (Country_Code)
    REFERENCES Country (Code);

-- Reference: Stock_Information_Country (table: Stock)
ALTER TABLE Stock ADD CONSTRAINT Stock_Information_Country
    FOREIGN KEY (Country_Code)
    REFERENCES Country (Code);

-- Reference: Stock_Information_Currency (table: Stock)
ALTER TABLE Stock ADD CONSTRAINT Stock_Information_Currency
    FOREIGN KEY (Currency_Code)
    REFERENCES Currency (Code);

-- Reference: Stock_Stock_Exchange (table: Stock)
ALTER TABLE Stock ADD CONSTRAINT Stock_Stock_Exchange
    FOREIGN KEY (Stock_Exchange_Id)
    REFERENCES Stock_Exchange (Id);

-- Reference: Stock_Stock_Type (table: Stock)
ALTER TABLE Stock ADD CONSTRAINT Stock_Stock_Type
    FOREIGN KEY (Stock_Type_Id)
    REFERENCES Stock_Type (Id);

-- End of file.

