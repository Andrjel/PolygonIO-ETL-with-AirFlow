-- Created by Vertabelo (http://vertabelo.com)
-- Last modification date: 2024-04-25 20:03:19.969

-- foreign keys
ALTER TABLE DailyQuote DROP CONSTRAINT DailyQuote_Stock;

ALTER TABLE Stock_Exchange DROP CONSTRAINT Stock_Exchange_Locales;

ALTER TABLE Stock DROP CONSTRAINT Stock_Information_Currency;

ALTER TABLE Stock DROP CONSTRAINT Stock_Locales;

ALTER TABLE Stock DROP CONSTRAINT Stock_Stock_Exchange;

ALTER TABLE Stock DROP CONSTRAINT Stock_Stock_Type;

-- tables
DROP TABLE Currency;

DROP TABLE DailyQuote;

DROP TABLE Locales;

DROP TABLE Stock;

DROP TABLE StockTypes;

DROP TABLE Stock_Exchange;

-- End of file.

