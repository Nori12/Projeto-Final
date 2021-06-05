-- Database: StockMarket

-- DROP DATABASE "StockMarket";

-- CREATE DATABASE "StockMarket"
--     WITH
--     OWNER = postgres
--     ENCODING = 'UTF8'
--     LC_COLLATE = 'C'
--     LC_CTYPE = 'C'
--     TABLESPACE = pg_default
--     CONNECTION LIMIT = -1;

-- COMMENT ON DATABASE "StockMarket"
--     IS 'A database to store stock market data.';

CREATE TABLE company_classification (
  id SERIAL PRIMARY KEY,
  economic_sector VARCHAR(100) NOT NULL,
  economic_subsector VARCHAR(100) NOT NULL,
  economic_segment VARCHAR(100) NOT NULL,

  UNIQUE (economic_sector, economic_subsector, economic_segment)
);

CREATE TABLE entity_type (
  id SERIAL PRIMARY KEY,
  type VARCHAR(10) NOT NULL UNIQUE
);

CREATE TABLE entity (
  trading_name VARCHAR(100) PRIMARY KEY,
  ticker_root CHAR (4) NOT NULL,
  entity_type_id INTEGER REFERENCES entity_type(id),
  company_classification_id INTEGER,

  CONSTRAINT fk_company_classification
    FOREIGN KEY(company_classification_id)
	  REFERENCES company_classification(id)
	  ON DELETE CASCADE
);

CREATE TABLE symbol (
  ticker CHAR (7) NOT NULL PRIMARY KEY,
  trading_name VARCHAR(100) REFERENCES entity(trading_name)
);

CREATE TABLE daily_candles (
  ticker CHAR (7) REFERENCES symbol(ticker),
  day TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  open_price DECIMAL(7, 2) NOT NULL,
  max_price DECIMAL(7, 2) NOT NULL,
  min_price DECIMAL(7, 2) NOT NULL,
  close_price DECIMAL(7, 2) NOT NULL,
  volume BIGINT NOT NULL,

  CONSTRAINT daily_data_pkey PRIMARY KEY (ticker, day),
  CONSTRAINT greater_than_zero CHECK (open_price > 0 AND max_price > 0 AND min_price > 0 AND close_price > 0 AND volume >= 0),
  CONSTRAINT max_is_max CHECK (max_price >= open_price AND max_price >= min_price AND max_price >= close_price),
  CONSTRAINT min_is_min CHECK (min_price <= open_price AND min_price <= close_price)
);

CREATE TABLE weekly_candles (
  ticker CHAR (7) REFERENCES symbol(ticker),
  week TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  open_price DECIMAL(7, 2) NOT NULL,
  max_price DECIMAL(7, 2) NOT NULL,
  min_price DECIMAL(7, 2) NOT NULL,
  close_price DECIMAL(7, 2) NOT NULL,
  volume BIGINT NOT NULL,

  CONSTRAINT weekly_data_pkey PRIMARY KEY (ticker, week),
  CONSTRAINT greater_than_zero CHECK (open_price > 0 AND max_price > 0 AND min_price > 0 AND close_price > 0 AND volume >= 0),
  CONSTRAINT max_is_max CHECK (max_price >= open_price AND max_price >= min_price AND max_price >= close_price),
  CONSTRAINT min_is_min CHECK (min_price <= open_price AND min_price <= close_price)
);

CREATE TABLE split (
  ticker CHAR (7) REFERENCES symbol(ticker),
  split_date TIMESTAMP WITHOUT TIME ZONE,
  ratio REAL NOT NULL,
  manual_check BOOLEAN DEFAULT FALSE,

  CONSTRAINT split_pkey PRIMARY KEY (ticker, split_date)
);

CREATE TYPE currency_type AS ENUM ('R$');

CREATE TABLE status (
  ticker CHAR (7) REFERENCES symbol(ticker),
  currency currency_type DEFAULT 'R$',
  last_update_daily_candles TIMESTAMP WITHOUT TIME ZONE,
  initial_date_daily_candles TIMESTAMP WITHOUT TIME ZONE,
  final_date_daily_candles TIMESTAMP WITHOUT TIME ZONE,

  CONSTRAINT status_pkey PRIMARY KEY(ticker)
);

CREATE TYPE interest_origin_type AS ENUM ('DIV', 'IOC');

CREATE TABLE dividends (
  ticker CHAR (7) REFERENCES symbol(ticker),
  payment_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  payment_date_correction TIMESTAMP WITHOUT TIME ZONE,
  price_per_stock DECIMAL(10, 6) NOT NULL,
  reference_date TIMESTAMP WITHOUT TIME ZONE,
  origin interest_origin_type,
  manual_check BOOLEAN DEFAULT FALSE,

  CONSTRAINT dividends_pkey PRIMARY KEY(ticker, payment_date)
);

CREATE TABLE holidays (
  day TIMESTAMP WITHOUT TIME ZONE PRIMARY KEY
);

CREATE TABLE daily_features (
  ticker CHAR (7) REFERENCES symbol(ticker),
  day TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  peak SMALLINT,
  ema_17 DECIMAL(10, 6),
  ema_72 DECIMAL(10, 6),
  up_down_trend_coef REAL,
  up_down_trend_status SMALLINT,

  CONSTRAINT daily_features_pkey PRIMARY KEY(ticker, day),
  CONSTRAINT peak_is_valid CHECK (peak = 1 OR peak = -1 or peak = 0),
  CONSTRAINT greater_than_zero CHECK (ema_17 > 0 AND ema_72 > 0),
  CONSTRAINT coef_in_bounds CHECK (up_down_trend_coef >= -1 AND up_down_trend_coef <= 1),
  CONSTRAINT up_down_trend_status_valid CHECK (up_down_trend_status = 1 OR up_down_trend_status = -1 OR up_down_trend_status = 0)
);

CREATE TABLE weekly_features (
  ticker CHAR (7) REFERENCES symbol(ticker),
  week TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  peak SMALLINT,
  ema_17 DECIMAL(10, 6),
  ema_72 DECIMAL(10, 6),
  up_down_trend_coef REAL,
  up_down_trend_status SMALLINT,

  CONSTRAINT weekly_features_pkey PRIMARY KEY(ticker, week),
  CONSTRAINT peak_is_valid CHECK (peak = 1 OR peak = -1 or peak = 0),
  CONSTRAINT greater_than_zero CHECK (ema_17 > 0 AND ema_72 > 0),
  CONSTRAINT coef_in_bounds CHECK (up_down_trend_coef >= -1 AND up_down_trend_coef <= 1),
  CONSTRAINT up_down_trend_status_valid CHECK (up_down_trend_status = 1 OR up_down_trend_status = -1 OR up_down_trend_status = 0)
)

-- Triggers, Functions, Procedures, Views

CREATE OR REPLACE FUNCTION update_daily_status() RETURNS trigger AS $update_daily_status$
  BEGIN

    IF (TG_OP = 'INSERT') OR (TG_OP = 'UPDATE') THEN

      INSERT INTO status (ticker, last_update_daily_candles, initial_date_daily_candles, final_date_daily_candles)
        SELECT hc.ticker, NOW(), MIN(hc.day), MAX(hc.day)
        FROM daily_candles hc
        LEFT JOIN status s on s.ticker = hc.ticker
        GROUP BY hc.ticker, s.initial_date_daily_candles, s.final_date_daily_candles
        HAVING MIN(hc.day) <> COALESCE(s.initial_date_daily_candles, NOW()) OR MAX(hc.day) <> COALESCE(s.final_date_daily_candles, NOW())
      ON CONFLICT (ticker)
      DO UPDATE
        SET
          ticker = EXCLUDED.ticker,
          last_update_daily_candles = EXCLUDED.last_update_daily_candles,
          initial_date_daily_candles = EXCLUDED.initial_date_daily_candles,
          final_date_daily_candles = EXCLUDED.final_date_daily_candles;

    ELSIF (TG_OP = 'DELETE') THEN

      UPDATE status s1
      SET
        last_update_daily_candles =
        CASE WHEN (initial_date_daily_candles <> COALESCE(q.min_date, NOW()) OR final_date_daily_candles <> COALESCE(q.max_date, NOW())) THEN NOW()
        ELSE last_update_daily_candles END,
        initial_date_daily_candles = q.min_date,
        final_date_daily_candles = q.max_date
      FROM (
        SELECT s2.ticker, MIN(hc.day) AS min_date, MAX(hc.day) AS max_date FROM status s2
        LEFT JOIN daily_candles hc on hc.ticker = s2.ticker
        GROUP BY s2.ticker
      ) q
      WHERE q.ticker = s1.ticker;

    ELSIF (TG_OP = 'TRUNCATE') THEN

      UPDATE status SET last_update_daily_candles = NOW(), initial_date_daily_candles = NULL, final_date_daily_candles = NULL;

    END IF;

    RETURN NULL;
  END;
$update_daily_status$ LANGUAGE plpgsql;


CREATE TRIGGER update_daily_status
  AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE
  ON daily_candles
  FOR EACH STATEMENT
  EXECUTE FUNCTION update_daily_status();
