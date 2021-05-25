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

CREATE TABLE hourly_candles (
  ticker CHAR (7) REFERENCES symbol(ticker),
  date_hour TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  open_price DECIMAL(6, 2) NOT NULL,
  max_price DECIMAL(6, 2) NOT NULL,
  min_price DECIMAL(6, 2) NOT NULL,
  close_price DECIMAL(6, 2) NOT NULL,
  volume INTEGER NOT NULL,

  CONSTRAINT hourly_data_pkey PRIMARY KEY (ticker, date_hour),
  CHECK (open_price > 0 AND max_price > 0 AND min_price > 0 AND close_price > 0 AND volume >= 0),
  CHECK (max_price >= open_price AND max_price >= min_price AND max_price >= close_price),
  CHECK (min_price <= open_price AND min_price <= close_price)
);

CREATE TABLE daily_candles (
  ticker CHAR (7) REFERENCES symbol(ticker),
  day TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  open_price DECIMAL(6, 2) NOT NULL,
  max_price DECIMAL(6, 2) NOT NULL,
  min_price DECIMAL(6, 2) NOT NULL,
  close_price DECIMAL(6, 2) NOT NULL,
  volume INTEGER NOT NULL,

  CONSTRAINT daily_data_pkey PRIMARY KEY (ticker, day),
  CHECK (open_price > 0 AND max_price > 0 AND min_price > 0 AND close_price > 0 AND volume >= 0),
  CHECK (max_price >= open_price AND max_price >= min_price AND max_price >= close_price),
  CHECK (min_price <= open_price AND min_price <= close_price)
);

CREATE TABLE weekly_candles (
  ticker CHAR (7) REFERENCES symbol(ticker),
  week TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  open_price DECIMAL(6, 2) NOT NULL,
  max_price DECIMAL(6, 2) NOT NULL,
  min_price DECIMAL(6, 2) NOT NULL,
  close_price DECIMAL(6, 2) NOT NULL,
  volume INTEGER NOT NULL,

  CONSTRAINT weekly_data_pkey PRIMARY KEY (ticker, week),
  CHECK (open_price > 0 AND max_price > 0 AND min_price > 0 AND close_price > 0 AND volume >= 0),
  CHECK (max_price >= open_price AND max_price >= min_price AND max_price >= close_price),
  CHECK (min_price <= open_price AND min_price <= close_price)
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
  last_update_hourly_candles TIMESTAMP WITHOUT TIME ZONE,
  initial_date_hourly_candles TIMESTAMP WITHOUT TIME ZONE,
  final_date_hourly_candles TIMESTAMP WITHOUT TIME ZONE,
  last_update_daily_candles TIMESTAMP WITHOUT TIME ZONE,
  initial_date_daily_candles TIMESTAMP WITHOUT TIME ZONE,
  final_date_daily_candles TIMESTAMP WITHOUT TIME ZONE,

  CONSTRAINT status_pkey PRIMARY KEY(ticker)
  --add constraint final_date > inital_date
);

CREATE TYPE interest_origin_type AS ENUM ('DIV', 'IOC');

CREATE TABLE dividends (
  ticker CHAR (7) REFERENCES symbol(ticker),
  payment_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  price_per_stock DECIMAL(10, 6) NOT NULL,
  reference_date TIMESTAMP WITHOUT TIME ZONE,
  announcement_date TIMESTAMP WITHOUT TIME ZONE,
  origin interest_origin_type,
  manual_check BOOLEAN DEFAULT FALSE,

  CONSTRAINT dividends_pkey PRIMARY KEY(ticker, payment_date)
);

CREATE TABLE holidays (
  day TIMESTAMP WITHOUT TIME ZONE PRIMARY KEY
);

-- Triggers, Functions, Procedures, Views

CREATE OR REPLACE FUNCTION update_hourly_status() RETURNS trigger AS $update_hourly_status$
  BEGIN

    IF (TG_OP = 'INSERT') OR (TG_OP = 'UPDATE') THEN

      INSERT INTO status (ticker, last_update_hourly_candles, initial_date_hourly_candles, final_date_hourly_candles)
        SELECT hc.ticker, NOW(), MIN(hc.date_hour), MAX(hc.date_hour)
        FROM hourly_candles hc
        LEFT JOIN status s on s.ticker = hc.ticker
        GROUP BY hc.ticker, s.initial_date_hourly_candles, s.final_date_hourly_candles
        HAVING MIN(hc.date_hour) <> COALESCE(s.initial_date_hourly_candles, NOW()) OR MAX(hc.date_hour) <> COALESCE(s.final_date_hourly_candles, NOW())
      ON CONFLICT (ticker)
      DO UPDATE
        SET
          ticker = EXCLUDED.ticker,
          last_update_hourly_candles = EXCLUDED.last_update_hourly_candles,
          initial_date_hourly_candles = EXCLUDED.initial_date_hourly_candles,
          final_date_hourly_candles = EXCLUDED.final_date_hourly_candles;

    ELSIF (TG_OP = 'DELETE') THEN

      UPDATE status s1
      SET
        last_update_hourly_candles =
        CASE WHEN (initial_date_hourly_candles <> COALESCE(q.min_date, NOW()) OR final_date_hourly_candles <> COALESCE(q.max_date, NOW())) THEN NOW()
        ELSE last_update_hourly_candles END,
        initial_date_hourly_candles = q.min_date,
        final_date_hourly_candles = q.max_date
      FROM (
        SELECT s2.ticker, MIN(hc.date_hour) AS min_date, MAX(hc.date_hour) AS max_date FROM status s2
        LEFT JOIN hourly_candles hc on hc.ticker = s2.ticker
        GROUP BY s2.ticker
      ) q
      WHERE q.ticker = s1.ticker;

    ELSIF (TG_OP = 'TRUNCATE') THEN

      UPDATE status SET last_update_hourly_candles = NOW(), initial_date_hourly_candles = NULL, final_date_hourly_candles = NULL;

    END IF;

    RETURN NULL;
  END;
$update_hourly_status$ LANGUAGE plpgsql;


CREATE TRIGGER update_hourly_status
  AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE
  ON hourly_candles
  FOR EACH STATEMENT
  EXECUTE FUNCTION update_hourly_status();


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


-- CREATE AGGREGATE MUL(DOUBLE PRECISION) (SFUNC=float8mul, STYPE=DOUBLE PRECISION);

-- CREATE OR REPLACE FUNCTION cumulative_split(ticker_name CHAR(7), interval_start TIMESTAMP WITHOUT TIME ZONE, interval_stop TIMESTAMP WITHOUT TIME ZONE)
-- RETURNS TABLE(ticker CHAR(7), ratio REAL) AS $$
-- DECLARE result REAL;
-- BEGIN
--   RETURN QUERY SELECT s.ticker, CAST(MUL(s.ratio) AS REAL)
--   FROM split s
--   WHERE
--     s.ticker = ticker_name
-- 	AND s.split_date >= interval_start
-- 	AND s.split_date <= interval_stop
--   GROUP BY s.ticker;

-- END;
-- $$  LANGUAGE plpgsql
