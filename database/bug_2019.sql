
-- Weekly candles creation (Bug on every first week of year)
INSERT INTO weekly_candles (ticker, week, open_price, max_price, min_price, close_price, volume)
SELECT agg2.ticker, agg2.min_day, agg2.open_price, agg2.max_price, agg2.min_price, dc3.close_price, agg2.volume
FROM
	(SELECT agg.ticker, agg.min_day, dc2.open_price, agg.max_price, agg.min_price, agg.volume, agg.max_day
	FROM
		(SELECT dc.ticker, DATE_PART('year', dc.day) AS year, DATE_PART('week', dc.day) AS week, MIN(dc.day) AS min_day, MAX(dc.day) AS max_day,
			MAX(dc.max_price) AS max_price, MIN(dc.min_price) AS min_price, 
			SUM(dc.volume) AS volume
		FROM daily_candles dc
		WHERE dc.ticker = 'ABEV3'
		GROUP BY dc.ticker, DATE_PART('year', dc.day), DATE_PART('week', dc.day)) agg
	INNER JOIN daily_candles dc2 ON dc2.ticker = agg.ticker AND dc2.day = agg.min_day) agg2
INNER JOIN daily_candles dc3 ON dc3.ticker = agg2.ticker AND dc3.day = agg2.max_day
ON CONFLICT ON CONSTRAINT weekly_data_pkey DO NOTHING;




select * from weekly_candles where ticker = 'ABEV3' AND DATE(week) >= '2018-12-10' ORDER BY week;
select * from daily_candles where ticker = 'ABEV3' AND max_price = 18.76 ORDER BY day;

select dc.*, DATE_PART('year', dc.day) as year, DATE_PART('week', dc.day) as week from daily_candles dc 
where ticker = 'ABEV3' AND DATE(day) >= '2014-12-15' AND DATE(day) <= '2015-01-15' ORDER BY day;




SELECT EXTRACT(WEEK FROM TIMESTAMP '2013-01-06 00:00:00');
SELECT EXTRACT(WEEK FROM TIMESTAMP '2015-01-05 00:00:00');

-- Query de dentro 1
SELECT agg2.ticker, agg2.min_day, agg2.open_price, agg2.max_price, agg2.min_price, dc3.close_price, agg2.volume, agg2.year, agg2.week
FROM
	(SELECT agg.ticker, agg.min_day, dc2.open_price, agg.max_price, agg.min_price, agg.volume, agg.max_day, agg.year, agg.week
	FROM
		(SELECT dc.ticker, DATE_PART('year', dc.day) AS year, DATE_PART('week', dc.day) AS week, MIN(dc.day) AS min_day, MAX(dc.day) AS max_day,
			MAX(dc.max_price) AS max_price, MIN(dc.min_price) AS min_price, SUM(dc.volume) AS volume
		FROM daily_candles dc
		WHERE dc.ticker = 'ABEV3'
		GROUP BY dc.ticker, DATE_PART('year', dc.day), DATE_PART('week', dc.day)) agg
	INNER JOIN daily_candles dc2 ON dc2.ticker = agg.ticker AND dc2.day = agg.min_day) agg2
INNER JOIN daily_candles dc3 ON dc3.ticker = agg2.ticker AND dc3.day = agg2.max_day
where DATE(agg2.min_day) >= '2018-12-23'
ORDER BY agg2.ticker, agg2.min_day

-- Query de dentro 2
SELECT dc.ticker, DATE_PART('year', dc.day) AS year, DATE_PART('week', dc.day) AS week, MIN(dc.day) AS min_day, MAX(dc.day) AS max_day,
			MAX(dc.max_price) AS max_price, MIN(dc.min_price) AS min_price, SUM(dc.volume) AS volume
FROM daily_candles dc
WHERE dc.ticker = 'ABEV3'
GROUP BY dc.ticker, DATE_PART('year', dc.day), DATE_PART('week', dc.day)
ORDER BY dc.ticker, DATE_PART('year', dc.day), DATE_PART('week', dc.day);



-- Query de dentro 2 corrigida
SELECT 
	dc.ticker, 
-- 	CASE 
-- 		WHEN DATE_PART('week', dc.day) = 1 AND DATE_PART('month', dc.day) = 12 THEN DATE_PART('year', dc.day) + 1
-- 		ELSE DATE_PART('year', dc.day)
-- 	END AS year,
-- 	DATE_PART('week', dc.day) AS week, 
	MIN(dc.day) AS min_day, 
 	MAX(dc.day) AS max_day, 
	MAX(dc.max_price) AS max_price, 
	MIN(dc.min_price) AS min_price, 
	SUM(dc.volume) AS volume 
FROM daily_candles dc
WHERE dc.ticker = 'ABEV3' AND DATE_PART('year', dc.day) = 2019
GROUP BY dc.ticker,
	CASE 
		WHEN DATE_PART('week', dc.day) = 1 AND DATE_PART('month', dc.day) = 12 THEN DATE_PART('year', dc.day) + 1
		ELSE DATE_PART('year', dc.day)
	END, 
	DATE_PART('week', dc.day)
ORDER BY dc.ticker, 
	CASE 
		WHEN DATE_PART('week', dc.day) = 1 AND DATE_PART('month', dc.day) = 12 THEN DATE_PART('year', dc.day) + 1
		ELSE DATE_PART('year', dc.day)
	END,
	DATE_PART('week', dc.day)

-- Weekly candles creation (Bug fixed)
INSERT INTO weekly_candles (ticker, week, open_price, max_price, min_price, close_price, volume)
SELECT agg2.ticker, agg2.min_day, agg2.open_price, agg2.max_price, agg2.min_price, dc3.close_price, agg2.volume
FROM
	(SELECT agg.ticker, agg.min_day, dc2.open_price, agg.max_price, agg.min_price, agg.volume, agg.max_day
	FROM
		(
        SELECT 
            dc.ticker, 
            MIN(dc.day) AS min_day, 
			MAX(dc.day) AS max_day, 
            MAX(dc.max_price) AS max_price, 
            MIN(dc.min_price) AS min_price, 
            SUM(dc.volume) AS volume 
        FROM daily_candles dc
         WHERE dc.ticker = \'{ticker}\'
        GROUP BY dc.ticker,
            CASE 
                WHEN DATE_PART('week', dc.day) = 1 AND DATE_PART('month', dc.day) = 12 THEN DATE_PART('year', dc.day) + 1
                ELSE DATE_PART('year', dc.day)
            END, 
            DATE_PART('week', dc.day)
        ) agg
	INNER JOIN daily_candles dc2 ON dc2.ticker = agg.ticker AND dc2.day = agg.min_day) agg2
INNER JOIN daily_candles dc3 ON dc3.ticker = agg2.ticker AND dc3.day = agg2.max_day
ON CONFLICT ON CONSTRAINT weekly_data_pkey DO NOTHING;

-- TRUNCATE TABLE weekly_candles;