
-- Total de operações
SELECT COUNT(*) FROM operation o
INNER JOIN strategy s ON o.strategy_id = s.id
INNER JOIN negotiation n ON n.operation_id = o.id AND buy_sell_flag = 'B'
WHERE
  s.id = 58
  AND o.state = 'CLOSE'

-- Operações de volume zero
SELECT n.volume FROM operation o
INNER JOIN strategy s ON o.strategy_id = s.id
INNER JOIN negotiation n ON n.operation_id = o.id AND buy_sell_flag = 'B'
WHERE
  s.id = 58
  AND n.volume <= 2

-- Análise de operações de sucesso
SELECT 
  q.ticker, q.start_date, q.end_date, q.target_purchase_price, q.target_sale_price, q.stop_loss, 
  MIN(dc.min_price) AS min_price, 
  CASE 
    WHEN ROUND((q.target_purchase_price - MIN(dc.min_price)) / q.target_purchase_price, 4) < 0 THEN 0.0000
	ELSE ROUND((q.target_purchase_price - MIN(dc.min_price)) / q.target_purchase_price, 4)
  END AS max_risk
FROM daily_candles dc
INNER JOIN
(SELECT 
  o.id, o.ticker, o.start_date, o.end_date, o.target_purchase_price, o.target_sale_price, o.stop_loss
FROM operation o
INNER JOIN strategy s ON o.strategy_id = s.id AND o.state = 'CLOSE'
INNER JOIN negotiation n ON n.operation_id = o.id AND buy_sell_flag = 'S' AND stop_flag = FALSE AND partial_sale_flag = FALSE
WHERE
  s.id = 58
) q ON q.ticker = dc.ticker
WHERE dc.day >= q.start_date AND dc.day < q.end_date
GROUP BY q.id, q.ticker, q.start_date, q.end_date, q.target_purchase_price, q.target_sale_price, q.stop_loss
ORDER BY max_risk


-- Análise de operações de falha
SELECT 
  q.id, q.ticker, q.start_date, q.end_date, q.target_purchase_price, q.target_sale_price, q.stop_loss, 
  MAX(dc.max_price) AS max_price, ROUND((MAX(dc.max_price) / q.target_purchase_price), 4) AS max_percentage,
  CASE 
    WHEN FLOOR((MAX(dc.max_price) - q.target_purchase_price) / (q.target_purchase_price - q.stop_loss)) = -1 THEN 0
	ELSE FLOOR((MAX(dc.max_price) - q.target_purchase_price) / (q.target_purchase_price - q.stop_loss))
  END AS risk_level
FROM daily_candles dc
INNER JOIN
(SELECT 
  o.id, o.ticker, o.start_date, o.end_date, o.target_purchase_price, o.target_sale_price, o.stop_loss
FROM operation o
INNER JOIN strategy s ON o.strategy_id = s.id
INNER JOIN negotiation n ON n.operation_id = o.id AND buy_sell_flag = 'S' AND stop_flag = TRUE AND partial_sale_flag = FALSE
WHERE
  s.id = 58
) q ON q.ticker = dc.ticker
WHERE dc.day > q.start_date AND dc.day < q.end_date
GROUP BY q.id, q.ticker, q.start_date, q.end_date, q.target_purchase_price, q.target_sale_price, q.stop_loss
ORDER BY risk_level DESC

-- Análise de Stop móvel por faixas sobre operações de sucesso
SELECT 
	q3.ticker, 
    q3.target_purchase_price AS tgp, 
    q3.stop_loss AS sl, 
	ROUND((q3.target_purchase_price - q3.stop_loss) / q3.target_purchase_price, 4) AS op_risk,
    q3.target_sale_price AS tsp, 
    q3.start_date AS m0_day, 
    q3.min_price_m0,
    q3.max_risk_m0,
    q3.m1_day,
    q3.m1_price,
	MIN(dc_m1.min_price) AS min_price_m1,
    CASE 
        WHEN ROUND((q3.m1_price - MIN(dc_m1.min_price)) / q3.m1_price, 4) < 0 THEN 0.0000
        ELSE ROUND((q3.m1_price - MIN(dc_m1.min_price)) / q3.m1_price, 4)
    END AS max_risk_m1,
    q3.m2_day,
    q3.m2_price,
	MIN(dc_m2.min_price) AS min_price_m2,
    CASE 
        WHEN ROUND((q3.m2_price - MIN(dc_m2.min_price)) / q3.m2_price, 4) < 0 THEN 0.0000
        ELSE ROUND((q3.m2_price - MIN(dc_m2.min_price)) / q3.m2_price, 4)
    END AS max_risk_m2,
	CASE 
		WHEN MIN(dc_m1.min_price) <= q3.target_purchase_price THEN 'Y'
		ELSE 'N'
	END AS m1_stop_hit,
	CASE 
		WHEN MIN(dc_m2.min_price) <= q3.target_purchase_price + (q3.target_purchase_price - q3.stop_loss) THEN 'Y'
		ELSE 'N'
	END AS m2_stop_hit
FROM 
	(SELECT 
		q2.ticker, 
        q2.end_date, 
        q2.target_purchase_price, 
        q2.stop_loss, 
        q2.target_sale_price, 
        q2.start_date, 
        q2.min_price_m0, 
        q2.max_risk_m0,
		MIN(dc_m1.day) AS m1_day, 
        q2.m1_price,
		MIN(dc_m2.day) AS m2_day,
        q2.m2_price
	FROM
		(SELECT 
			q.ticker, 
            q.end_date,
            q.target_purchase_price, 
            q.stop_loss, 
            q.target_sale_price, 
            q.start_date, 
			MIN(dc.min_price) AS min_price_m0, 
			CASE 
				WHEN ROUND((q.target_purchase_price - MIN(dc.min_price)) / q.target_purchase_price, 4) < 0 THEN 0.0000
				ELSE ROUND((q.target_purchase_price - MIN(dc.min_price)) / q.target_purchase_price, 4)
			END AS max_risk_m0,
		    ROUND(q.target_purchase_price + (q.target_purchase_price - q.stop_loss), 2) AS m1_price,
		    ROUND(q.target_purchase_price + 2*(q.target_purchase_price - q.stop_loss), 2) AS m2_price
		FROM 
			(SELECT o.id, o.ticker, o.start_date, o.end_date, o.target_purchase_price, o.target_sale_price, o.stop_loss
			FROM operation o
			INNER JOIN strategy s ON o.strategy_id = s.id AND o.state = 'CLOSE'
			INNER JOIN negotiation n ON n.operation_id = o.id 
                AND buy_sell_flag = 'S' 
                AND stop_flag = FALSE 
                AND partial_sale_flag = FALSE
			WHERE
				s.id = 58
			) q
        LEFT JOIN daily_candles dc ON q.ticker = dc.ticker AND dc.day > q.start_date AND dc.day <= q.end_date
		GROUP BY q.ticker, q.end_date, q.target_purchase_price, q.stop_loss, q.target_sale_price, q.start_date
		) q2
	LEFT JOIN daily_candles dc_m1 
		ON dc_m1.ticker = q2.ticker 
		AND dc_m1.close_price > ROUND(q2.target_purchase_price + (q2.target_purchase_price - q2.stop_loss), 2)
		AND dc_m1.day > q2.start_date
		AND dc_m1.day <= q2.end_date
	LEFT JOIN daily_candles dc_m2
		ON dc_m2.ticker = q2.ticker 
		AND dc_m2.close_price > ROUND(q2.target_purchase_price + 2 * (q2.target_purchase_price - q2.stop_loss), 2)
        AND dc_m2.day > q2.start_date
		AND dc_m2.day <= q2.end_date  
	GROUP BY 
		q2.ticker, q2.end_date, q2.target_purchase_price, q2.stop_loss, q2.target_sale_price, q2.start_date, 
        q2.min_price_m0, q2.max_risk_m0, q2.m1_price, q2.m2_price
	) q3
LEFT JOIN daily_candles dc_m1
	ON dc_m1.ticker = q3.ticker 
	AND dc_m1.day > q3.m1_day
	AND dc_m1.day <= q3.m2_day  
LEFT JOIN daily_candles dc_m2
	ON dc_m2.ticker = q3.ticker 
	AND dc_m2.day > q3.m2_day
	AND dc_m2.day <= q3.end_date  	
GROUP BY 
	q3.ticker, q3.end_date, q3.target_purchase_price, q3.stop_loss, q3.target_sale_price, q3.start_date, 
    q3.min_price_m0, q3.max_risk_m0, q3.m1_day, q3.m1_price, q3.m2_day, q3.m2_price
ORDER BY m0_day ASC

