WITH get_raw_data AS (
	SELECT  
	visit_id as session_id,
	y.client_id,
	order_id,
	current_visit_id_datetime as timestamp,  
	CONCAT(visit_source, ' / ', medium) as source_medium,
	-- добавляем внутренний переход в список которые будем игнорить
	CASE WHEN visit_source = 'EXAMLPE' AND medium = 'internal' THEN 'direct / (none)' ELSE 
	CONCAT(visit_source, ' / ', medium) END source,
	CASE WHEN order_id IS NOT NULL THEN 1 ELSE 0 END count_order,
	ROW_NUMBER() OVER(PARTITION BY client_id ORDER BY current_visit_id_datetime ASC) as rn_client_id,
	case when сatalog_price is null then 0 else сatalog_price end revenue
	FROM  session_totals y 
	-- вытаскиваем уникальные сессии + уникальные заказы
	WHERE 
	1=1
	AND rn_visit_id__order_id = 1 
	-- фильтруем искусственные визиты и пустые значения
	 AND
	visit_id IS NOT NULL AND visit_id > 0
	)

-- EXAMLPE-internal превратить в директ также


select
session_id,
sum(revenue) revenue,
sum(last_non_direct) last_non_direct,
sum(first_click) first_click
from
(
SELECT  timestamp, session_id, client_id, source,
	CASE
	-- все кейсы когда по сессии есть конверсия
	WHEN revenue > 0 THEN
		-- если в сессиях больше одного источника, то оставляем все как есть
		CASE WHEN COUNT(*) OVER(PARTITION BY client_id, session_id ORDER BY timestamp) > 1 AND
			source = 'direct / (none)' THEN 
				CASE WHEN LAG(source) OVER(PARTITION BY client_id ORDER BY timestamp) != 'direct / (none)'
					AND LAG(revenue) OVER(PARTITION BY client_id ORDER BY timestamp) = 0
					THEN LAG(revenue) OVER(PARTITION BY client_id, session_id ORDER BY timestamp ROWS BETWEEN 3 Preceding AND 0 Following)
					ELSE revenue END 

				
			WHEN COUNT(*) OVER(PARTITION BY client_id, session_id ORDER BY timestamp) > 1 AND
			source != 'direct / (none)' THEN revenue
	
			WHEN source = 'direct / (none)' THEN
			-- если прямой заход первый заход оставляем как есть
				CASE WHEN timestamp = FIRST_VALUE(timestamp) OVER (PARTITION BY client_id ORDER BY timestamp) 
					THEN revenue
			-- если эта сессия прямой заход, а предыдущая прямой заход, то оставляем как есть
					WHEN source = 'direct / (none)' AND
					LAG(source) OVER(PARTITION BY client_id ORDER BY timestamp) = 'direct / (none)'
					THEN revenue
					ELSE 0 END
			
		-- если сессия не прямой заход
			WHEN source != 'direct / (none)' AND 
		-- но следующая сессия с конверсией и прямой заход, то мы суммируем эту конверсию и конверсию следующей сессии
			LEAD(revenue) OVER(PARTITION BY client_id ORDER BY timestamp ROWS BETWEEN 0 Preceding AND 1 Following) > 0 AND
			LEAD(source) OVER(PARTITION BY client_id ORDER BY timestamp ROWS BETWEEN 0 Preceding AND 1 Following) = 'direct / (none)'
				THEN revenue + LEAD(revenue) OVER(PARTITION BY client_id ORDER BY timestamp ROWS BETWEEN 1 Preceding AND 1 Following)
				ELSE revenue
				END
	
	WHEN revenue = 0 THEN 
		CASE WHEN COUNT(*) OVER(PARTITION BY client_id, session_id ORDER BY timestamp) > 1 AND
			source = 'direct / (none)' THEN revenue
			WHEN COUNT(*) OVER(PARTITION BY client_id, session_id ORDER BY timestamp) > 1 AND
			source != 'direct / (none)' THEN revenue
			
			WHEN source = 'direct / (none)' 
			AND LEAD(revenue) OVER(PARTITION BY client_id ORDER BY timestamp ROWS BETWEEN 0 Preceding AND 1 Following) > 0
				THEN revenue 
				-- LEAD(revenue) OVER(PARTITION BY client_id ORDER BY timestamp ROWS BETWEEN 0 Preceding AND 1 Following)

			WHEN source != 'direct / (none)' 
			AND LEAD(revenue) OVER(PARTITION BY client_id ORDER BY timestamp ROWS BETWEEN 0 Preceding AND 1 Following) > 0
			AND LEAD(source) OVER(PARTITION BY client_id ORDER BY timestamp ROWS BETWEEN 0 Preceding AND 1 Following) = 'direct / (none)'
				THEN  LEAD(revenue) OVER(PARTITION BY client_id ORDER BY timestamp ROWS BETWEEN 0 Preceding AND 1 Following) 
			ELSE 0 
			END 
		ELSE 0 END last_non_direct,


-- Модель Первый Клик 
-- для первой сессии, первой записи в партиции client_id
    CASE WHEN rn_client_id = 1 THEN SUM(revenue) OVER(PARTITION BY client_id) ELSE 0 END as first_click,
	revenue, rn_client_id
	FROM get_raw_data
	--GROUP BY source,  timestamp, session_id, client_id, revenue

	ORDER BY client_id, timestamp
) t 
GROUP BY session_id
-- ORDER BY revenue DESC
-- HAVING sum(revenue) != sum(last_non_direct) 
