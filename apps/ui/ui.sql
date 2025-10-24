SELECT
  s.symbol,
  p.price AS last_price,
  pr.y_hat AS pred_1d,
  pr.method
FROM (SELECT DISTINCT symbol FROM prices) s
LEFT JOIN prices_latest      p  USING(symbol)
LEFT JOIN predictions_latest pr USING(symbol)
ORDER BY s.symbol, pr.method;