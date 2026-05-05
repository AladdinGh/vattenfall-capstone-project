DESCRIBE TABLE vattenfall_dev.raw.bronze_market_prices;
DESCRIBE TABLE vattenfall_dev.raw.bronze_weather;
DESCRIBE TABLE vattenfall_dev.raw.bronze_grid_events;

SELECT * FROM vattenfall_dev.raw.bronze_market_prices LIMIT 20;
SELECT * FROM vattenfall_dev.raw.bronze_weather LIMIT 20;
SELECT * FROM vattenfall_dev.raw.bronze_grid_events LIMIT 20;