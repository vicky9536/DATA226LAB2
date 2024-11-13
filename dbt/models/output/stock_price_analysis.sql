WITH staged_data AS (
    SELECT *
    FROM {{ ref('price90days_all_symbols') }}
),

-- 50-day Moving Average
moving_averages AS (
    SELECT
        date,
        symbol,
        close,
        AVG(close) OVER (
            PARTITION BY symbol 
            ORDER BY date 
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS ma_50
    FROM staged_data
),

-- 14-day RSI
price_changes AS (
    SELECT
        date,
        symbol,
        close,
        LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close,
        COALESCE(close - LAG(close) OVER (PARTITION BY symbol ORDER BY date), 0) AS price_change
    FROM staged_data
),
gains_and_losses AS (
    SELECT
        date,
        symbol,
        close,
        CASE WHEN price_change > 0 THEN price_change ELSE 0 END AS gain,
        CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END AS loss
    FROM price_changes
),
average_gain_loss AS (
    SELECT
        date,
        symbol,
        close,
        AVG(gain) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain,
        AVG(loss) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss
    FROM gains_and_losses
),
rsi_14_calculation AS (
    SELECT
        date,
        symbol,
        close,
        ma_50,
        100 - (100 / (1 + (avg_gain / NULLIF(avg_loss, 0)))) AS rsi_14
    FROM average_gain_loss
    JOIN moving_averages USING (date, symbol, close)
),

-- 10-day Momentum
momentum_calculation AS (
    SELECT
        date,
        symbol,
        close,
        ma_50,
        rsi_14,
        LAG(close, 10) OVER (PARTITION BY symbol ORDER BY date) AS price_10_days_ago,
        ((close - LAG(close, 10) OVER (PARTITION BY symbol ORDER BY date)) / 
        NULLIF(LAG(close, 10) OVER (PARTITION BY symbol ORDER BY date), 0)) * 100 AS momentum_10
    FROM rsi_14_calculation
)

SELECT
    date,
    symbol,
    close,
    ma_50,
    rsi_14,
    momentum_10
FROM momentum_calculation

