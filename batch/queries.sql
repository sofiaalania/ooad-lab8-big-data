SELECT COUNT(*) AS total_events
FROM lab8_payments.payment_events;



CREATE OR REPLACE VIEW lab8_payments.payments AS
WITH unique_events AS (
    SELECT DISTINCT *
    FROM lab8_payments.payment_events
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY invoiceid
            ORDER BY
                modifieddate DESC,
                receivedat DESC,
                status DESC,
                reference DESC
        ) AS rn
    FROM unique_events
)
SELECT
    invoiceid,
    status,
    failurereason,
    amount,
    finalamount,
    ccy,
    reference,
    createddate,
    modifieddate,
    receivedat,
    merchantid,
    merchantname,
    mcc,
    customerid,
    paymentmethod,
    device,
    dt
FROM ranked
WHERE rn = 1;

SELECT COUNT(*) AS unique_payments
FROM lab8_payments.payments;




SELECT
    merchantname,
    CASE ccy
        WHEN 980 THEN 'UAH'
        WHEN 840 THEN 'USD'
        WHEN 978 THEN 'EUR'
        ELSE CAST(ccy AS VARCHAR)
    END AS currency,
    COUNT(*) AS successful_payments,
    ROUND(SUM(finalamount) / 100.0, 2) AS turnover
FROM lab8_payments.payments
WHERE status = 'success'
GROUP BY merchantname, ccy
ORDER BY turnover DESC;



SELECT
    paymentmethod,
    device,
    COUNT(*) AS total_payments,
    COUNT_IF(status = 'failure') AS failed_payments,
    ROUND(
        100.0 * COUNT_IF(status = 'failure') / COUNT(*),
        2
    ) AS failure_rate_percent
FROM lab8_payments.payments
GROUP BY paymentmethod, device
ORDER BY failure_rate_percent DESC;




SELECT
    HOUR(FROM_ISO8601_TIMESTAMP(createddate)) AS hour_of_day,
    COUNT(*) AS payments_count
FROM lab8_payments.payments
GROUP BY 1
ORDER BY hour_of_day;




WITH delays AS (
    SELECT
        DATE_DIFF(
            'second',
            FROM_ISO8601_TIMESTAMP(modifieddate),
            FROM_ISO8601_TIMESTAMP(receivedat)
        ) AS delay_seconds
    FROM lab8_payments.payment_events
)
SELECT
    COUNT(*) AS total_events,
    COUNT_IF(delay_seconds > 60) AS late_events,
    ROUND(
        100.0 * COUNT_IF(delay_seconds > 60) / COUNT(*),
        2
    ) AS late_percent
FROM delays;




CREATE TABLE lab8_payments.payments_parquet
WITH (
    format = 'PARQUET',
    external_location = 's3://ooad-lab8-s/curated/payments/',
    partitioned_by = ARRAY['dt']
) AS
SELECT
    invoiceid,
    status,
    failurereason,
    amount,
    finalamount,
    ccy,
    reference,
    createddate,
    modifieddate,
    receivedat,
    merchantid,
    merchantname,
    mcc,
    customerid,
    paymentmethod,
    device,
    dt
FROM lab8_payments.payments;



SELECT
    merchantname,
    CASE ccy
        WHEN 980 THEN 'UAH'
        WHEN 840 THEN 'USD'
        WHEN 978 THEN 'EUR'
        ELSE CAST(ccy AS VARCHAR)
    END AS currency,
    COUNT(*) AS successful_payments,
    ROUND(SUM(finalamount) / 100.0, 2) AS turnover
FROM lab8_payments.payments_parquet
WHERE status = 'success'
GROUP BY merchantname, ccy
ORDER BY turnover DESC;


SELECT
    paymentmethod,
    device,
    COUNT(*) AS total_payments,
    COUNT_IF(status = 'failure') AS failed_payments,
    ROUND(
        100.0 * COUNT_IF(status = 'failure') / COUNT(*),
        2
    ) AS failure_rate_percent
FROM lab8_payments.payments_parquet
GROUP BY paymentmethod, device
ORDER BY failure_rate_percent DESC;



SELECT
    HOUR(FROM_ISO8601_TIMESTAMP(createddate)) AS hour_of_day,
    COUNT(*) AS payments_count
FROM lab8_payments.payments_parquet
GROUP BY 1
ORDER BY hour_of_day;
