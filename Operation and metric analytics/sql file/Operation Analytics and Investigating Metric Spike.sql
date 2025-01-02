CREATE DATABASE operations_analytics;
USE operations_analytics;
CREATE TABLE job_data (
    ds DATE NOT NULL,
    job_id INT NOT NULL,
    actor_id INT NOT NULL,
    event VARCHAR(50),
    language VARCHAR(50),
    time_spent INT,
    org VARCHAR(50)
);
LOAD DATA INFILE 'D:/job_data.csv'
INTO TABLE job_data
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@ds, job_id, actor_id, event, language, time_spent, org)
SET ds = STR_TO_DATE(@ds, '%m/%d/%Y');

select * from job_data

SELECT 
    DATE(ds) AS review_date, 
    HOUR(TIMESTAMP(ds)) AS review_hour, 
    COUNT(job_id) AS jobs_reviewed 
FROM job_data
WHERE ds BETWEEN '2020-11-01' AND '2020-11-30'
GROUP BY review_date, review_hour
ORDER BY review_date, review_hour;

WITH daily_throughput AS (
    SELECT 
        DATE(ds) AS review_date,
        COUNT(*) AS daily_events
    FROM job_data
    GROUP BY review_date
)
SELECT 
    review_date,
    daily_events,
    ROUND(AVG(daily_events) OVER (ORDER BY review_date ROWS 6 PRECEDING), 2) AS rolling_avg_7_days
FROM daily_throughput
ORDER BY review_date;

WITH language_counts AS (
    SELECT 
        language,
        COUNT(*) AS language_count
    FROM job_data
    WHERE STR_TO_DATE(ds, '%Y-%m-%d') BETWEEN '2020-11-01' AND '2020-11-30'
    GROUP BY language
),
total_count AS (
    SELECT 
        SUM(language_count) AS total_jobs
    FROM language_counts
)
SELECT 
    lc.language,
    lc.language_count,
    ROUND((lc.language_count / tc.total_jobs) * 100, 2) AS percentage_share
FROM 
    language_counts lc,
    total_count tc
ORDER BY 
    percentage_share DESC;


SELECT * 
FROM 
(
 SELECT *, ROW_NUMBER()OVER(PARTITION BY job_id) AS row_num
 FROM job_data
 ) a 
WHERE row_num>1;

CREATE TABLE users (
    user_id INT PRIMARY KEY,
    created_at VARCHAR(255),
    company_id int,
    `language` VARCHAR(255),
    activated_at VARCHAR(255),
    state varchar(255)
);

CREATE TABLE events1 (
    user_id INT,                 
    occurred_at DATETIME,        
    event_type VARCHAR(255),     
    event_name VARCHAR(255),     
    location VARCHAR(255),       
    device VARCHAR(255),         
    user_type VARCHAR(255)       
);

ALTER TABLE events1 MODIFY COLUMN occurred_at VARCHAR(255);



CREATE TABLE user_events (
    user_id INT,                 
    occurred_at DATETIME,        
    action VARCHAR(255),         
    user_type VARCHAR(255)       
);
ALTER TABLE user_events MODIFY COLUMN occurred_at VARCHAR(255);

LOAD DATA INFILE 'D:\users.csv'
INTO TABLE users
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS

select * from users


LOAD DATA INFILE 'D:\events.csv'
INTO TABLE 	events1
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS

LOAD DATA INFILE 'D:/email_events.csv'
INTO TABLE user_events
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


SELECT 
extract(week from occurred_at) as week_number,
 count(distinct user_id) as number_of_users
 FROM 
operations_analytics.
 group by 
week_number;

SELECT 
    EXTRACT(YEAR FROM STR_TO_DATE(activated_at, '%Y-%m-%d')) AS year,
    EXTRACT(WEEK FROM STR_TO_DATE(activated_at, '%Y-%m-%d')) AS no_of_weeks,
    COUNT(DISTINCT user_id) AS no_of_active_users,
    SUM(COUNT(DISTINCT user_id)) OVER (ORDER BY 
        EXTRACT(YEAR FROM STR_TO_DATE(activated_at, '%Y-%m-%d')),
        EXTRACT(WEEK FROM STR_TO_DATE(activated_at, '%Y-%m-%d')) 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumm_active_users
FROM users
GROUP BY year, no_of_weeks
ORDER BY year, no_of_weeks;

SELECT 
    WEEK(occurred_at) AS week_number,
    COUNT(DISTINCT user_id) AS number_of_users
FROM 
    OPERATIONS_analytics.events1
GROUP BY 
    week_number;




WITH signup_cohort AS (
    SELECT 
        user_id, 
        DATE(STR_TO_DATE(created_at, '%Y-%m-%d')) AS cohort_date  -- Adjust format if necessary
    FROM users
),
weekly_activity AS (
    SELECT 
        user_id, 
        YEARWEEK(STR_TO_DATE(occurred_at, '%Y-%m-%d')) AS activity_week  -- Adjust format if necessary
    FROM events1
    GROUP BY user_id, activity_week
)
SELECT 
    cohort_date, 
    activity_week, 
    COUNT(DISTINCT wa.user_id) AS retained_users 
FROM signup_cohort sc
JOIN weekly_activity wa ON sc.user_id = wa.user_id
GROUP BY cohort_date, activity_week;

SELECT 
    YEAR(STR_TO_DATE(occurred_at, '%Y-%m-%d %H:%i:%s')) AS year_num,
    WEEK(STR_TO_DATE(occurred_at, '%Y-%m-%d %H:%i:%s')) AS week_num,
    device,
    COUNT(DISTINCT user_id) AS no_of_users_engaged
FROM 
    operations_analytics.events1
WHERE 
    event_type = 'engagement' 
    AND occurred_at IS NOT NULL 
    AND occurred_at != '' 
    AND STR_TO_DATE(occurred_at, '%Y-%m-%d %H:%i:%s') IS NOT NULL  -- Ensure valid date
GROUP BY 
    year_num, week_num, device
ORDER BY 
    year_num, week_num, device;

UPDATE events1
SET occurred_at = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i')
WHERE occurred_at NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$';


SET SQL_SAFE_UPDATES = 0;

SELECT 
    YEAR(occurred_at) AS year,
    WEEK(occurred_at) AS week,
    device,
    COUNT(DISTINCT event_name) AS weekly_engagement
FROM 
    events1
WHERE 
    occurred_at IS NOT NULL
GROUP BY 
    YEAR(occurred_at), WEEK(occurred_at), device
ORDER BY 
    year, week, device;


UPDATE user_events
SET occurred_at = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i')
WHERE occurred_at REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4} [0-9]{2}:[0-9]{2}$';

UPDATE user_events
SET action = TRIM(LOWER(action));

SELECT 
    COUNT(CASE WHEN action = 'sent' THEN 1 END) AS emails_sent,
    COUNT(CASE WHEN action = 'opened' THEN 1 END) AS emails_opened,
    COUNT(CASE WHEN action = 'clicked' THEN 1 END) AS emails_clicked
FROM user_events;




SELECT
 100.0*SUM(CASE when email_cat = 'email_opened' then 1 else 0 end)/SUM(CASE when 
email_cat = 'email_sent' then 1 else 0 end) as email_opening_rate,
 100.0*SUM(CASE when email_cat = 'email_clicked' then 1 else 0 end)/SUM(CASE when 
email_cat = 'email_sent' then 1 else 0 end) as email_clicking_rate
 FROM 
(
 SELECT 
*,
 CASE 
WHEN action in ('sent_weekly_digest','sent_reengagement_email')
 then 'email_sent'
 WHEN action in ('email_open')
 then 'email_opened'
 WHEN action in ('email_clickthrough')
 then 'email_clicked'
 end as email_cat
 from operations_analytics.user_events
 ) a;



             






