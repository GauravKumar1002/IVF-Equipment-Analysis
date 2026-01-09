CREATE DATABASE IF NOT EXISTS ivf_analysis;
USE ivf_analysis;

DROP TABLE IF EXISTS ivf_equipment;

CREATE TABLE ivf_equipment (
    record_id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE,
    lab_id VARCHAR(20),
    equipment_id VARCHAR(30),
    equipment_type VARCHAR(50),
    max_capacity_hrs DECIMAL(6,2),
    utilization_hrs DECIMAL(6,2),
    utilization_pct DECIMAL(5,2),
    idle_hrs DECIMAL(6,2),
    technical_downtime_hrs DECIMAL(6,2),
    planned_maintenance_hrs DECIMAL(6,2),
    workflow_delay_events INT,
    avg_delay_minutes DECIMAL(6,2),
    primary_procedure VARCHAR(100),
    redundancy_available VARCHAR(10),
    total_cases_day_lab INT
);

LOAD DATA INFILE 
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/ivf_equipment_utilization_2yrs.csv'
INTO TABLE ivf_equipment
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    @date,
    lab_id,
    equipment_id,
    equipment_type,
    @max_capacity_hrs,
    @utilization_hrs,
    @utilization_pct,
    @idle_hrs,
    @technical_downtime_hrs,
    @planned_maintenance_hrs,
    @workflow_delay_events,
    @avg_delay_minutes,
    primary_procedure,
    redundancy_available,
    @total_cases_day_lab
)
SET
    date = CASE 
              WHEN @date = '' THEN NULL
              ELSE STR_TO_DATE(@date, '%d-%m-%Y')
           END,
    max_capacity_hrs = NULLIF(@max_capacity_hrs, ''),
    utilization_hrs = NULLIF(@utilization_hrs, ''),
    utilization_pct = NULLIF(@utilization_pct, ''),
    idle_hrs = NULLIF(@idle_hrs, ''),
    technical_downtime_hrs = NULLIF(@technical_downtime_hrs, ''),
    planned_maintenance_hrs = NULLIF(@planned_maintenance_hrs, ''),
    workflow_delay_events = NULLIF(@workflow_delay_events, ''),
    avg_delay_minutes = NULLIF(@avg_delay_minutes, ''),
    total_cases_day_lab = NULLIF(@total_cases_day_lab, '');

SELECT COUNT(*) AS total_records FROM ivf_equipment;
SELECT * FROM ivf_equipment LIMIT 5;

DESCRIBE ivf_equipment;
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT equipment_id) AS total_equipment,
    COUNT(DISTINCT lab_id) AS total_labs
FROM ivf_equipment;

SELECT
    SUM(utilization_hrs IS NULL) AS missing_utilization,
    SUM(utilization_pct IS NULL) AS missing_util_pct,
    SUM(idle_hrs IS NULL) AS missing_idle,
    SUM(technical_downtime_hrs IS NULL) AS missing_tech_down
FROM ivf_equipment;

SELECT 
    equipment_type,
    ROUND(AVG(utilization_pct),2) AS avg_utilization
FROM ivf_equipment
GROUP BY equipment_type
ORDER BY avg_utilization DESC;

SELECT 
    equipment_type,
    ROUND(SUM(technical_downtime_hrs),2) AS total_tech_downtime
FROM ivf_equipment
GROUP BY equipment_type
ORDER BY total_tech_downtime DESC;

CREATE INDEX idx_dedup 
ON ivf_equipment (date, lab_id, equipment_id);

DELETE FROM ivf_equipment
WHERE record_id NOT IN (
    SELECT record_id FROM (
        SELECT MIN(record_id) AS record_id
        FROM ivf_equipment
        GROUP BY date, lab_id, equipment_id
    ) AS t
);

SELECT
    date, lab_id, equipment_id, COUNT(*) AS cnt
FROM ivf_equipment
GROUP BY date, lab_id, equipment_id
HAVING cnt > 1;

UPDATE ivf_equipment
SET 
technical_downtime_hrs = 0,
planned_maintenance_hrs = 0
WHERE technical_downtime_hrs IS NULL
OR planned_maintenance_hrs IS NULL;

DELETE FROM ivf_equipment
WHERE utilization_hrs < 0
OR max_capacity_hrs <= 0;

ALTER TABLE ivf_equipment
ADD utilization_efficiency DECIMAL(5,2);

UPDATE ivf_equipment
SET utilization_efficiency =
ROUND((utilization_hrs / max_capacity_hrs) * 100, 2);

ALTER TABLE ivf_equipment
ADD year INT,
ADD month INT;

UPDATE ivf_equipment
SET
year = YEAR(date),
month = MONTH(date);

SELECT DISTINCT redundancy_available FROM ivf_equipment;

ALTER TABLE ivf_equipment
ADD redundancy_flag INT;

UPDATE ivf_equipment
SET redundancy_flag =
CASE 
    WHEN redundancy_available = 'Yes' THEN 1
    ELSE 0
END;

SELECT
    year,
    ROUND(AVG(utilization_pct),2) AS avg_utilization
FROM ivf_equipment
GROUP BY year
ORDER BY year;

SELECT
    lab_id,
    ROUND(AVG(total_cases_day_lab),2) AS avg_cases_per_day
FROM ivf_equipment
GROUP BY lab_id
ORDER BY avg_cases_per_day DESC;
