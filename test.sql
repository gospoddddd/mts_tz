-- Создание базы данных и схемы
CREATE DATABASE home;

CREATE SCHEMA work;

-- Создание таблиц
CREATE TABLE work.metro_lines (
    id SERIAL PRIMARY KEY,
    city VARCHAR(255),
    line_name VARCHAR(255)
);

CREATE TABLE work.metro_stations (
    id SERIAL PRIMARY KEY,
    city VARCHAR(255),
    line_name VARCHAR(255),
    station_name VARCHAR(255)
);

-- Создание витрины данных
CREATE VIEW work.metro_metrics AS
SELECT
    l.city,
    l.line_name AS metro_line,
    EXISTS (
        SELECT 1
        FROM work.metro_stations s1
        JOIN work.metro_stations s2 ON s1.station_name = s2.station_name AND s1.line_name <> s2.line_name
        WHERE s1.city = l.city AND s1.line_name = l.line_name
    ) AS repeat_line,
    ROUND((COUNT(s.id)::FLOAT / SUM(COUNT(s.id)) OVER (PARTITION BY l.city))::numeric, 2) AS proportion


FROM
    work.metro_lines l
JOIN
    work.metro_stations s ON l.city = s.city AND l.line_name = s.line_name
GROUP BY
    l.city, l.line_name;
select * from work.metro_metrics;
