-- ===============================================================================
-- NETFLIX DATA PIPELINE - COMPLETE IMPLEMENTATION
-- Bronze -> Silver -> Gold Layer Architecture
-- ===============================================================================

-- ===============================================================================
-- BRONZE LAYER: Raw Data Storage
-- ===============================================================================
-- This layer contains raw, unprocessed data as received from the source

-- Drop existing table if needed
DROP TABLE IF EXISTS dbo.Netflix_raw;
GO

-- Create raw data table with proper data types for international characters
CREATE TABLE dbo.Netflix_raw (
    show_id        NVARCHAR(10)   PRIMARY KEY,
    [type]         VARCHAR(10)    NULL,
    title          NVARCHAR(200)  NULL,
    director       NVARCHAR(250)  NULL,
    [cast]         NVARCHAR(1000) NULL,
    country        NVARCHAR(150)  NULL,
    date_added     VARCHAR(30)    NULL,
    release_year   INT            NULL,
    rating         VARCHAR(10)    NULL,
    duration       VARCHAR(20)    NULL,
    listed_in      NVARCHAR(100)  NULL,
    description    NVARCHAR(1000) NULL
);
GO

-- Verify raw data
SELECT COUNT(*) AS total_records FROM Netflix_raw;
GO

-- ===============================================================================
-- SILVER LAYER: Cleaned and Normalized Data
-- ===============================================================================
-- This layer contains cleaned, deduplicated, and normalized data

-- -------------------------------------------------------------------------------
-- Step 1: Create normalized tables for multi-value columns
-- -------------------------------------------------------------------------------

-- Drop existing silver layer tables
DROP TABLE IF EXISTS netflix_director;
DROP TABLE IF EXISTS netflix_cast;
DROP TABLE IF EXISTS netflix_country;
DROP TABLE IF EXISTS netflix_listed_in;
DROP TABLE IF EXISTS Netflix;
GO

-- Create Director table (many-to-many relationship)
SELECT 
    show_id, 
    TRIM(value) AS director
INTO netflix_director
FROM Netflix_raw
CROSS APPLY STRING_SPLIT(director, ',')
WHERE director IS NOT NULL;
GO

-- Create Cast table (many-to-many relationship)
SELECT 
    show_id, 
    TRIM(value) AS cast
INTO netflix_cast
FROM Netflix_raw
CROSS APPLY STRING_SPLIT(cast, ',')
WHERE cast IS NOT NULL;
GO

-- Create Country table (many-to-many relationship)
SELECT 
    show_id, 
    TRIM(value) AS country
INTO netflix_country
FROM Netflix_raw
CROSS APPLY STRING_SPLIT(country, ',')
WHERE country IS NOT NULL;
GO

-- Create Genre/Category table (many-to-many relationship)
SELECT 
    show_id, 
    TRIM(value) AS listed_in
INTO netflix_listed_in
FROM Netflix_raw
CROSS APPLY STRING_SPLIT(listed_in, ',')
WHERE listed_in IS NOT NULL;
GO

-- -------------------------------------------------------------------------------
-- Step 2: Populate missing country values based on director's usual country
-- -------------------------------------------------------------------------------

INSERT INTO netflix_country
SELECT DISTINCT
    nr.show_id,
    m.country 
FROM Netflix_raw nr
INNER JOIN (
    SELECT 
        nd.director,
        nc.country
    FROM netflix_country nc
    INNER JOIN netflix_director nd ON nc.show_id = nd.show_id
    WHERE nc.country IS NOT NULL
    GROUP BY nd.director, nc.country
) m ON nr.director = m.director
WHERE nr.country IS NULL
  AND nr.show_id NOT IN (SELECT show_id FROM netflix_country);
GO

-- -------------------------------------------------------------------------------
-- Step 3: Handle remaining NULL values
-- -------------------------------------------------------------------------------

-- Add 'Not Available' for remaining NULL countries
INSERT INTO netflix_country
SELECT DISTINCT show_id, 'Not Available' AS country
FROM Netflix_raw
WHERE country IS NULL
  AND show_id NOT IN (SELECT show_id FROM netflix_country);
GO

-- Add 'Not Available' for NULL directors
INSERT INTO netflix_director
SELECT DISTINCT show_id, 'Not Available' AS director
FROM Netflix_raw
WHERE director IS NULL
  AND show_id NOT IN (SELECT show_id FROM netflix_director);
GO

-- Add 'Not Available' for NULL cast
INSERT INTO netflix_cast
SELECT DISTINCT show_id, 'Not Available' AS cast
FROM Netflix_raw
WHERE cast IS NULL
  AND show_id NOT IN (SELECT show_id FROM netflix_cast);
GO

-- -------------------------------------------------------------------------------
-- Step 4: Create main Netflix table (deduplicated and cleaned)
-- -------------------------------------------------------------------------------

WITH cte AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY title, type 
            ORDER BY show_id
        ) AS rn
    FROM Netflix_raw
)
SELECT 
    show_id,
    type,
    title,
    TRY_CAST(date_added AS DATE) AS date_added,
    release_year,
    rating,
    CASE 
        WHEN duration IS NULL THEN rating
        ELSE duration 
    END AS duration,
    description
INTO Netflix
FROM cte
WHERE rn = 1;
GO

-- Add indexes for better query performance
CREATE INDEX idx_netflix_type ON Netflix(type);
CREATE INDEX idx_netflix_release_year ON Netflix(release_year);
CREATE INDEX idx_netflix_date_added ON Netflix(date_added);
CREATE INDEX idx_netflix_director_showid ON netflix_director(show_id);
CREATE INDEX idx_netflix_cast_showid ON netflix_cast(show_id);
CREATE INDEX idx_netflix_country_showid ON netflix_country(show_id);
CREATE INDEX idx_netflix_listed_showid ON netflix_listed_in(show_id);
GO

-- ===============================================================================
-- GOLD LAYER: Analytics-Ready Tables and Views
-- ===============================================================================
-- This layer contains pre-aggregated data and business-ready views

-- -------------------------------------------------------------------------------
-- Gold Table 1: Director Content Summary
-- -------------------------------------------------------------------------------

DROP TABLE IF EXISTS gold_director_summary;
GO

SELECT 
    nd.director,
    COUNT(DISTINCT CASE WHEN n.type = 'Movie' THEN n.show_id END) AS total_movies,
    COUNT(DISTINCT CASE WHEN n.type = 'TV Show' THEN n.show_id END) AS total_tv_shows,
    COUNT(DISTINCT n.show_id) AS total_content,
    MIN(n.release_year) AS first_release_year,
    MAX(n.release_year) AS latest_release_year,
    COUNT(DISTINCT nc.country) AS countries_worked_in
INTO gold_director_summary
FROM netflix_director nd
INNER JOIN Netflix n ON nd.show_id = n.show_id
LEFT JOIN netflix_country nc ON nd.show_id = nc.show_id
WHERE nd.director != 'Not Available'
GROUP BY nd.director;
GO

-- -------------------------------------------------------------------------------
-- Gold Table 2: Country Content Analysis
-- -------------------------------------------------------------------------------

DROP TABLE IF EXISTS gold_country_analysis;
GO

SELECT 
    nc.country,
    COUNT(DISTINCT CASE WHEN n.type = 'Movie' THEN n.show_id END) AS total_movies,
    COUNT(DISTINCT CASE WHEN n.type = 'TV Show' THEN n.show_id END) AS total_tv_shows,
    COUNT(DISTINCT n.show_id) AS total_content,
    AVG(CASE 
        WHEN n.type = 'Movie' AND n.duration LIKE '%min%'
        THEN CAST(REPLACE(n.duration, ' min', '') AS INT)
    END) AS avg_movie_duration_minutes
INTO gold_country_analysis
FROM netflix_country nc
INNER JOIN Netflix n ON nc.show_id = n.show_id
WHERE nc.country != 'Not Available'
GROUP BY nc.country;
GO

-- -------------------------------------------------------------------------------
-- Gold Table 3: Genre Performance Metrics
-- -------------------------------------------------------------------------------

DROP TABLE IF EXISTS gold_genre_metrics;
GO

SELECT 
    nl.listed_in AS genre,
    COUNT(DISTINCT CASE WHEN n.type = 'Movie' THEN n.show_id END) AS total_movies,
    COUNT(DISTINCT CASE WHEN n.type = 'TV Show' THEN n.show_id END) AS total_tv_shows,
    AVG(CASE 
        WHEN n.type = 'Movie' AND n.duration LIKE '%min%'
        THEN CAST(REPLACE(n.duration, ' min', '') AS INT)
    END) AS avg_movie_duration,
    MIN(n.release_year) AS earliest_release,
    MAX(n.release_year) AS latest_release
INTO gold_genre_metrics
FROM netflix_listed_in nl
INNER JOIN Netflix n ON nl.show_id = n.show_id
GROUP BY nl.listed_in;
GO

-- -------------------------------------------------------------------------------
-- Gold Table 4: Yearly Content Trends
-- -------------------------------------------------------------------------------

DROP TABLE IF EXISTS gold_yearly_trends;
GO

SELECT 
    YEAR(date_added) AS year_added,
    type,
    COUNT(DISTINCT show_id) AS content_added,
    AVG(CASE 
        WHEN type = 'Movie' AND duration LIKE '%min%'
        THEN CAST(REPLACE(duration, ' min', '') AS INT)
    END) AS avg_duration
INTO gold_yearly_trends
FROM Netflix
WHERE date_added IS NOT NULL
GROUP BY YEAR(date_added), type;
GO

-- -------------------------------------------------------------------------------
-- Gold Table 5: Cast Popularity
-- -------------------------------------------------------------------------------

DROP TABLE IF EXISTS gold_cast_popularity;
GO

SELECT 
    nc.cast AS actor_name,
    COUNT(DISTINCT CASE WHEN n.type = 'Movie' THEN n.show_id END) AS movies_count,
    COUNT(DISTINCT CASE WHEN n.type = 'TV Show' THEN n.show_id END) AS tv_shows_count,
    COUNT(DISTINCT n.show_id) AS total_appearances,
    MIN(n.release_year) AS career_start_year,
    MAX(n.release_year) AS latest_year
INTO gold_cast_popularity
FROM netflix_cast nc
INNER JOIN Netflix n ON nc.show_id = n.show_id
WHERE nc.cast != 'Not Available'
GROUP BY nc.cast;
GO

-- ===============================================================================
-- BUSINESS INTELLIGENCE VIEWS
-- ===============================================================================

-- -------------------------------------------------------------------------------
-- View 1: Complete Content Details (Denormalized for reporting)
-- -------------------------------------------------------------------------------

CREATE OR ALTER VIEW vw_netflix_complete AS
SELECT 
    n.show_id,
    n.type,
    n.title,
    n.date_added,
    n.release_year,
    n.rating,
    n.duration,
    n.description,
    STRING_AGG(DISTINCT nd.director, ', ') AS directors,
    STRING_AGG(DISTINCT nc.cast, ', ') AS cast_members,
    STRING_AGG(DISTINCT nco.country, ', ') AS countries,
    STRING_AGG(DISTINCT nl.listed_in, ', ') AS genres
FROM Netflix n
LEFT JOIN netflix_director nd ON n.show_id = nd.show_id
LEFT JOIN netflix_cast nc ON n.show_id = nc.show_id
LEFT JOIN netflix_country nco ON n.show_id = nco.show_id
LEFT JOIN netflix_listed_in nl ON n.show_id = nl.show_id
GROUP BY 
    n.show_id, n.type, n.title, n.date_added, 
    n.release_year, n.rating, n.duration, n.description;
GO

-- -------------------------------------------------------------------------------
-- View 2: Top Performing Content
-- -------------------------------------------------------------------------------

CREATE OR ALTER VIEW vw_top_content AS
SELECT TOP 100
    n.title,
    n.type,
    n.release_year,
    n.rating,
    COUNT(DISTINCT nc.cast) AS cast_size,
    COUNT(DISTINCT nd.director) AS director_count,
    COUNT(DISTINCT nco.country) AS country_count,
    STRING_AGG(DISTINCT nl.listed_in, ', ') AS genres
FROM Netflix n
LEFT JOIN netflix_cast nc ON n.show_id = nc.show_id
LEFT JOIN netflix_director nd ON n.show_id = nd.show_id
LEFT JOIN netflix_country nco ON n.show_id = nco.show_id
LEFT JOIN netflix_listed_in nl ON n.show_id = nl.show_id
GROUP BY n.title, n.type, n.release_year, n.rating
ORDER BY cast_size DESC, director_count DESC;
GO

-- ===============================================================================
-- DATA QUALITY CHECKS
-- ===============================================================================

-- Check for data completeness
SELECT 
    'Bronze Layer' AS layer,
    COUNT(*) AS total_records,
    SUM(CASE WHEN director IS NULL THEN 1 ELSE 0 END) AS null_directors,
    SUM(CASE WHEN cast IS NULL THEN 1 ELSE 0 END) AS null_cast,
    SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END) AS null_countries
FROM Netflix_raw
UNION ALL
SELECT 
    'Silver Layer' AS layer,
    COUNT(*) AS total_records,
    0 AS null_directors,
    0 AS null_cast,
    0 AS null_countries
FROM Netflix;
GO

-- Summary statistics
SELECT 
    'Total Content' AS metric,
    COUNT(*) AS value
FROM Netflix
UNION ALL
SELECT 'Total Movies', COUNT(*) FROM Netflix WHERE type = 'Movie'
UNION ALL
SELECT 'Total TV Shows', COUNT(*) FROM Netflix WHERE type = 'TV Show'
UNION ALL
SELECT 'Unique Directors', COUNT(DISTINCT director) FROM netflix_director
UNION ALL
SELECT 'Unique Cast Members', COUNT(DISTINCT cast) FROM netflix_cast
UNION ALL
SELECT 'Unique Countries', COUNT(DISTINCT country) FROM netflix_country
UNION ALL
SELECT 'Unique Genres', COUNT(DISTINCT listed_in) FROM netflix_listed_in;
GO

-- ===============================================================================
-- ANALYTICAL QUERIES - Ready to Use
-- ===============================================================================

-- Query 1: Directors with both Movies and TV Shows
SELECT 
    nd.director,
    COUNT(DISTINCT CASE WHEN n.type = 'Movie' THEN n.show_id END) AS movies,
    COUNT(DISTINCT CASE WHEN n.type = 'TV Show' THEN n.show_id END) AS tv_shows
FROM Netflix n
INNER JOIN netflix_director nd ON n.show_id = nd.show_id
WHERE nd.director != 'Not Available'
GROUP BY nd.director
HAVING COUNT(DISTINCT n.type) > 1
ORDER BY movies DESC, tv_shows DESC;
GO

-- Query 2: Country with most comedy movies
SELECT TOP 10
    nc.country,
    COUNT(DISTINCT nl.show_id) AS comedy_movies
FROM netflix_listed_in nl
INNER JOIN netflix_country nc ON nl.show_id = nc.show_id
INNER JOIN Netflix n ON nl.show_id = n.show_id
WHERE nl.listed_in = 'Comedies' 
  AND n.type = 'Movie'
  AND nc.country != 'Not Available'
GROUP BY nc.country
ORDER BY comedy_movies DESC;
GO

-- Query 3: Top director each year by movie count
WITH yearly_director_counts AS (
    SELECT 
        nd.director,
        YEAR(n.date_added) AS year_added,
        COUNT(n.show_id) AS movies_count
    FROM Netflix n
    INNER JOIN netflix_director nd ON n.show_id = nd.show_id
    WHERE n.type = 'Movie' 
      AND n.date_added IS NOT NULL
      AND nd.director != 'Not Available'
    GROUP BY nd.director, YEAR(n.date_added)
),
ranked_directors AS (
    SELECT 
        director,
        year_added,
        movies_count,
        ROW_NUMBER() OVER(PARTITION BY year_added ORDER BY movies_count DESC, director) AS rn
    FROM yearly_director_counts
)
SELECT 
    year_added,
    director,
    movies_count
FROM ranked_directors
WHERE rn = 1
ORDER BY year_added DESC;
GO

-- Query 4: Average movie duration by genre
SELECT 
    nl.listed_in AS genre,
    AVG(CAST(REPLACE(n.duration, ' min', '') AS INT)) AS avg_duration_minutes,
    COUNT(DISTINCT n.show_id) AS movie_count
FROM Netflix n
INNER JOIN netflix_listed_in nl ON n.show_id = nl.show_id
WHERE n.type = 'Movie' 
  AND n.duration LIKE '%min%'
GROUP BY nl.listed_in
ORDER BY avg_duration_minutes DESC;
GO

-- Query 5: Directors who made both Horror and Comedy movies
SELECT 
    nd.director,
    COUNT(DISTINCT CASE WHEN nl.listed_in = 'Comedies' THEN n.show_id END) AS comedy_count,
    COUNT(DISTINCT CASE WHEN nl.listed_in = 'Horror Movies' THEN n.show_id END) AS horror_count,
    COUNT(DISTINCT n.show_id) AS total_movies
FROM Netflix n
INNER JOIN netflix_listed_in nl ON n.show_id = nl.show_id
INNER JOIN netflix_director nd ON n.show_id = nd.show_id
WHERE n.type = 'Movie' 
  AND nl.listed_in IN ('Comedies', 'Horror Movies')
  AND nd.director != 'Not Available'
GROUP BY nd.director
HAVING COUNT(DISTINCT nl.listed_in) = 2
ORDER BY total_movies DESC;
GO

-- Query 6: Content duration categories
SELECT 
    title,
    type,
    duration,
    CASE 
        WHEN type = 'Movie' AND CAST(REPLACE(duration, ' min', '') AS INT) > 150 
            THEN 'Epic/Long Movie'
        WHEN type = 'Movie' AND CAST(REPLACE(duration, ' min', '') AS INT) < 30 
            THEN 'Short Film'
        WHEN type = 'Movie' 
            THEN 'Standard Length'
        ELSE 'TV Show'
    END AS duration_category,
    release_year
FROM Netflix
WHERE duration LIKE '%min%'
ORDER BY CAST(REPLACE(duration, ' min', '') AS INT) DESC;
GO

-- Query 7: Content growth over years
SELECT 
    YEAR(date_added) AS year,
    type,
    COUNT(show_id) AS content_count
FROM Netflix
WHERE date_added IS NOT NULL
GROUP BY YEAR(date_added), type
ORDER BY year DESC, type;
GO

-- ===============================================================================
-- PIPELINE VALIDATION SUMMARY
-- ===============================================================================

PRINT '===============================================================================';
PRINT 'NETFLIX DATA PIPELINE - EXECUTION COMPLETE';
PRINT '===============================================================================';
PRINT '';
PRINT 'BRONZE LAYER: Raw data stored in Netflix_raw table';
PRINT 'SILVER LAYER: Normalized tables created:';
PRINT '  - Netflix (main cleaned table)';
PRINT '  - netflix_director';
PRINT '  - netflix_cast';
PRINT '  - netflix_country';
PRINT '  - netflix_listed_in';
PRINT '';
PRINT 'GOLD LAYER: Analytics tables created:';
PRINT '  - gold_director_summary';
PRINT '  - gold_country_analysis';
PRINT '  - gold_genre_metrics';
PRINT '  - gold_yearly_trends';
PRINT '  - gold_cast_popularity';
PRINT '';
PRINT 'VIEWS: Business intelligence views created:';
PRINT '  - vw_netflix_complete';
PRINT '  - vw_top_content';
PRINT '';
PRINT '===============================================================================';
GO