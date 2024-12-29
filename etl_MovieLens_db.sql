--- Použitie roly
USE ROLE TRAINING_ROLE;
--- Vytvorenie a použitie skladiska 
CREATE WAREHOUSE IF NOT EXISTS HIPPO_WH;
USE WAREHOUSE HIPPO_WH;
--- Vytvorenie a použitie databázy
CREATE DATABASE IF NOT EXISTS HIPPO_MovieLens;
USE HIPPO_MovieLens;
--- Vytvorenie a použitie schémy
CREATE SCHEMA HIPPO_MovieLens.staging;
USE SCHEMA HIPPO_MovieLens.staging;
--- Vytvorenie stagu 
CREATE OR REPLACE STAGE Hippo_stage;


--- 1. Vytváranie tabuliek na vloženie csv súborv.
CREATE TABLE age_group_staging(
    group_id INT PRIMARY KEY,
    name VARCHAR(45)
);

CREATE TABLE occupations_staging (
    occupation_id INT PRIMARY KEY,
    name VARCHAR(45)
);

CREATE TABLE users_staging (
    user_id INT PRIMARY KEY,
    age INT,
    gender CHAR(1),
    occupation_id INT,
    zipcode VARCHAR(255),
    FOREIGN KEY (age) REFERENCES age_group_staging(group_id),
    FOREIGN KEY (occupation_id) REFERENCES occupations_staging(occupation_id)
);

CREATE TABLE movies_staging (
    movie_id INT PRIMARY KEY,
    title VARCHAR(255),
    release_year CHAR(4)
);

CREATE TABLE ratings_staging (
    rating_id INT PRIMARY KEY,
    user_id INT,
    movie_id INT,
    rating INT,
    rated_at DATETIME,
    FOREIGN KEY (user_id) REFERENCES users_staging(user_id),
    FOREIGN KEY (movie_id) REFERENCES movies_staging(movie_id)
);

CREATE TABLE genres_staging (
    genre_id INT PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE genres_movies_staging(
    main_id INT PRIMARY KEY,
    movie_id INT,
    genre_id INT,
    FOREIGN KEY (movie_id) REFERENCES movies_staging(movie_id),
    FOREIGN KEY (genre_id) REFERENCES genres_staging(genre_id)
);

CREATE TABLE tags_staging(
    tag_id INT PRIMARY KEY,
    user_id INT,
    movie_id INT,
    name VARCHAR(255),
    created_at DATETIME,
    FOREIGN KEY (user_id) REFERENCES users_staging(user_id),
    FOREIGN KEY (movie_id) REFERENCES movies_staging(movie_id)
);

  --- 2. Importovanie údajov z csv súborov do tabuliek 
COPY INTO age_group_staging
FROM @Hippo_stage/age_group.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO occupations_staging
FROM @Hippo_stage/occupations.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO users_staging
FROM @Hippo_stage/users.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO movies_staging
FROM @Hippo_stage/movies.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO ratings_staging
FROM @Hippo_stage/ratings.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO genres_staging
FROM @Hippo_stage/genres.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO genres_movies_staging
FROM @Hippo_stage/genres_movies.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO tags_staging
FROM @Hippo_stage/tags.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

--- 3. Vytváranie dimenzionálnych tabuliek 
CREATE OR REPLACE TABLE DIM_USERS AS
SELECT DISTINCT
    u.user_id AS dim_userID,
    g.name AS age_group,
    o.name AS occupation,
    u.gender
FROM users_staging u
LEFT JOIN occupations_staging o 
    ON u.occupation_id = o.occupation_id
LEFT JOIN age_group_staging g 
    ON (
        (g.name = 'Under 18' AND u.age < 18) OR
        (g.name = '18-24' AND u.age BETWEEN 18 AND 24) OR
        (g.name = '25-34' AND u.age BETWEEN 25 AND 34) OR
        (g.name = '35-44' AND u.age BETWEEN 35 AND 44) OR
        (g.name = '45-49' AND u.age BETWEEN 45 AND 49) OR
        (g.name = '50-55' AND u.age BETWEEN 50 AND 55) OR
        (g.name = '56+' AND u.age >= 56))
ORDER BY u.user_id;

CREATE TABLE DIM_TAGS AS 
SELECT DISTINCT
    t.tag_id AS dim_tagID,
    u.user_id AS dim_userID,
    m.movie_id AS dim_movieID,
    t.name AS description,
    CAST(created_at AS DATE) AS date, 
    CAST(created_at AS TIME) AS time, 
FROM tags_staging t
LEFT JOIN users_staging u
    ON t.user_id = u.user_id
LEFT JOIN movies_staging m 
    ON t.movie_id = m.movie_id;

CREATE TABLE DIM_DATE AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(rated_at AS DATE)) AS dim_dateID, 
    CAST(rated_at AS DATE) AS date,                    
    DATE_PART(day, rated_at) AS day,                   
    DATE_PART(dayofweek, rated_at) + 1 AS dayOfWeek,    
    CASE DATE_PART(dayofweek, rated_at) + 1
        WHEN 1 THEN 'Pondelok'
        WHEN 2 THEN 'Utorok'
        WHEN 3 THEN 'Streda'
        WHEN 4 THEN 'Štvrtok'
        WHEN 5 THEN 'Piatok'
        WHEN 6 THEN 'Sobota'
        WHEN 7 THEN 'Nedeľa'
    END AS DayOfWeek_String,
    DATE_PART(week, rated_at) AS week,
    DATE_PART(month, rated_at) AS month,              
    CASE DATE_PART(month, rated_at)
        WHEN 1 THEN 'Január'
        WHEN 2 THEN 'Február'
        WHEN 3 THEN 'Marec'
        WHEN 4 THEN 'Apríl'
        WHEN 5 THEN 'Máj'
        WHEN 6 THEN 'Jún'
        WHEN 7 THEN 'Júl'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'Október'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS month_String,
    DATE_PART(year, rated_at) AS year,                              
FROM RATINGS_STAGING
GROUP BY CAST(rated_at AS DATE), 
         DATE_PART(day, rated_at), 
         DATE_PART(dayofweek, rated_at),
         DATE_PART(week, rated_at),
         DATE_PART(month, rated_at), 
         DATE_PART(year, rated_at); 
         



CREATE TABLE DIM_GENRES AS
SELECT DISTINCT
    genre_id AS dim_genreID,
    name 
FROM genres_staging;

CREATE TABLE DIM_MOVIES AS 
SELECT DISTINCT
    movie_id AS dim_movieID,
    title,
    release_year
FROM movies_staging;

    
--- 4. Vytvorenie faktovej tabuľky 
CREATE TABLE FACT_RATINGS AS
SELECT DISTINCT
    r.rating_id AS ratingID, 
    r.rating, 
    r.rated_at AS timestamp, 
    m.dim_movieID AS movieID,
    u.dim_userID AS userID,
    g.dim_genreID AS genreID,
    COALESCE(ta.dim_tagID,'-1') AS tagID,
    d.dim_dateID AS dateID              
            
FROM ratings_staging r
LEFT JOIN DIM_MOVIES m ON r.movie_id = m.dim_movieID 
LEFT JOIN DIM_USERS u ON r.user_id = u.dim_userID 
LEFT JOIN DIM_DATE d ON CAST(r.rated_at AS DATE) = d.date 
LEFT JOIN genres_movies_staging gm ON r.movie_id = gm.movie_id 
LEFT JOIN DIM_GENRES g ON gm.genre_id = g.dim_genreID
LEFT JOIN DIM_TAGS ta ON r.movie_id = ta.dim_movieID
;

-- 5. Vymazanie tabuliek určené na nahranie csv súborov.
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS tags_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS users_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS genres_movies_staging;
DROP TABLE IF EXISTS age_group_staging;
