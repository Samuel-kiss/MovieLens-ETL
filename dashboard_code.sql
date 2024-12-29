
-- Graf 1: Najviac hodnotené filmy (top 10 fimov)
SELECT 
    m.title AS movie_title,
    COUNT(f.ratingID) AS total_ratings
FROM FACT_RATINGS f
JOIN DIM_MOVIES m ON f.movieID = m.dim_movieID
GROUP BY m.title
ORDER BY total_ratings DESC
LIMIT 10;

-- Graf 2: Najviac hodnotené filmy mužským pohlavím (top 10 filmov)
SELECT
    u.gender AS Gender,
    m.title AS Movie,
    COUNT(f.movieID) AS RatingCount
FROM FACT_RATINGS f
JOIN DIM_USERS u ON f.userID = u.dim_userID
JOIN DIM_MOVIES m ON f.movieID = m.dim_movieID
WHERE u.gender = 'M'
GROUP BY u.gender,m.title
ORDER BY RatingCount DESC
LIMIT 10;


-- Graf 3: Najviac hodnotené filmy ženským pohlavím (top 10 filmov)
SELECT
    u.gender AS Gender,
    m.title AS Movie,
    COUNT(f.movieID) AS RatingCount
FROM FACT_RATINGS f
JOIN DIM_USERS u ON f.userID = u.dim_userID
JOIN DIM_MOVIES m ON f.movieID = m.dim_movieID
WHERE u.gender = 'F'
GROUP BY u.gender,m.title
ORDER BY RatingCount DESC
LIMIT 10;

-- Graf 4: Počet hodnotení v priebehu dňa 
SELECT 
    CAST(f.timestamp AS TIME) AS time,
    u.age_group AS age_group,
    COUNT(f.ratingID) AS total_ratings
FROM FACT_RATINGS f
JOIN DIM_USERS u ON f.userID = u.dim_userID
GROUP BY f.timestamp, u.age_group
ORDER BY total_ratings DESC;

-- Graf 5: Celková aktivitu počas dní v týždni
SELECT 
    d.dayOfWeek_string AS day,
    COUNT(f.ratingID) AS total_ratings
FROM FACT_RATINGS f
JOIN DIM_DATE d ON f.dateID = d.dim_dateID
GROUP BY d.dayOfWeek_string
ORDER BY total_ratings DESC;

-- Graf 6: Počet hodnotení podľa povolaní
SELECT 
    u.occupation AS occupation,
    COUNT(f.ratingID) AS total_ratings
FROM FACT_RATINGS f
JOIN DIM_USERS u ON f.userID = u.dim_userID
GROUP BY u.occupation
ORDER BY total_ratings DESC
LIMIT 10;

-- Graf 7: Rozdelenie hodnotení podľa pohlavia používateľov
SELECT 
    u.gender,
    COUNT(f.ratingID) AS total_ratings
FROM FACT_RATINGS f
JOIN DIM_USERS u ON f.userID = u.dim_userID
GROUP BY u.gender;

-- Graf 8: Aktivita používateľov podľa vekových kategórií
SELECT 
    u.age_group AS age_group,
    COUNT(f.ratingID) AS total_ratings
FROM FACT_RATINGS f
JOIN DIM_USERS u ON f.userID = u.dim_userID
GROUP BY u.age_group
ORDER BY total_ratings DESC;
