# **ETL proces datasetu MovieLens**

Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z **MovieLens** datasetu. Projekt sa zameriava na preskúmanie recenzií používateľov a ich filmových preferencií na základe hodnotení filmov a demografických údajov používateľov. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu kľúčových metrik.

---
## **1. Úvod a popis zdrojových dát**
Cieľom semestrálneho projektu je analyzovať dáta týkajúce sa filmov, používateľov a ich recenzií. Táto analýza umožňuje identifikovať trendy vo filmových preferenciách, najpopulárnejšie filmy a správanie používateľov.

Zdrojové dáta pochádzajú z GroupLens datasetu dostupného [tu](https://grouplens.org/datasets/movielens/). Dataset obsahuje osem hlavných tabuliek:
- `age_group`
- `genres`
- `genres_movies`
- `movies`
- `occupations`
- `ratings`
- `tags`
- `users`

Účelom ETL procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.

---
### **1.1 Dátová architektúra**

### **ERD diagram**
Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)**:

<p align="center">
  <img src=https://github.com/Samuel-kiss/MovieLens-ETL/blob/main/MovieLens_ERD.png alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma MovieLens</em>
</p>

---
## **2 Dimenzionálny model**

Navrhnutý bol **hviezdicový model (star schema)**, pre efektívnu analýzu kde centrálny bod predstavuje faktová tabuľka **`fact_ratings`**, ktorá je prepojená s nasledujúcimi dimenziami:
- **`dim_users`**: Obsahuje demografické údaje o používateľoch, ako sú vekové kategórie, povolanie a pohlavie.
- **`dim_tags`**: Obsahuje podrobné informácie o tagoch (popis, dátum vzniku).
- **`dim_date`**: Zahrňuje informácie o dátumoch hodnotení (deň, mesiac, rok).
- **`dim_genres`**: Obsahuje podrobné údaje o žánroch (názov žánru).
- **`dim_movies`**: Obsahuje podrobné údaje o filmoch (titulok a rok vydania).
  
Štruktúra hviezdicového modelu je znázornená na diagrame nižšie. Diagram ukazuje prepojenia medzi faktovou tabuľkou a dimenziami, čo zjednodušuje pochopenie a implementáciu modelu.

<p align="center">
  <img src=https://github.com/Samuel-kiss/MovieLens-ETL/blob/main/star_scheme.png alt="Star Schema">
  <br>
  <em>Obrázok 2 Schéma hviezdy pre MovieLens</em>
</p>

---
---
## **3. ETL proces v Snowflake**
ETL proces pozostával z troch hlavných fáz: `extrahovanie` (Extract), `transformácia` (Transform) a `načítanie` (Load). Tento proces bol implementovaný v Snowflake s cieľom pripraviť zdrojové dáta zo staging vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.

---
### **3.1 Extract (Extrahovanie dát)**
Dáta zo zdrojového datasetu (formát `.csv`) boli najprv nahraté do Snowflake prostredníctvom interného stage úložiska s názvom `my_stage`. Stage v Snowflake slúži ako dočasné úložisko na import alebo export dát. Vytvorenie stage bolo zabezpečené príkazom:

#### Príklad kódu:
```sql
CREATE OR REPLACE STAGE my_stage;
```
Do stage boli následne nahraté súbory obsahujúce údaje o filmoch, používateľoch, hodnoteniach, zamestnaniach, tagoch, žánroch a vekových skupín. Dáta boli importované do staging tabuliek pomocou príkazu `COPY INTO`. Pre každú tabuľku sa použil podobný príkaz:

```sql
COPY INTO occupations_staging
FROM @my_stage/occupations.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
```

V prípade nekonzistentných záznamov bol použitý parameter `ON_ERROR = 'CONTINUE'`, ktorý zabezpečil pokračovanie procesu bez prerušenia pri chybách.

---
### **3.1 Transfor (Transformácia dát)**

V tejto fáze boli dáta zo staging tabuliek vyčistené, transformované a obohatené. Hlavným cieľom bolo pripraviť dimenzie a faktovú tabuľku, ktoré umožnia jednoduchú a efektívnu analýzu.

Dimenzie boli navrhnuté na poskytovanie kontextu pre faktovú tabuľku. `Dim_users` obsahuje údaje o používateľoch vrátane vekových kategórií, pohlavia, zamestnania. Transformácia zahŕňala rozdelenie veku používateľov do kategórií (napr. „18-24“) a pridanie popisov zamestnaní. Táto dimenzia je typu SCD 2, čo umožňuje sledovať historické zmeny v zamestnaní používateľov.
```sql
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
```
Dimenzia `dim_date` je navrhnutá tak, aby uchovávala informácie o dátumoch hodnotení filmov. Obsahuje odvodené údaje, ako sú deň, mesiac, rok, deň v týždni (v textovom aj číselnom formáte). Táto dimenzia je štruktúrovaná tak, aby umožňovala podrobné časové analýzy, ako sú trendy hodnotení podľa dní, mesiacov alebo rokov. Z hľadiska SCD je táto dimenzia klasifikovaná ako SCD Typ 0. To znamená, že existujúce záznamy v tejto dimenzii sú nemenné a uchovávajú statické informácie.

V prípade, že by bolo potrebné sledovať zmeny súvisiace s odvodenými atribútmi (napr. pracovné dni vs. sviatky), bolo by možné prehodnotiť klasifikáciu na SCD Typ 1 (aktualizácia hodnôt) alebo SCD Typ 2 (uchovávanie histórie zmien). V aktuálnom modeli však táto potreba neexistuje, preto je `dim_date` navrhnutá ako SCD Typ 0 s rozširovaním o nové záznamy podľa potreby.

```sql
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
```
Podobne `dim_movies` obsahuje údaje o filmoch, ako sú názov a rok vydania . Táto dimenzia je typu SCD Typ 0, pretože údaje o filoch sú považované za nemenné, napríklad názov filmu alebo rok vydania sa nemenia. 
```sql
CREATE TABLE DIM_MOVIES AS 
SELECT DISTINCT
    movie_id AS dim_movieID,
    title,
    release_year
FROM movies_staging;
```
Faktová tabuľka `fact_ratings` obsahuje záznamy o hodnoteniach a prepojenia na všetky dimenzie. Obsahuje kľúčové metriky, ako je hodnota hodnotenia a časový údaj.
```sql
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
```

---
### **3.3 Load (Načítanie dát)**

Po úspešnom vytvorení dimenzií a faktovej tabuľky boli dáta nahraté do finálnej štruktúry. Na záver boli staging tabuľky odstránené, aby sa optimalizovalo využitie úložiska:
```sql
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS tags_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS users_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS genres_movies_staging;
DROP TABLE IF EXISTS age_group_staging;
```
ETL proces v Snowflake umožnil spracovanie pôvodných dát z `.csv` formátu do viacdimenzionálneho modelu typu hviezda. Tento proces zahŕňal čistenie, obohacovanie a reorganizáciu údajov. Výsledný model umožňuje analýzu čitateľských preferencií a správania používateľov, pričom poskytuje základ pre vizualizácie a reporty.

---
## **4 Vizualizácia dát**

Dashboard obsahuje `8 vizualizácií`, ktoré poskytujú základný prehľad o kľúčových metrikách a trendoch týkajúcich sa filmov, používateľov a hodnotení. Tieto vizualizácie odpovedajú na dôležité otázky a umožňujú lepšie pochopiť správanie používateľov a ich preferencie.

<p align="center">
  <img src="https://github.com/Samuel-kiss/MovieLens-ETL/blob/main/Dashboard.png" alt="ERD Schema">
  <br>
  <em>Obrázok 3 Dashboard MovieLens datasetu</em>
</p>

---
### **Graf 1: Najviac hodnotené filmy (Top 10 filmov)**
Táto vizualizácia zobrazuje 10 filmov s najväčším počtom hodnotení. Umožňuje identifikovať najpopulárnejšie tituly medzi používateľmi. Tieto informácie môžu byť užitočné na odporúčanie filmov alebo marketingové kampane.

```sql
SELECT 
    m.title AS movie_title,
    COUNT(f.ratingID) AS total_ratings
FROM FACT_RATINGS f
JOIN DIM_MOVIES m ON f.movieID = m.dim_movieID
GROUP BY m.title
ORDER BY total_ratings DESC
LIMIT 10;
```
---
### **Graf 2: Najviac hodnotené filmy mužským pohlavím (Top 10 filmov)**
Graf znázorňuje 10 filmov s najväčším počtom hodnotení mužského pohlavia. Z údajov je zrejmé, že muži najviac hodnotili film `Star Wars: Episode IV - A New Hope (1977)`. Táto vizualizácia ukazuje, aké filmy by sa mali odporúčať pre mužské pohlavie.

```sql
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
```
---
### **Graf 3: Najviac hodnotené filmy ženským pohlavím (Top 10 filmov)**
Graf znázorňuje 10 filmov s najväčším počtom hodnotení ženského pohlavia. Z údajov je zrejmé, že ženy najviac hodnotili film `Star Wars: Episode IV - A New Hope (1977)`. Táto vizualizácia ukazuje, aké filmy by sa mali odporúčať pre ženské pohlavie.

```sql
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
```
---
### **Graf 4: Celková aktivita v priebehu dňa**
Tabuľka znázorňuje, ako sú hodnotenia rozdelené podľa jednotlivých časových úsekov počas dňa. Z údajov vyplýva, že najväčšia aktivita je zaznamenaná v čase `23:26:40` s celkovým počtom hodnotení `82040`. Tento trend naznačuje, v akom časovom úseku používatilia hodnotili filmy najčastejšie.

```sql
SELECT 
    CAST(f.timestamp AS TIME) AS time,
    u.age_group AS age_group,
    COUNT(f.ratingID) AS total_ratings
FROM FACT_RATINGS f
JOIN DIM_USERS u ON f.userID = u.dim_userID
GROUP BY f.timestamp, u.age_group
ORDER BY total_ratings DESC;
```
---
### **Graf 5: Celková aktivita počas dní v týždni**
Tabuľka znázorňuje, ako sú hodnotenia rozdelené podľa jednotlivých dní v týždni. Z údajov vyplýva, že najväčšia aktivita je zaznamenaná počas pracovného týždna. Tento trend naznačuje, že používatelia majú viac času na sledovanie a hodnotenie filmov počas pracovných dní.

```sql
SELECT 
    d.dayOfWeek_string AS day,
    COUNT(f.ratingID) AS total_ratings
FROM FACT_RATINGS f
JOIN DIM_DATE d ON f.dateID = d.dim_dateID
GROUP BY d.dayOfWeek_string
ORDER BY total_ratings DESC;
```
---
### **Graf 6: Počet hodnotení podľa povolaní**
Tento graf  poskytuje informácie o počte hodnotení podľa povolaní používateľov. Umožňuje analyzovať, ktoré profesijné skupiny sú najviac aktívne pri hodnotení filmov a ako môžu byť tieto skupiny zacielené pri vytváraní personalizovaných odporúčaní. Z údajov je zrejmé, že najaktívnejšími profesijnými skupinami sú `Educator` , `Others/Not Specified` a `Executive` s viac ako 1,5 milióna hodnotení. 

```sql
SELECT 
    u.occupation AS occupation,
    COUNT(f.ratingID) AS total_ratings
FROM FACT_RATINGS f
JOIN DIM_USERS u ON f.userID = u.dim_userID
GROUP BY u.occupation
ORDER BY total_ratings DESC
LIMIT 10;
```
---
### **Graf 7: Rozdelenie hodnotení podľa pohlavia používateľov**
Graf znázorňuje rozdiely v počte hodnotení medzi mužmi a ženami. Z údajov je zrejmé, že muži hodnotili filmy častejšie ako ženy. Táto vizualizácia ukazuje, že obsah alebo kampane môžu byť viacej zamerané na mužské pohlavie.

```sql
SELECT 
    u.gender,
    COUNT(f.ratingID) AS total_ratings
FROM FACT_RATINGS f
JOIN DIM_USERS u ON f.userID = u.dim_userID
GROUP BY u.gender;
```
---
### **Graf 8: Aktivita používateľov podľa vekových kategórií**
Tento stĺpcový graf ukazuje, ako sa aktivita používateľov líši medzi rôznymi vekovými skupinami. Z grafu vyplýva, že používatelia vo vekovej kategórii `25-34` sú najviac aktívny medzi všetkými skupinami, zatiaľ čo ostatné vekové skupiny vykazujú výrazne nižšiu aktivitu, čo môže súvisieť s pracovnými povinnosťami. Tieto informácie môžu pomôcť lepšie zacieliť obsah a plánovať aktivity pre rôzne vekové kategórie.
```sql
SELECT 
    u.age_group AS age_group,
    COUNT(f.ratingID) AS total_ratings
FROM FACT_RATINGS f
JOIN DIM_USERS u ON f.userID = u.dim_userID
GROUP BY u.age_group
ORDER BY total_ratings DESC;

```

Dashboard poskytuje komplexný pohľad na dáta, pričom zodpovedá dôležité otázky týkajúce sa filmových preferencií a správania používateľov. Vizualizácie umožňujú jednoduchú interpretáciu dát a môžu byť využité na optimalizáciu odporúčacích systémov, marketingových stratégií a filmových služieb.

---


**Autor:** Samuel Kiss
