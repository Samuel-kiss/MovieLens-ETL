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
CREATE TABLE DIM_USERS AS
SELECT DISTINCT
    u.userId AS dim_userId,
    COALESCE(g.name, 'Wrong Data') AS age_group,
    COALESCE(o.name, 'Wrong Data') AS occupation,
    u.gender
FROM users_staging u
LEFT JOIN occupations_staging o 
    ON u.occupationId = o.occupationId
LEFT JOIN age_group_staging g 
    ON (
        (g.name = 'Under 18' AND u.age < 18) OR
        (g.name = '18-24' AND u.age BETWEEN 18 AND 24) OR
        (g.name = '25-34' AND u.age BETWEEN 25 AND 34) OR
        (g.name = '35-44' AND u.age BETWEEN 35 AND 44) OR
        (g.name = '45-49' AND u.age BETWEEN 45 AND 49) OR
        (g.name = '50-55' AND u.age BETWEEN 50 AND 55) OR
        (g.name = '56+' AND u.age >= 56))
ORDER BY dim_userId;
```
Dimenzia `dim_date` je navrhnutá tak, aby uchovávala informácie o dátumoch hodnotení filmov. Obsahuje odvodené údaje, ako sú deň, mesiac, rok, deň v týždni (v textovom aj číselnom formáte). Táto dimenzia je štruktúrovaná tak, aby umožňovala podrobné časové analýzy, ako sú trendy hodnotení podľa dní, mesiacov alebo rokov. Z hľadiska SCD je táto dimenzia klasifikovaná ako SCD Typ 0. To znamená, že existujúce záznamy v tejto dimenzii sú nemenné a uchovávajú statické informácie.

V prípade, že by bolo potrebné sledovať zmeny súvisiace s odvodenými atribútmi (napr. pracovné dni vs. sviatky), bolo by možné prehodnotiť klasifikáciu na SCD Typ 1 (aktualizácia hodnôt) alebo SCD Typ 2 (uchovávanie histórie zmien). V aktuálnom modeli však táto potreba neexistuje, preto je `dim_date` navrhnutá ako SCD Typ 0 s rozširovaním o nové záznamy podľa potreby.

```sql
CREATE TABLE DIM_DATE AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(rated_at AS DATE)) AS dim_dateID, 
    CAST(rated_at AS DATE) AS date,                    
    DATE_PART(day, rated_at) AS day,                   
    DATE_PART(dow, rated_at) + 1 AS dayOfWeek,        
    CASE DATE_PART(dow, rated_at) + 1
        WHEN 1 THEN 'Pondelok'
        WHEN 2 THEN 'Utorok'
        WHEN 3 THEN 'Streda'
        WHEN 4 THEN 'Štvrtok'
        WHEN 5 THEN 'Piatok'
        WHEN 6 THEN 'Sobota'
        WHEN 7 THEN 'Nedeľa'
    END AS dayOfWeekAsString,
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
    END AS monthAsString,
    DATE_PART(year, rated_at) AS year,                
    DATE_PART(week, rated_at) AS week,                       
FROM RATINGS_STAGING
GROUP BY CAST(rated_at AS DATE), 
         DATE_PART(day, rated_at), 
         DATE_PART(dow, rated_at), 
         DATE_PART(month, rated_at), 
         DATE_PART(year, rated_at), 
         DATE_PART(week, rated_at);
```
Podobne `dim_movies` obsahuje údaje o filmoch, ako sú názov a rok vydania . Táto dimenzia je typu SCD Typ 0, pretože údaje o filoch sú považované za nemenné, napríklad názov filmu alebo rok vydania sa nemenia. 

Faktová tabuľka `fact_ratings` obsahuje záznamy o hodnoteniach a prepojenia na všetky dimenzie. Obsahuje kľúčové metriky, ako je hodnota hodnotenia a časový údaj.
```sql
CREATE TABLE FACT_RATINGS AS
SELECT DISTINCT
    r.ratingId AS fact_ratingID, 
    r.rating, 
    r.rated_at AS timestamp, 
    r.movieId AS movieID,
    r.userId AS userID,
    g.dim_genreid AS genreID,
    ta.dim_tagid AS tagID,
    d.dim_dateId AS dateID                  
FROM RATINGS_STAGING r
JOIN DIM_MOVIES m ON r.movieId = m.dim_movieId 
JOIN DIM_USERS u ON r.userId = u.dim_userId 
JOIN DIM_DATE d ON CAST(r.rated_at AS DATE) = d.date 
JOIN genres_movies_staging gm ON m.dim_movieId = gm.movieId 
JOIN DIM_GENRES g ON gm.genreId = g.dim_genreId
JOIN DIM_TAGS ta ON m.dim_movieId = ta.dim_movieId
GROUP BY 
    r.ratingId, 
    r.rating, 
    r.rated_at, 
    r.movieId,
    r.userId,
    d.dim_dateId,
    g.dim_genreid,
    ta.dim_tagid
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



**Autor:** Samuel Kiss
