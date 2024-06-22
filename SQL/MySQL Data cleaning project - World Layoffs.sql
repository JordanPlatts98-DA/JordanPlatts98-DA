-- SQL Project - Data Cleaning
-- Data set used: https://www.kaggle.com/datasets/swaptr/layoffs-2022

SELECT * 
FROM world_layoffs.layoffs;

-- The following steps will be taken to clean the data:
-- 1. Check for and remove any duplicates
-- 2. Standardise the data and fix any errors
-- 3. Look for NULL or/and blank values
-- 4. Remove any columns that may not be neccessary for the EDA


-- first thing i want to do is create a staging table. This is the one i will work in and clean the data. I want to keep the raw data intact and unchanged in case of any issues:
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

SELECT * 
FROM world_layoffs.layoffs_staging;

INSERT layoffs_staging 
SELECT * 
FROM world_layoffs.layoffs;

-- 1. Remove Duplicates
# First let's check for duplicates:

SELECT *
FROM world_layoffs.layoffs_staging;

SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
	FROM world_layoffs.layoffs_staging;
    
WITH duplicate_cte AS
(
SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
	FROM world_layoffs.layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;
    
-- let's just look at oda to confirm

SELECT *
FROM world_layoffs.layoffs_staging
WHERE company = 'Oda'
;
-- it looks like these are all legitimate entries and shouldn't be deleted. I need to really look at every single row to be accurate:
-- these are our real duplicates 

WITH duplicate_cte AS
(
SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM world_layoffs.layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- these are the ones I want to delete where the row number is > 1 or 2 or greater essentially

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM world_layoffs.layoffs_staging2;

INSERT INTO world_layoffs.layoffs_staging2
SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM world_layoffs.layoffs_staging;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;

DELETE
FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;

-- Duplicates are now deleted

-- 2. Standardising the data

SELECT company, TRIM(company)
FROM world_layoffs.layoffs_staging2;

UPDATE world_layoffs.layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY 1;

-- Identified a few blanks, nulls and industries that are the same but the data has been inputed differently

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE world_layoffs.layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY 1;

SELECT DISTINCT location
FROM world_layoffs.layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY 1;

-- United states is entered twice

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE country LIKE 'United States%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM world_layoffs.layoffs_staging2
ORDER BY 1;

UPDATE world_layoffs.layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT *
FROM world_layoffs.layoffs_staging2;

-- Now i want to change the date column data type to date, currently it is text

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM world_layoffs.layoffs_staging2;

UPDATE world_layoffs.layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM world_layoffs.layoffs_staging2;

ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM world_layoffs.layoffs_staging2;

-- 3. NULL and blank values

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company = 'Airbnb';

SELECT *
FROM world_layoffs.layoffs_staging2 AS t1
JOIN world_layoffs.layoffs_staging2 AS t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

SELECT t1.industry, t2.industry
FROM world_layoffs.layoffs_staging2 AS t1
JOIN world_layoffs.layoffs_staging2 AS t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

UPDATE world_layoffs.layoffs_staging2 AS t1
JOIN world_layoffs.layoffs_staging2 AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

-- NULLS and blanks removed from the industry column apart from one result

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';

-- As Ballys Interactive is only one row, we have no information to fill in the nulls, likewise for the columns total laid off, percentage laid off and funds raised millions

-- 4. Removing columns and rows

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- As we have no way of filling this infomation, and these columns will be used heavily in the EDA project, I can remove them

SELECT *
FROM world_layoffs.layoffs_staging2;

ALTER TABLE world_layoffs.layoffs_staging2
DROP COLUMN row_num;

-- row_num is dropped as the data is not neccessary for the EDA

-- Data is now cleaned ready for the EDA
