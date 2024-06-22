-- Exploratory Data Analysis ----------------------------------------------------------------------------------------------------------------------------------------------

-- I am just going to explore the data and find trends or patterns or anything interesting like outliers

SELECT *
FROM world_layoffs.layoffs_staging2;

-- EASIER/SIMPLER QUERIES -------------------------------------------------------------------------------------------------------------------------------------------------

SELECT MAX(total_laid_off)
FROM world_layoffs.layoffs_staging2;

SELECT MAX(total_laid_off),  MAX(percentage_laid_off)
FROM world_layoffs.layoffs_staging2;

-- 12000, at one point, were laid off in one go, by one company, likely due to the company going bust or was an industry with huge layoffs like Tech

-- Which companies had 1 which is basically 100 percent of they company laid off
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1;
-- these are mostly startups it looks like who all went out of business during this time

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER by total_laid_off DESC;

-- Katerra, a construction company, went bust with 2434 people being laid off

-- if we order by funds_raised_millions we can see how big some of these companies were
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;
-- BritishVolt looks like an EV company and they raised 2.4 billion, Quibi, a media company raised nearly 2 billion dollars and went under

-- SOMEWHAT TOUGHER AND MOSTLY USING GROUP BY------------------------------------------------------------------------------------------------------------------------------

-- Companies with the biggest single Layoff

SELECT company, total_laid_off
FROM world_layoffs.layoffs_staging
ORDER BY 2 DESC
LIMIT 5;
-- now that's just on a single day
-- This answers our previous question of who laid off 12000 people in one go. Google META, Amazon, Microsoft laid of over 10000

-- Companies with the most Total Layoffs over the course of when the data was collected
SELECT company, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;

-- over that period of time, Amazon laid of 18150 people
-- Lets look to see when these layoffs occured

SELECT MIN(date), MAX(date)
FROM world_layoffs.layoffs_staging2;

-- these layoffs occured between 2020-03-11 and 2023-03-06, over 3 years

-- What industries had the highest total laid off over the 3 years

SELECT industry, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- Consumer and retail both had over 40000 lay offs over 3 years
-- Manufacturing was the lowest with only 20

-- Lets see which country saw the most layoffs

SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- United States laid off 256559 people with the second highest being India at 35993

-- Lets look at the locations of these countries 

SELECT location, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

-- The san francisco bay area saw the most layoffs. The top 3 are all US locations which is not a surpise given the USA had the most layoffs by country

-- Lets look by date (years)

SELECT YEAR(date), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(date)
ORDER BY 1 ASC;

-- 2022 had the highest total laid off with 160661
-- However, in just 3 months of 2023, 125677 were laid off

-- At what stage did these layoffs occur

SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- Post IPO was the highets with 204132, these are where the big tech companies sit so shouldnt be a suprise

-- TOUGHER QUERIES------------------------------------------------------------------------------------------------------------------------------------

-- Earlier I looked at Companies with the most Layoffs. Now let's look at that per year.

-- Rolling Total of Layoffs Per Month

SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC;

-- Lets remove the nulls

SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
WHERE SUBSTRING(date,1,7) IS NOT NULL
GROUP BY dates
ORDER BY dates ASC;

WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`,1,7) as `MONTH` , SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_laid_off
, SUM(total_laid_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;

-- The above CTE shows a month by month progression of how many people were laid off, and the rolling totals by month
-- Between March 2020 and March 2023, 383159 people were laid off, with most of those occuring during 2022 and 2023

-- Lets have a look at which companies (top 5) laid of the most employees in each year

WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 5
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;

-- 2020 -- Uber, booking.com, Groupon, Swiggy, Airbnb 
-- 2021 -- Bytedance, Katerra, Zillow, Instacart, WhiteHat Jr
-- 2022 -- Meta, Amazon, Cisco, Peloton, Carvana, Philips
-- 2023 -- Google, Microsoft, Ericsson, Amazon, Salesforce, Dell

-- We can also rewrite the above query to look at which industries (top 5) laid of the most employees in each year

WITH Industry_Year AS 
(
  SELECT industry, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY industry, YEAR(date)
)
, Industry_Year_Rank AS (
  SELECT industry, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Industry_Year
)
SELECT industry, years, total_laid_off, ranking
FROM Industry_Year_Rank
WHERE ranking <= 5
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;

-- 2020 -- Transportation, Travel, Finance, Retail, Food
-- 2021 -- Consumer, Real Estate, Food, Construction. Education
-- 2022 -- Retail, Consumer, Transportation, Healthcare, Finance
-- 2023 -- Other, Consumer, Retail, Hardware, Healthcare

-- We can also rewrite the above query to look at which locations (top 5) laid of the most employees in each year

WITH Location_Year AS 
(
  SELECT location, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY location, YEAR(date)
)
, Location_Year_Rank AS (
  SELECT location, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Location_Year
)
SELECT location, years, total_laid_off, ranking
FROM Location_Year_Rank
WHERE ranking <= 5
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;

-- 2020 -- SF Bay Area, Bengaluru, New York City, Amsterdam, Boston
-- 2021 -- SF Bay Area, Mumbai, Seattle, Shanghai, New York City
-- 2022 -- SF Bay Area, New York City, Seattle, Bengaluru, Amsterdam
-- 2023 -- SF Bay Area, Seattle, Stockholm, Amsterdam, New York City

-- We can also rewrite the above query to look at which countries (top 5) laid of the most employees in each year

WITH Country_Year AS 
(
  SELECT country, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY country, YEAR(date)
)
, Country_Year_Rank AS (
  SELECT country, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Country_Year
)
SELECT country, years, total_laid_off, ranking
FROM Country_Year_Rank
WHERE ranking <= 5
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;

-- 2020 -- United States, India, Netherlands, Brazil, Singapore
-- 2021 -- United States, India, China, Germany, Canada
-- 2022 -- United States, India, Netherlands, Brazil, Canada
-- 2023 -- United States, Sweden, Netherlands, India, Germany

-- We can also rewrite the above query to look at which stage (top 5) saw the most employees laid of in each year

WITH Stage_Year AS 
(
  SELECT stage, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY stage, YEAR(date)
)
, Stage_Year_Rank AS (
  SELECT stage, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Stage_Year
)
SELECT stage, years, total_laid_off, ranking
FROM Stage_Year_Rank
WHERE ranking <= 5
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;

-- 2020 -- Post-IPO, Acquired, Series D, Unknown, Series B
-- 2021 -- Unknown, Acquired, Post-IPO, Series F, Series D
-- 2022 -- Post-IPO, Unknown, Series C, Series D, Series B
-- 2023 -- Post-IPO, Acquired, Unknown, Series C, Series E