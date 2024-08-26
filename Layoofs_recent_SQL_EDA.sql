-- Exploratory Data Analysis 

select * 
from layoffs_staging2;

-- checking out the max laid off in numbers as well as percentage 
select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

-- checking out the min laid off in numbers as well as percentage
select min(total_laid_off), min(percentage_laid_off)
from layoffs_staging2;

-- checking out the records where the laid off percent is 100%
select * 
from layoffs_staging2
where percentage_laid_off = 1 
order by total_laid_off DESC;
-- Almost 263 companies laid off 100% of their employees

select count(*), stage
from layoffs_staging2
where percentage_laid_off = 1
group by stage;
-- High Layoff Rates in Early Stages: 32 companies in Series B, 19 in Seed, and 13 in Series C stages laid off 100% of their employees, 
-- highlighting the vulnerability of early-stage startups to market shifts

-- checking out which company got funded the most
select * 
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions DESC;

-- checking out the total number of people being laid off in each company sorted in max
select company, sum(total_laid_off)
from layoffs_staging2
group by company 
order by 2 DESC;

-- checking out the date range in which people got laid off the most -->started in ealy 2020(COVID beginning) to early 2023
select max(`date`), min(`date`)
from layoffs_staging2;

-- checking out which industry got hit the most ->Consumer,Retail,Transportation hit the most layoffs 
select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 DESC;

-- checking out which country got hit the most -->United States, India, Germany 
select country, sum(total_laid_off) as tloff
from layoffs_staging2
group by country
having tloff is not null
order by 2 DESC;

-- Seeing which year got the most layoffs
select YEAR(`date`), sum(total_laid_off) 
from layoffs_staging2
group by YEAR(`date`)
order by 1 desc;
-- In 2023 -->  164,319 people are laid off, worst is in 2023 --> 264,220 people are laid off
-- 2020 and 2021 were peak covid years but the layoffs seem to be around 15,000 - 80,000

-- Seeing year wise, how many people were laid off based on the industry 
SELECT industry, YEAR(date) as year, SUM(total_laid_off) as total_laid_off
FROM layoffs_staging2
GROUP BY industry, year
ORDER BY industry, year;


-- checking the layoffs based on the stage of the company 
select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 DESC;
-- Post IPO Layoffs 365325- Higher layoffs in the "Post-IPO" stage could suggest challenges in adapting to public market pressures.

-- CHECKING OUT ROLLING TOTALS 

select substring(`date`, 1 , 7) as `month`, sum(total_laid_off)
from layoffs_staging2
group by substring(`date`, 1 , 7) 
order by 1 ASC;

WITH ROLLING_TOTAL AS
(select substring(`date`, 1 , 7) as `month`, sum(total_laid_off) AS TOTAL_LOFF
from layoffs_staging2
group by substring(`date`, 1 , 7) 
order by 1 ASC
)
SELECT `MONTH`, TOTAL_LOFF, SUM(TOTAL_LOFF) OVER(ORDER BY `MONTH`) AS ROLLING_TOTAL_month
FROM ROLLING_TOTAL;

-- CONCLUSION OF THIS QUERY 
/*
1. Gradual Increase (2020-2021): Layoffs grew steadily, reflecting a slow economic impact.
2. Sharp Spike in 2022: Layoffs nearly doubled, indicating a worsening economic situation.
3. Massive Surge in 2023: January 2023 saw the highest spike, signaling a severe crisis.
4. High Layoff Levels in 2024: Layoffs remained elevated, showing continued economic challenges.
*/

-- CHECKING HOW MANY LAYOFFS WAS DONE ON A YEAR BASIS PER COMPANY
select company, location, year(`date`), sum(total_laid_off) as tloff
from layoffs_staging2
group by company, year(`date`), location
order by tloff DESC;

with company_year(COMPANY, LOCATION, `YEAR`, TOTAL_LAID_OFF) as
(select company, location, year(`date`), sum(total_laid_off) as tloff
from layoffs_staging2
group by company, year(`date`), location
)
select *, DENSE_RANK() OVER (PARTITION BY YEAR ORDER BY TOTAL_LAID_OFF DESC) AS Ranking
from company_year
order by ranking;


with company_year(COMPANY, LOCATION, `YEAR`, TOTAL_LAID_OFF) as 
(select company, location, year(`date`), sum(total_laid_off) as tloff
from layoffs_staging2
group by company, year(`date`), location
),company_year_rank as 
(
select *, DENSE_RANK() OVER (PARTITION BY YEAR ORDER BY TOTAL_LAID_OFF DESC) AS Ranking
from company_year
)
select * 
from company_year_rank 
where ranking <=5;
;

-- CONCLUSION OF THIS QUERY
/* 
1. Amazon and Meta lead in layoffs, especially in 2023.
2. Tech companies like Microsoft, Google, and Intel are heavily impacted.
3. SF Bay Area and Seattle are major layoff hubs.
4. 2023-2024 show the highest layoff spikes.
*/