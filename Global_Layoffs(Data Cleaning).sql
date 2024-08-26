select * 
from layoffs;

/* DATA CLEANING IN SQL 
1. REMOVE DUPLICATES
2. STANDARDISE THE DATA
3. REMOVE NULL VALUES OR BLANK VALUES 
4. YOU REMOVE UNECESSARY COLUMNS(DONE WITH CAUTION TO NOT DISTURB THE ETL PIPELINE
 */
 
select count(*) 
from layoffs;  -- just to check if the correct number of records is present in the dataset
 
 -- 1. Step 1: Removing Duplicates
create table layoffs_staging 
like layoffs;

-- Creating a dummy table to make all your chnages, instead of making the changes in the original table*/
select * 
from layoffs_staging;

-- Copying all the values from layoffs_new table and inserting into layoffs_staging table 
insert into layoffs_staging
select * from layoffs;

UPDATE layoffs_staging
SET 
    `company` = NULLIF(`company`, ''),
    `location` = NULLIF(`location`, ''),
    `industry` = NULLIF(`industry`, ''),
    `percentage_laid_off` = NULLIF(`percentage_laid_off`, ''),
    `stage` = NULLIF(`stage`,''),
    `country` = NULLIF(`country`, ''),
    `funds_raised` = NULLIF(`funds_raised`,''),
    `date` = NULLIF(`date`,''),
    `total_laid_off` = NULLIF(`total_laid_off`,'');

ALTER TABLE layoffs_staging
CHANGE COLUMN funds_raised funds_raised_millions float;

select *,
ROW_NUMBER() OVER
(PARTITION BY COMPANY,LOCATION,INDUSTRY,TOTAL_LAID_OFF,PERCENTAGE_LAID_OFF,`DATE`,STAGE,COUNTRY,FUNDS_RAISED_MILLIONS) AS ROW_NUM
FROM LAYOFFS_STAGING;

/* So now 3743 rows are returned now we can't obviously go to each row 
and see which row as row_num = 2 as our duplicate, That's when CTE comes into play. */

with duplicate_cte as 
(select *,
ROW_NUMBER() OVER
(PARTITION BY COMPANY,LOCATION,INDUSTRY,TOTAL_LAID_OFF,PERCENTAGE_LAID_OFF,`DATE`,STAGE,COUNTRY,FUNDS_RAISED_MILLIONS) AS ROW_NUM
FROM LAYOFFS_STAGING
)
SELECT * -- techincally we can't do this see error code below 
from duplicate_cte
where row_num > 1;

-- Error Code: 1288. The target table duplicate_cte of the DELETE is not updatable
-- So we need to create an other table and remove the rows that has these duplicate rows 


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised` text,
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select * 
from layoffs_staging2;

insert into layoffs_staging2
select *,
ROW_NUMBER() OVER
(PARTITION BY COMPANY,LOCATION,INDUSTRY,TOTAL_LAID_OFF,PERCENTAGE_LAID_OFF,`DATE`,STAGE,COUNTRY,FUNDS_RAISED_MILLIONS) AS ROW_NUM
FROM LAYOFFS_STAGING;

delete 
from layoffs_staging2 
where row_num > 1;

-- Step 2: Standardizing Data
select distinct(company) 
from layoffs_staging2;

select trim(company), company 
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct(industry) 
from layoffs_staging2;

select * 
from layoffs_staging2
where industry is null;

-- droping this record since the industry doesn't make sense and total_laid off and percentage laid off is NULL
delete 
from layoffs_staging2
where industry like 'https%';

update layoffs_staging2
set industry = 'Data'
where company = 'appsmith';

select distinct country 
from layoffs_staging2
order by 1;

UPDATE layoffs_staging2
SET `date` = TRIM(`date`);

select `date`, 
str_to_date(`date`,'%Y-%m-%d')
from layoffs_staging2;

DELETE 
FROM layoffs_staging2
WHERE `date` IS NULL;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%Y-%m-%d');

-- Now Changing the Date Column to Actual Date Datatype

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


ALTER TABLE layoffs_staging2
MODIFY COLUMN `total_laid_off` int;

ALTER TABLE layoffs_staging2
CHANGE COLUMN funds_raised funds_raised_millions float;


select * 
from layoffs_staging2;

-- 3. WORKING WITH NULL AND BLANK VALUES

select * 
from layoffs_staging2
where (total_laid_off IS NULL)
AND (percentage_laid_off  IS NULL) ; 
-- The above query return 661 rows where percentage_laid_off and total_laid_off is null, so we have to remove those rows 

delete
from layoffs_staging2
where (total_laid_off IS NULL)
AND (percentage_laid_off IS NULL) ; 

select distinct industry 
from 
layoffs_staging2;
-- There is space and NULL 


select * from 
layoffs_staging2
where 
(industry = 'NULL' or industry is null)
OR industry = '' ;
-- We can populate the industries which have null or blank with the actual industry-->self join to the rescue!
-- THIS QUERY RETURNS companies Airbnb,Bally's Interactive, Carvana, Juul

select * 
from layoffs_staging2
where company in ('Juul','Airbnb',"Bally's Interactive", 'Carvana');
-- Just checking if there is value for industry in other rows that has company as Airbnb or Juul 

-- Now let's create a self join in order to populate the blank/null values

select *
from layoffs_staging2 as tb1 
join layoffs_staging2 as tb2
	on tb1.company = tb2.company 
where (tb1.industry = 'NULL' or tb1.industry = '')
and (tb2.industry is not null and tb2.industry <> 'NULL' and tb2.industry <> '');

-- Let's update now with the populated values 
Update layoffs_staging2 as tb1 
join layoffs_staging2 as tb2
	on tb1.company = tb2.company 
set tb1.industry = tb2.industry
where (tb1.industry = 'NULL' or tb1.industry = '')
and (tb2.industry is not null and tb2.industry <> 'NULL' and tb2.industry <> '');

-- 4. Remove unecessary columns

select * 
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;

-- ------- ---
-- Just trying to convert the string NULL values to actual NULL for EDA
    

/* Now there are few null values in total_laid_off,percentage_laid_off..technically we can populate 
the null values since we don't have the total proportion. For the funds_raised_millions too we cannot populate the null values since we dontknow the value, 
we can web scrape and find the values but thats an whole other level*/

 
 