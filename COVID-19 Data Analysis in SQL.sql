/*
COVID-19 Data Exploration (SQL) – Project

This project explores global COVID-19 deaths and vaccination data using SQL, with the goal of
deriving key metrics and preparing a dataset for downstream Tableau visualisations.
The project involves: initial data cleaning and preparation, analysis of infection and death rates by country
and over time, and combining deaths + vaccinations data to compute rolling vaccination totals
and vaccination rates. Outputs are stored via a view for subsequent use in Tableu.
*/

SELECT *
FROM PortfolioProject..CovidDeaths
ORDER BY 3,4;

-- SELECT *
-- FROM PortfolioProject..CovidVaccinations
-- ORDER BY 3,4;

-----------------------------------------------------------------------------------------
-- 0) Exploring Key Data Used in Analysis
-----------------------------------------------------------------------------------------
-- Select columns used throughout the analysis
SELECT location, date, total_cases, new_cases, total_deaths, population
FROM PortfolioProject..CovidDeaths
ORDER BY 1,2;


-----------------------------------------------------------------------------------------
-- I) Data Preparation & Cleaning
-----------------------------------------------------------------------------------------
-- 1) Convert select data from varchar to float for calculations
ALTER TABLE PortfolioProject..CovidDeaths
ALTER COLUMN total_cases FLOAT;

ALTER TABLE PortfolioProject..CovidDeaths
ALTER COLUMN new_cases FLOAT;

ALTER TABLE PortfolioProject..CovidDeaths
ALTER COLUMN total_deaths FLOAT;

ALTER TABLE PortfolioProject..CovidDeaths
ALTER COLUMN new_deaths FLOAT;

ALTER TABLE PortfolioProject..CovidDeaths
ALTER COLUMN population FLOAT;

-- 2) Replace "0's" with NULLs
UPDATE PortfolioProject..CovidDeaths
SET total_cases = NULL
WHERE total_cases = 0;

UPDATE PortfolioProject..CovidDeaths
SET new_cases = NULL
WHERE new_cases = 0;

UPDATE PortfolioProject..CovidDeaths
SET total_deaths = NULL
WHERE total_deaths = 0;

UPDATE PortfolioProject..CovidDeaths
SET new_deaths = NULL
WHERE new_deaths = 0;

UPDATE PortfolioProject..CovidDeaths
SET population = NULL
WHERE population = 0;


-----------------------------------------------------------------------------------------
-- II) Exploratory Data Analysis (CovidDeaths)
-----------------------------------------------------------------------------------------
-- 1) Total Cases vs Total Deaths
--    Approximate case fatality rate (death rate) by location over time
SELECT location, date, total_cases, total_deaths,
(total_deaths/total_cases) * 100 AS death_rate
FROM PortfolioProject..CovidDeaths
ORDER BY 1,2;

-- US focus
SELECT location, date, total_cases, total_deaths,
(total_deaths /total_cases) * 100 AS death_rate
FROM PortfolioProject..CovidDeaths
WHERE location like '%states%'
ORDER BY 1,2;


-- 2) Total Cases vs Population
--    Infection rate as a % of population (US example)
SELECT location, date, population, total_cases,
(total_cases/population) * 100 AS infection_rate
FROM PortfolioProject..CovidDeaths
WHERE location like '%states%'
ORDER BY 1,2;


-- 3) Countries with Highest Infection Rate per Population
SELECT location, MAX(total_cases) AS Highest_Infection_Count,
     (MAX(total_cases)/population) * 100 AS Percent_Population_Infected
FROM PortfolioProject..CovidDeaths
GROUP BY location, population
ORDER BY Percent_Population_Infected DESC;


-- 4) Countries with Highest Death Count
SELECT location, MAX(total_deaths) AS Total_Death_Count 
FROM PortfolioProject..CovidDeaths
GROUP BY location
ORDER BY Total_Death_Count DESC;

-- Aggregated regions are shown (e.g. World, continents)
-- Exclude these from country-level comparisons by nulling blank continent values
UPDATE PortfolioProject..CovidDeaths
SET continent = NULL
WHERE continent = '';

-- Check country-only results (continent present)
SELECT location, MAX(total_deaths) AS Total_Death_Count 
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY Total_Death_Count DESC;

 
-- 5) Continent/Region-Level Death Counts (aggregated rows)
SELECT location, MAX(total_deaths) AS Total_Death_Count 
FROM PortfolioProject..CovidDeaths
WHERE continent IS NULL
GROUP BY location
ORDER BY Total_Death_Count DESC;


 -- 6) Global Daily Totals and Global Death Rate
SELECT date, SUM(new_cases) AS total_cases, SUM(new_deaths) AS total_deaths, (SUM(new_deaths)/SUM(new_cases)) * 100 AS Global_Death_Rate
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY date
ORDER BY 1, 2; 


-----------------------------------------------------------------------------------------
-- III) Combining Deaths + Vaccinations 
-----------------------------------------------------------------------------------------
-- 1) Prepare Vaccinations Table (type conversion + cleaning)
ALTER TABLE PortfolioProject..CovidVaccinations
ALTER COLUMN new_vaccinations FLOAT;

-- Replace '' with NULLs for consistency
UPDATE PortfolioProject..CovidVaccinations
SET new_vaccinations = NULL
WHERE new_vaccinations = '';


-- 2) Join Tables (Deaths + Vaccinations by location + date)
SELECT * 
FROM PortfolioProject..CovidDeaths dea
JOIN PortfolioProject..CovidVaccinations vac
    ON dea.location = vac.location
    and dea.date = vac.date;


-- 3) Population vs Vaccinations
--    Rolling vaccinations by location using a window function
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS Rolling_People_Vaccinated
FROM PortfolioProject..CovidDeaths dea
JOIN PortfolioProject..CovidVaccinations vac
    ON dea.location = vac.location
    and dea.date = vac.date
    WHERE dea.continent IS NOT NULL
    ORDER BY 2,3;


-- Use CTE: compute rolling vaccinations + derive vaccination rate (%)
WITH Pop_vs_Vac (Continent, Location, Date, Population, New_Vaccinations, Rolling_People_Vaccinated)
AS 
(
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS Rolling_People_Vaccinated
FROM PortfolioProject..CovidDeaths dea
JOIN PortfolioProject..CovidVaccinations vac
    ON dea.location = vac.location
    and dea.date = vac.date
    WHERE dea.continent IS NOT NULL
)
SELECT *, (Rolling_People_Vaccinated/Population) * 100 AS Vaccination_Rate
FROM Pop_vs_Vac
ORDER BY 2,3
;


-- Use Temp Table for rolling metrics for repeated querying
DROP Table if exists #Percent_Population_Vaccinated
CREATE TABLE #Percent_Population_Vaccinated
(Continent nvarchar(255), 
Location nvarchar(255) , 
Date datetime, 
Population FLOAT, 
New_Vaccinations FLOAT,
Rolling_People_Vaccinated FLOAT,
)

INSERT INTO #Percent_Population_Vaccinated
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS Rolling_People_Vaccinated
FROM PortfolioProject..CovidDeaths dea
JOIN PortfolioProject..CovidVaccinations vac
    ON dea.location = vac.location
    and dea.date = vac.date
    WHERE dea.continent IS NOT NULL

SELECT *, (Rolling_People_Vaccinated/Population) * 100 AS Vaccination_Rate
FROM #Percent_Population_Vaccinated
ORDER BY 2,3
;


-----------------------------------------------------------------------------------------
-- IV) Create View for Tableau Visualisations
-----------------------------------------------------------------------------------------
-- Store the joined + rolling vaccination dataset for reuse in dashboards
CREATE VIEW Percent_Population_Vaccinated AS
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS Rolling_People_Vaccinated
FROM PortfolioProject..CovidDeaths dea
JOIN PortfolioProject..CovidVaccinations vac
    ON dea.location = vac.location
    and dea.date = vac.date
    WHERE dea.continent IS NOT NULL
;



SELECT * 
FROM Percent_Population_Vaccinated;
