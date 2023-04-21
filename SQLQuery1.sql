--Select the data were are going to be using
-- needed to add 'where continent is not null' due to issues with the dataset, where location was a continent and the continent field was null.

select 
	Location, 
	date, 
	total_cases, 
	new_cases, 
	total_deaths, 
	population
from Portfolio..CovidDeaths
where continent is not null
order by location, date


-- Looking at total cases vs total deaths
-- shows likelihood of dying after contracting covid in your country
--needed to convert total_deaths and total_cases from nvarchar to floats to be able to use division operation

select 
	Location, 
	date, 
	total_cases, 
	total_deaths,
	cast(total_deaths as float) / cast(total_cases as float) * 100 as death_percentage
from Portfolio..CovidDeaths
where continent is not null
order by location, date

-- looking at total cases vs population
-- shows % population that got covid
-- using where filter to look at United Kingdom

select 
	Location, 
	date, 
	total_cases, 
	population,
	cast(total_cases as float) / cast(population as float) * 100 as infection_rate
from Portfolio..CovidDeaths
where location like '%United Kingdom%'
and continent is not null
order by location, date

-- looking at countries with highest infection rate compared to population

select 
	Location, 
	Max(total_cases) as highest_infection_count, 
	population,
	cast(max(total_cases) as float) / cast(population as float) * 100 as infection_rate
from Portfolio..CovidDeaths
where continent is not null
group by location, population
order by infection_rate desc

-- looking at countries with highest death count per population

select 
	Location, 
	cast(Max(total_deaths) as int) as death_count
from Portfolio..CovidDeaths
where continent is not null
group by location
order by death_count desc

-- breaking things down by continent
-- showing continents with the highest death count

select 
	location, 
	cast(Max(total_deaths) as int) as death_count
from Portfolio..CovidDeaths
where continent is null
group by location
order by death_count desc

-- for use in drill down

--select 
--	continent, 
--	cast(Max(total_deaths) as int) as death_count
--from Portfolio..CovidDeaths
--where continent is not null
--group by continent
--order by death_count desc

-- global numbers
-- filtered out 0 numbers to avoid dividing by 0

select 
	date,
	sum(new_cases) as total_cases,
	sum(new_deaths) as total_deaths,
	sum(cast(new_deaths as bigint)) / sum(new_cases) * 100 as death_percentage
from Portfolio..CovidDeaths
where continent is not null
and new_cases <> 0
group by date
order by 1, 2

-- total death percentage globally

select 
	sum(new_cases) as total_cases,
	sum(new_deaths) as total_deaths,
	sum(cast(new_deaths as bigint)) / sum(new_cases) * 100 as death_percentage
from Portfolio..CovidDeaths
where continent is not null
and new_cases <> 0
order by 1, 2

-- joining death and vaccination tables

select *
from Portfolio..CovidDeaths dea
join Portfolio..CovidVaccinations vac
	on dea.location = vac.location 
	and dea.date = vac.date

-- looking at total population vs vaccination (number of people in world who are vaccinated)

select 
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	vac.new_vaccinations,
	sum(cast(vac.new_vaccinations as bigint)) over (partition by dea.location order by dea.location, dea.date) as rolling_number_vaccinated
from Portfolio..CovidDeaths dea
join Portfolio..CovidVaccinations vac
	on dea.location = vac.location 
	and dea.date = vac.date
where dea.continent is not null
order by 2,3

-- Using CTE
-- number of columns in CTE must equal number of columns in the select statement within the CTE
-- using the cte allows us to use the new column we have created 'rolling_number_vaccinated' in the calculation 'rolling_number_vaccinated/population*100)

with PopvsVac (continent, location, date, population, new_vaccinations, rolling_number_vaccinated)
as
(
select 
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	vac.new_vaccinations,
	sum(cast(vac.new_vaccinations as bigint)) over (partition by dea.location order by dea.location, dea.date) as rolling_number_vaccinated
from Portfolio..CovidDeaths dea
join Portfolio..CovidVaccinations vac
	on dea.location = vac.location 
	and dea.date = vac.date
where dea.continent is not null
)

select *, (rolling_number_vaccinated/population * 100) as rolling_percentage_vaccinated
from PopvsVac

-- or can use a temporary table instead of CTE
-- must state the data type for each as we are creating a table from scratch

drop table if exists #PercentPopulationVaccinated  -- this line is added so that we can make ammendments to the table without worrying about deleting any tables
create table #PercentPopulationVaccinated
(
	continent nvarchar(255),
	location nvarchar(255),
	date datetime,
	population numeric,
	new_vaccinations numeric,
	rolling_number_vaccinated numeric
)
Insert into #PercentPopulationVaccinated
select 
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	vac.new_vaccinations,
	sum(cast(vac.new_vaccinations as bigint)) over (partition by dea.location order by dea.location, dea.date) as rolling_number_vaccinated
from Portfolio..CovidDeaths dea
join Portfolio..CovidVaccinations vac
	on dea.location = vac.location 
	and dea.date = vac.date
where dea.continent is not null
order by 2,3

select *, (rolling_number_vaccinated/population * 100) as rolling_percentage_vaccinated
from #PercentPopulationVaccinated

-- creating view to store data for later visualisations

create view PercentPopulationVaccinated as
select 
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	vac.new_vaccinations,
	sum(cast(vac.new_vaccinations as bigint)) over (partition by dea.location order by dea.location, dea.date) as rolling_number_vaccinated
from Portfolio..CovidDeaths dea
join Portfolio..CovidVaccinations vac
	on dea.location = vac.location 
	and dea.date = vac.date
where dea.continent is not null

-- needed to move view from master database to my portfolio database

USE master; -- this shows the code used to create the view so that it can be copied into the query below
GO
SELECT OBJECT_DEFINITION(OBJECT_ID('PercentPopulationVaccinated')) AS view_definition

use Portfolio -- this code saves the view in the portfolio database
go
create view PercentPopulationVaccinated as  select    dea.continent,   dea.location,   dea.date,   dea.population,   vac.new_vaccinations,   sum(cast(vac.new_vaccinations as bigint)) over (partition by dea.location order by dea.location, dea.date) as rolling_number_vaccinated  from Portfolio..CovidDeaths dea  join Portfolio..CovidVaccinations vac   on dea.location = vac.location    and dea.date = vac.date  where dea.continent is not null

-- this view can now be queried as it is permenant until deleted. Can now be used for visualisations.

 select *
 from PercentPopulationVaccinated