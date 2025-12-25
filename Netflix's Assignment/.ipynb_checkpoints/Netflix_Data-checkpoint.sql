-- Create the Table again, first drop the table then make it again 
CREATE TABLE dbo.Netflix_raw (
    show_id        NVARCHAR(10)  Primary key,
    [type]         VARCHAR(10)   NULL,
    title          NVARCHAR(200) NULL,
    director       VARCHAR(250)  NULL,
    [cast]         VARCHAR(1000) NULL,
    country        VARCHAR(150)  NULL,
    date_added     VARCHAR(30)   NULL,
    release_year   INT           NULL,
    rating         VARCHAR(10)   NULL,
    duration       VARCHAR(20)   NULL,
    listed_in      VARCHAR(100)  NULL,
    description    VARCHAR(500)  NULL
);
GO

select * from [dbo].[Netflix_raw]
where show_id = 's5023';

-- Handling Foreign Characters:

-- Remove The Duplicate:
-- for Checking the show_id..
select show_id,count(*)
from netflix_raw
group by show_id
having count(*)>1;

-- for checking the title

select * from Netflix_raw
where concat(upper(title),type) in (
select concat(upper(title) ,type)
from Netflix_raw
group by concat(upper(title) ,type)
having count(*)>1
)
order by title;


with cte as (
select *
, row_number() over (partition by title, type order by show_id) as rn
from Netflix_raw
)
select *
from cte
where rn = 1;


SELECT 
    show_id, 
    TRIM(value) AS cast
INTO netflix_cast 
FROM netflix_raw
CROSS APPLY STRING_SPLIT(cast, ',');

select * from netflix_director;

-- datatype conversion for date added:
with cte as (
select *
, row_number() over (partition by title, type order by show_id) as rn
from Netflix_raw
) 
select show_id, type, title, cast(date_added as date) as date_added, release_year,
rating, duration, description
from cte
where rn = 1;



-- New table for listed in, director, cast, country:

select show_id , trim(value) as netflix_cast
into netflix_cast
from netflix_raw
cross apply string_split(cast,',')


-- populate missing values in in country, duration columns:
insert into netflix_country
select  show_id,m.country 
from netflix_raw nr
inner join (
select director,country
from  netflix_country nc
inner join netflix_director nd on nc.show_id=nd.show_id
group by director,country
) m on nr.director=m.director
where nr.country is null
;

select * from netflix_raw where director='Christopher Storer';

select director,country
from  netflix_country nc
inner join netflix_director nd on nc.show_id=nd.show_id
group by director,country
;
--populate rest of the nulls as not_available

--drop columns director , listed_in,country,cast

select * from netflix_raw where duration is null

with cte as (
select *
, row_number() over (partition by title, type order by show_id) as rn
from Netflix_raw
) 
select show_id, type, title, cast(date_added as date) as date_added, release_year,
rating, case when duration is null then rating else duration end as duration, description
into Netflix
from cte
;
select * from Netflix

-- Netflix Data Analysis:-

--1) for each director count the no of movies and created by them in in seperate columns
-- for director who have created tv shows and movies both.

select nd.director 
,COUNT(distinct case when n.type='Movie' then n.show_id end) as no_of_Movies
,COUNT(distinct case when n.type='TV Show' then n.show_id end) as no_of_Tv_Shows
from netflix n
inner join netflix_director nd on n.show_id=nd.show_id
group by nd.director
having COUNT(distinct n.type)>1

--2) which country has the highest number of comedy movies..
select  top 1 nc.country , COUNT(distinct nl.show_id ) as no_of_movies
from netflix_listed_in nl
inner join netflix_country nc on nl.show_id=nc.show_id
inner join netflix n on nl.show_id=nc.show_id
where nl.listed_in ='Comedies' and n.type='Movie'
group by  nc.country
order by no_of_movies desc

--3) for each year (as per date added to netflix) which director has maximum number of movies 
-- released.
WITH cte AS (
    SELECT 
        nd.director,
        YEAR(n.date_added) AS date_year,
        COUNT(n.show_id) AS no_of_movies
    FROM netflix n
    INNER JOIN netflix_director nd ON n.show_id = nd.show_id
    WHERE n.type = 'Movie'
    GROUP BY nd.director, YEAR(n.date_added)
),
cte2 AS (
    SELECT 
        director,
        date_year,
        no_of_movies,
        ROW_NUMBER() OVER(PARTITION BY date_year ORDER BY no_of_movies DESC, director ASC) AS rn
    FROM cte
)
SELECT * FROM cte2 
WHERE rn = 1;



-- 4) What duration of movies in each Genre.

select nl.listed_in , avg(cast(REPLACE(duration,' min','') AS int)) as avg_duration
from netflix n
inner join netflix_listed_in nl on n.show_id=nl.show_id
where type='Movie'
group by nl.listed_in



--5) find the listed of directors who have created the horror movies and comedy both.
-- display the director name along with number of comedy and horror movies directed by them.

select 
    nd.director, 
    count(distinct case when nl.listed_in = 'comedies' then n.show_id end) as no_of_comedy, 
    count(distinct case when nl.listed_in = 'horror movies' then n.show_id end) as no_of_horror
from netflix n
inner join netflix_listed_in nl on n.show_id = nl.show_id
inner join netflix_director nd on n.show_id = nd.show_id
where n.type = 'movie' and nl.listed_in in ('comedies', 'horror movies')
group by nd.director
having count(distinct nl.listed_in) = 2;



-- 6) how many movies and TV shows were added each year.:
select 
    year(date_added) as year_added, 
    type, 
    count(show_id) as total_titles
from netflix
where date_added is not null
group by year(date_added), type
order by year_added desc, type;

--7) Identifying Content with Longest vs Shortest Durations:


select 
    title, 
    type,
    duration,
    case 
        when cast(replace(duration, ' min', '') as int) > 150 then 'epic/long movie'
        when cast(replace(duration, ' min', '') as int) < 30 then 'short film'
        else 'standard length'
    end as duration_category
from netflix
where type = 'movie' and duration is not null
order by cast(replace(duration, ' min', '') as int) desc;