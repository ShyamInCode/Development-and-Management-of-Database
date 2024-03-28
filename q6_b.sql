drop table if exists full_authors;
create table full_authors as 
select paper_id, author_id from researchpaper
union 
select paper_id, author_id from coauthors;