drop table if exists ordered_citation;
create table ordered_citation as 
select case when paper_id > cited_paper_id then cited_paper_id else paper_id end p1,
	   case when paper_id < cited_paper_id then cited_paper_id else paper_id end p2
from citation;