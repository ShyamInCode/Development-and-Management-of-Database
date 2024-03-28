select X, Z, paper_title,concat(t1.name,', ',t2.authors) as "Authors", publication_year, venue, abstract

from (select X, Z, paper_title, authorsinfo.name, venue, abstract, publication_year
      from (select c2.cited_paper_id as X, c1.paper_id as Z
            from citation as c1, citation as c2
           	where c1.cited_paper_id = c2.paper_id) as t0
      inner join researchpaper on Z = researchpaper.paper_id
      inner join authorsinfo on researchpaper.author_id = authorsinfo.author_id) as t1 

left join

(select paper_id, string_agg(name, ', ') as authors
from (select paper_id, name 
	 from coauthors
     inner join authorsinfo on authorsinfo.author_id = coauthors.author_id
     order by paper_id, rank) as temp
group by paper_id) as t2

on Z = t2.paper_id
order by X, Z