select cited_paper_id as paper_id, paper_title, concat(t1.name,', ',t2.authors) as authors, publication_year, venue, abstract
from (select cited_paper_id, cnt, paper_title, authorsinfo.name, venue, abstract, publication_year
      from (select cited_paper_id, count(cited_paper_id) as cnt
            from citation
            group by cited_paper_id
            order by count(cited_paper_id) DESC
            limit 20) as t0
      inner join researchpaper on cited_paper_id = researchpaper.paper_id
      inner join authorsinfo on researchpaper.author_id = authorsinfo.author_id) as t1
      
left join

(select paper_id, string_agg(name, ', ') as authors
from (select paper_id, name 
	 from coauthors
     inner join authorsinfo on authorsinfo.author_id = coauthors.author_id
     order by paper_id, rank) as temp 
group by paper_id) as t2
on cited_paper_id = t2.paper_id
order by cnt desc