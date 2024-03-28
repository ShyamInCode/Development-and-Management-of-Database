select cited_paper_id as "Cited Paper", cp as "Paper that cited", 
paper_title,concat(t1.name,', ',t2.authors) as "Authors", publication_year, venue, abstract

-- t1 has all details about the paper which cites except the coauthor names
from (select cited_paper_id, citation.paper_id as cp, paper_title, authorsinfo.name, venue, abstract, publication_year
      from citation
      inner join researchpaper on citation.paper_id = researchpaper.paper_id
      inner join authorsinfo on researchpaper.author_id = authorsinfo.author_id) as t1 

-- coauthor names are joined
left join

-- t2: coauthors names for each paper
(select paper_id, string_agg(name, ', ') as authors
from (select paper_id, name 
	 from coauthors
     inner join authorsinfo on authorsinfo.author_id = coauthors.author_id
     order by paper_id, rank) as temp
group by paper_id) as t2

on cp = t2.paper_id 
order by cited_paper_id,cp