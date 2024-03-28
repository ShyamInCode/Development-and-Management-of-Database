select a1.name, a2.name
from (select t1.author_id as X, t2.author_id as Y--, count(*)
      from (select paper_id, author_id from researchpaper
          union 
          select paper_id, author_id from coauthors) as t1,
          (select paper_id, author_id from researchpaper
          union 
          select paper_id, author_id from coauthors) as t2
      where t1.paper_id = t2.paper_id and t1.author_id < t2.author_id
      group by t1.author_id, t2.author_id
      having count(*) > 1) as temp
join authorsinfo as a1 on X = a1.author_id
join authorsinfo as a2 on Y = a2.author_id