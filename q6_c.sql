select a1.name as X, a2.name as Y, a3.name as Z, cnt as "COUNT"
from
  (select *, count(*) as cnt
  from
    (SELECT least(f1.author_id, f2.author_id, f3.author_id) AS X,
            CASE least(f1.author_id, f2.author_id, f3.author_id) WHEN f1.author_id THEN least(f2.author_id, f3.author_id)
              WHEN f2.author_id THEN least(f1.author_id, f3.author_id)
              ELSE least(f1.author_id, f2.author_id)
              END AS Y,
            greatest(f1.author_id, f2.author_id, f3.author_id) AS Z
    from ordered_citation as t1
    join ordered_citation as t2 on t1.p2 = t2.p1
    join ordered_citation as t3 on t1.p1 = t3.p1 and t2.p2 = t3.p2
    join full_authors as f1 on t1.p1 = f1.paper_id
    join full_authors as f2 on t1.p2 = f2.paper_id
    join full_authors as f3 on t2.p2 = f3.paper_id) as t1
  where X < Y and Y < Z
  group by X,Y,Z) as t2
join authorsinfo as a1 on X = a1.author_id
join authorsinfo as a2 on Y = a2.author_id
join authorsinfo as a3 on Z = a3.author_id
where a1.name != 'NOT SPECIFIED' and a2.name != 'NOT SPECIFIED' and a3.name != 'NOT SPECIFIED' 