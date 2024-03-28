import psycopg2
import psycopg2.extras
import csv
import re
import pandas as pd
from ordered_set import OrderedSet

hostname = 'localhost'
port_id = 5432
conn = None

def load_citation(df):
    data = []
    temp = df[['ID','RefIDs']]
    temp = temp[temp.RefIDs.notna()]
    
    for i in temp.index:
        l = set(re.split(",", temp['RefIDs'][i]))
        for j in l:
            if(temp['ID'][i] != j):
                data.append([temp['ID'][i], j])
    citation_table = pd.DataFrame(data, columns = ['ID', 'RefIDs'])
    return citation_table
    
def load_paper(df):
    main_authors_data = []
    coauthors_data = []
    author_list = OrderedSet([])

    authorIDs = {"NOT SPECIFIED" : 0}
    var = 1

    for i in df.index:
        l = OrderedSet(re.split(",", df['Authors'][i]))
        
        if(not authorIDs.get(l[0])):
            authorIDs[l[0]] = var
            var += 1
        main_authors_data.append([df.ID[i], authorIDs[l[0]]])
        
        for j in range(1,len(l)):
            if(not authorIDs.get(l[j])):
                authorIDs[l[j]] = var
                var += 1
            coauthors_data.append([df.ID[i], authorIDs[l[j]],j])
            
    main_author = pd.DataFrame(main_authors_data, columns = ['ID', 'AuthorID'])
    coauthor_table = pd.DataFrame(coauthors_data, columns = ['ID', 'AuthorID', 'Rank'])
    df['Authors'] = main_author['AuthorID']
    paper = df[['ID', 'Title', 'Authors', 'Year', 'Venue', 'Abstract']]
    return paper, coauthor_table, authorIDs

with psycopg2.connect(
            host = hostname,
            port = port_id) as conn:

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curr:
        filename = 'data.csv'
        df = pd.read_csv(filename)
        
        df['Authors'] = df['Authors'].fillna('NOT SPECIFIED')
        df['Abstract'] = df['Abstract'].fillna('NOT SPECIFIED')
        df['Venue'] = df['Venue'].fillna('NOT SPECIFIED')
        
        paper_table, coauthor_table, authorIDs = load_paper(df)
        citation_table = load_citation(df)
        tables_script =  '''DROP TABLE IF EXISTS ResearchPaper cascade;
                            DROP TABLE IF EXISTS AuthorsInfo cascade;
                            DROP TABLE IF EXISTS CoAuthors;
                            DROP TABLE IF EXISTS Citation;

                            CREATE TABLE AuthorsInfo
                            (
                              author_id INT NOT NULL,
                              name VARCHAR(500) NOT NULL,
                              PRIMARY KEY (author_id)
                            );

                            CREATE TABLE ResearchPaper
                            (
                              paper_id INT NOT NULL,
                              paper_title VARCHAR(500) NOT NULL,
                              author_id INT NOT NULL,
                              publication_year INT NOT NULL,
                              venue VARCHAR(1000),
                              abstract VARCHAR(65000),
                              PRIMARY KEY (paper_id),
                              FOREIGN KEY (author_id) REFERENCES AuthorsInfo(author_id)
                            );

                            CREATE TABLE CoAuthors
                            (
                              paper_id INT NOT NULL,
                              author_id INT NOT NULL,
                              rank INT NOT NULL,
                              PRIMARY KEY (paper_id, rank),
                              FOREIGN KEY (paper_id) REFERENCES ResearchPaper(paper_id),
                              FOREIGN KEY (author_id) REFERENCES AuthorsInfo(author_id)
                            );

                            CREATE TABLE Citation
                            (
                              paper_id INT NOT NULL,
                              cited_paper_id INT NOT NULL,
                              PRIMARY KEY (paper_id, cited_paper_id),
                              FOREIGN KEY (paper_id) REFERENCES ResearchPaper(paper_id),
                              FOREIGN KEY (cited_paper_id) REFERENCES ResearchPaper(paper_id)); '''
        
        papers_script  = 'INSERT INTO ResearchPaper (paper_id, paper_title, author_id, publication_year, venue, abstract) VALUES (%s, %s, %s, %s, %s, %s)'
        authors_script = 'INSERT INTO AuthorsInfo (author_id, name) VALUES (%s, %s)'
        citations_script = 'INSERT INTO Citation (paper_id, cited_paper_id) VALUES (%s, %s)'
        coauthors_script = 'INSERT INTO CoAuthors (paper_id, author_id, rank) VALUES (%s, %s, %s)'

        curr.execute(tables_script)
        for key,value in authorIDs.items():
            curr.execute(authors_script, list((value,key)))
        print("AuthorInfo table is populated. 3 more tables to go.")

        for i in paper_table.index:
            l = list(paper_table.loc[i])
            l[0] = int(l[0])
            l[2] = int(l[2])
            l[3] = int(l[3])
            curr.execute(papers_script, l)
        print("ResearchPaper table is populated. 2 more tables to go.")

        for i in citation_table.index:
            l = list(citation_table.loc[i])
            l[0] = int(l[0])
            l[1] = int(l[1])
            curr.execute(citations_script, l)
        print("Citation table is populated. 1 more tables to go.")
        
        for i in coauthor_table.index:
            l = list(coauthor_table.loc[i])
            l[0] = int(l[0])
            l[1] = int(l[1])
            l[2] = int(l[2])
            curr.execute(coauthors_script, l)
        print("CoAuthors table is populated. DONE, all tables are populated.")
