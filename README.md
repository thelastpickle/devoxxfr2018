# devoxxfr2018
Hands on lab Devoxx FR 2018 : initiation à Apache Cassandra 

CREATE TABLE movielens.movies_by_first_letter (
    first_letter text, 
    first_word text, 
    id uuid,
    avg_rating float,
    genres set<text>,
    name text,
    release_date date,
    url text,
    video_release_date date,
    PRIMARY KEY (first_letter, first_word, id)
) WITH CLUSTERING ORDER BY (first_word ASC, id ASC);
