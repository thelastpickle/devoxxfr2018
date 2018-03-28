CREATE KEYSPACE myks WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 3};

CREATE TABLE myks.workflow(
	 id_flux varchar,
	 id_etape varchar,  
	 start_time timestamp,
	 end_time timestamp,
	 status int,
	 PRIMARY KEY (id_flux, id_etape, start_time)
) WITH CLUSTERING ORDER BY (id_etape ASC, start_time DESC);

