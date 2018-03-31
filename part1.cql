CREATE KEYSPACE myks WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 3};

CREATE TABLE myks.workflow(
	 id_flux varchar,
	 id_etape varchar,  
	 start_time timestamp,
	 end_time timestamp,
	 status int,
	 PRIMARY KEY (id_flux, id_etape, start_time)
) WITH CLUSTERING ORDER BY (id_etape ASC, start_time DESC);

---


CREATE KEYSPACE myks WITH REPLICATION = 
	{‘class’:’SimpleStrategy’, ‘replication_factor’: 3}   


CREATE KEYSPACE myks WITH REPLICATION = 
	{‘class’:‘NetworkTopologyStrategy’, ‘dc1’: 3, ‘dc2’: 3}   


CREATE TABLE IF NOT EXISTS myks.workflow(
	 id_flux varchar,
	 id_etape varchar,  
	 start_time timestamp,
	 end_time timestamp,
	 status int,
	 PRIMARY KEY (id_flow, id_step, start_time)
);


NSERT INTO myks.workflow
(id_flux, id_etape, start_time)
VALUES (‘7172A’, ‘345-131’, ‘2018-03-19 15:32’)


INSERT INTO myks.workflow
(id_flux, id_etape, start_time)
VALUES (‘4864C’, ‘345-131’, ‘2018-03-19 19:45’)


CREATE TABLE myks.workflow(
	 id_flux varchar,
	 id_etape varchar,  
	 start_time timestamp,
	 end_time timestamp,
	 status int,
	 PRIMARY KEY (id_flux, id_etape, start_time)
) WITH CLUSTERING ORDER BY (id_etape ASC, start_time DESC);

SELECT * FROM myks.workflow
WHERE id_flux = ‘value’


SELECT * FROM myks.workflow 
WHERE id_flux = ‘value’ AND id_etape = ‘step’


SELECT * FROM myks.workflow 
WHERE id_flux = ‘value’ AND id_etape = ‘step’ 
AND start_time >= ‘2018-03-19 15:00’ AND start_time < ‘2018-03-19 16:00’;

CREATE TABLE myks.workflow_by_start_time(
	 id_flux varchar,
	 id_etape varchar,  
	 start_time timestamp,
	 end_time timestamp,
	 status int,
	 PRIMARY KEY (id_flux, start_time, id_etape)
) WITH CLUSTERING ORDER BY (start_time DESC, id_etape ASC);


SELECT * FROM myks.workflow_by_start_time
WHERE id_flux = ‘value’
AND start_time >= ‘2018-03-19 15:00’ AND start_time < ‘2018-03-19 16:00’;


SELECT * FROM myks.workflow_by_start_time 
WHERE id_flux = ‘value’ AND start_time = 1520958714000 AND id_etape = ‘step’


INSERT INTO myks.workflow 
(id_flux, id_etape, start_time)
VALUES (‘flow1’, ’step1’, ‘2018-03-19 12:00’);

INSERT INTO myks.workflow 
(id_flux, id_etape, start_time, end_time)
VALUES (‘flow1’, ’step1’, ‘2018-03-19 12:00’, ‘2018-03-19 12:10’);
Est équivalente à celle-ci :
UPDATE myks.workflow 
SET end_time = ‘2018-03-19 12:10’
WHERE id_flux=‘flow1’ AND id_etape = ’step1’ AND start_time = ‘2018-03-19 12:00’;

CREATE TABLE table_with_list (id int PRIMARY KEY, my_list list<text>);


UPDATE table_with_list SET my_list = my_list + [‘value1’,’value3’]
WHERE id = 1;

UPDATE table_with_list SET my_list = my_list + [‘value2’,’value3’]
WHERE id = 1;


select * from table_with_list;

CREATE TABLE table_with_set (id int PRIMARY KEY, my_set set<text>);


UPDATE table_with_set SET my_set = my_set + {‘value3’,’value1’}
WHERE id = 1;

UPDATE table_with_set SET my_set = my_set + {‘value2’,’value3’}
WHERE id = 1;

select * from table_with_set;

CREATE TABLE table_with_map 
(id int PRIMARY KEY, my_map map<text, int>);


UPDATE table_with_map SET my_map[‘key1’] = 1
WHERE id = 1;

UPDATE table_with_map SET my_map[‘key2’] = 2
WHERE id = 1;

select * from table_with_map;

CREATE TABLE table_with_counter 
(id int PRIMARY KEY, my_counter counter);
UPDATE table_with_counter SET my_counter = my_counter + 10
WHERE id = 1;

UPDATE table_with_counter SET my_counter = my_counter + 15
WHERE id = 1;


select * from table_with_counter;


CREATE TABLE users(
   username text,
   email text,
   name text,
   age int,
PRIMARY KEY(username));

CREATE TABLE users(
   username text,
   email text,
   name text,
   age int,
PRIMARY KEY(username));


UPDATE users
SET age = 41
WHERE username = ‘adejanovski’
IF age = 40

INSERT INTO myks.workflow (id_flux, id_etape, start_time, end_time)
VALUES (‘flow1’, ’step1’, ‘2018-03-19 12:00’, ‘2018-03-19 12:10’)
USING TTL 86400

UPDATE myks.workflow 
USING TTL 86400
SET end_time = ‘2018-03-19 12:10’
WHERE id_flux=‘flow1’ AND id_etape = ’step1’ 
AND start_time = ‘2018-03-19 12:00’;

CREATE TABLE IF NOT EXISTS myks.workflow_by_day(
	id_flux varchar,
	day varchar, 
	id_etape varchar,  
	start_time timestamp,
	end_time timestamp,
	status int,
	PRIMARY KEY ((id_flux, day), id_etape, start_time)
);

CREATE TABLE IF NOT EXISTS myks.workflow(
	id_flux varchar,
	id_etape varchar,  
	start_time timestamp,
	end_time timestamp,
	status int,
	PRIMARY KEY (id_flux, id_etape, start_time)
);
CREATE INDEX workflow_status ON myks.workflow (status);

select * from myks.workflow where status = 1;



