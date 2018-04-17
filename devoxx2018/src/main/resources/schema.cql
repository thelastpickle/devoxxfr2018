CREATE KEYSPACE devoxx
WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':3};

CREATE TABLE IF NOT EXISTS devoxx.messages(
  id_queue int,
  id_message timeuuid,
  payload varchar,
  published_by text,
  PRIMARY KEY (id_queue, id_message)
) WITH CLUSTERING ORDER BY (id_message ASC);

CREATE TABLE IF NOT EXISTS devoxx.messages_ack(
  id_queue int,
  id_message timeuuid,
  payload varchar,
  published_by text,
  processed_by list<text>,
  PRIMARY KEY (id_queue, id_message)
) WITH CLUSTERING ORDER BY (id_message DESC)
AND dclocal_read_repair_chance = 0;
