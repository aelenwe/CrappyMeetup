
### To Start Streaming

1. Start Zookeeper
	zkserver start
2. Start Kafka
	kafka-server-start /usr/local/etc/kafka/server.properties
3. Start Cassandra
	./apache-cassandra-2.1.14/bin/cassandra -f
4. Start Producer
5. Start Consumer

### To copy new csv into Postgres

After downloading a new CSV with 311 data, run `clean_up_sf311.py` to 
generate table ready for import to postgres. 
  
In postgres:
```
        CREATE TABLE tmp_sf311 (
          CaseID bigint NOT NULL,
          Opened timestamp,
          Closed timestamp,
          Updated timestamp,
          Status varchar(10),
          status_notes varchar(5000),
          Responsible_Agency varchar(80),
          Category varchar(80),
          Request_Type varchar(100),
          Request_Details varchar(100),
          Address varchar(255),
          Supervisor_District bigint,
          Neighborhood varchar(80),
          Location Point,
          Source varchar(80),
          Media_URL varchar(255),
          Lat double precision,
          Lon double precision,
          PRIMARY KEY (CaseID)
          );
          
        \COPY tmp_sf311 FROM 'new_311.csv' DELIMITER ',' CSV HEADER;
```
If import was successful:
```
        DROP TABLE sf311_old;
        ALTER TABLE sf311 RENAME TO sf311_old;
        ALTER TABLE tmp_sf311 RENAME TO sf311;
```
