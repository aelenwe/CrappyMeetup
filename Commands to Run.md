
### To Start Streaming

1. Start Zookeeper
	`$ zkserver start`
2. Start Kafka
	`$ kafka-server-start /usr/local/etc/kafka/server.properties`
3. Start Cassandra
	`$ ./apache-cassandra-2.1.14/bin/cassandra -f`
4. Start Producer
	`$ python event_producer.py`
5. Start Consumer
	`$ python event_consumer.py`

### To copy new csv into Postgres

After downloading a new copy of the csv of the 311 data to the project directory run `$ python clean_up_sf311.py`

