#!/usr/bin/env python

# Code adapted from https://github.com/pushlog/RsvpAnalytics

# Kakfa Library
from kafka import KafkaConsumer
# Custom Code Imports
from database import Eventstream  # Cassandra Model
from util import get_dict_val, load_json, datetime_from_epoch  # util functions
# Managing schemas
from cqlengine.management import sync_table, create_keyspace
# Setup connection
from cqlengine import connection

# Kafka broker
kafka_brokers_list = ["localhost:9092"]  # We can put multiple brokers here.
group_id = "meetup_event_stream"  # group id of topics

# Kafka Consumer
consumer = KafkaConsumer("eventstream", group_id=group_id)

# Connect to the meetup keyspace on our cluster running at 127.0.0.1
connection.setup(['127.0.0.1'], "meetup")
# create keyspace
# keyspace name, keyspace replication strategy, replication factor
create_keyspace('meetup', 'SimpleStrategy', 1)
# Sync your model with your cql table
sync_table(Eventstream)


def write_to_cassandra(**kwargs):
    """
    Write the data to cassandra.
    """
    try:
        Eventstream.create(
            status=get_dict_val(kwargs, 'status'),
            venue_name=get_dict_val(kwargs, 'venue_name'),
            venue_lon=get_dict_val(kwargs, 'venue_lon'),
            venue_lat=get_dict_val(kwargs, 'venue_lat'),
            venue_id=get_dict_val(kwargs, 'venue_id'),
            venue_zip=get_dict_val(kwargs, 'venue_zip'),
            venue_city=get_dict_val(kwargs, 'venue_city'),
            venue_state=get_dict_val(kwargs, 'venue_state'),
            venue_country=get_dict_val(kwargs, 'venue_country'),
            event_id=get_dict_val(kwargs, 'event_id'),
            event_name=get_dict_val(kwargs, 'event_name'),
            event_last_modified_time=get_dict_val(kwargs,
                                                  'event_last_modified_time'),
            event_url=get_dict_val(kwargs, 'event_url'),
            event_time=get_dict_val(kwargs, 'event_time'),
            group_name=get_dict_val(kwargs, 'group_name'),
            group_url=get_dict_val(kwargs, 'group_url'),
            group_id=get_dict_val(kwargs, 'group_id'),
            venue_addy1=get_dict_val(kwargs, 'venue_addy1'),
            venue_addy2=get_dict_val(kwargs, 'venue_addy2'),
            venue_addy3=get_dict_val(kwargs, 'venue_addy3'),
            venue_phone=get_dict_val(kwargs, 'venue_phone'),
            category_id=get_dict_val(kwargs, 'category_id'),
            cat_name=get_dict_val(kwargs, 'cat_name'),
            cat_shortname=get_dict_val(kwargs, 'cat_shortname'),
            yes_rsvp_count=get_dict_val(kwargs, 'yes_rsvp_count'))
    except Exception, e:
        print e


def fetch_data():
    """
    If data exists, load data using json.
    Get values out of json dict.
    Call write_to_cassandra to write the values to Cassandra.
    """
    if consumer:
        for message in consumer:
            message = load_json(message.value)  # Convert to json
            status = get_dict_val(message, 'status')
            event_id = get_dict_val(message, 'id')
            event_name = get_dict_val(message, 'name')
            event_url = get_dict_val(message, 'event_url')
            yes_rsvp_count = get_dict_val(message, 'yes_rsvp_count')
            # Venue hosting the event
            venue = get_dict_val(message, 'venue')
            if venue:
                venue_name = get_dict_val(venue, 'name')
                venue_lon = get_dict_val(venue, 'lon')
                venue_lat = get_dict_val(venue, 'lat')
                venue_id = get_dict_val(venue, 'id')
                venue_zip = get_dict_val(venue, 'zip')
                venue_city = get_dict_val(venue, 'city')
                venue_state = get_dict_val(venue, 'state')
                venue_country = get_dict_val(venue, 'country')
                venue_addy1 = get_dict_val(venue, 'address_1')
                venue_addy2 = get_dict_val(venue, 'address_2')
                venue_addy3 = get_dict_val(venue, 'address_3')
                venue_phone = get_dict_val(venue, 'phone')

            else:
                venue_name = None
                venue_lon = None
                venue_lat = None
                venue_id = None
                venue_zip = None
                venue_city = None
                venue_state = None
                venue_country = None
                venue_addy1 = None
                venue_addy2 = None
                venue_addy3 = None
                venue_phone = None

            # since epoch
            mtime = get_dict_val(message, 'mtime')
            if mtime:
                event_last_modified_time = datetime_from_epoch(mtime)
            else:
                event_last_modified_time = None

            e_time = get_dict_val(message, 'time')
            if e_time:
                event_time = datetime_from_epoch(e_time)
            else:
                event_time = None
            # Group hosting the event
            group = get_dict_val(message, 'group')
            if group:
                category = get_dict_val(group, 'category')
                if category:
                    category_id = get_dict_val(category, 'id')
                    cat_name = get_dict_val(category, 'name')
                    cat_shortname = get_dict_val(category, 'shortname')
                else:
                    category_id = None
                    cat_name = None
                    cat_shortname = None
                group_id = get_dict_val(group, 'id')
                group_name = get_dict_val(group, 'name')
                group_url = get_dict_val(group, 'urlname')
            else:
                group_id = None
                group_name = None
                group_url = None

            # Write data to Cassandra database
            write_to_cassandra(status=status,
                               event_time=event_time,
                               event_name=event_name,
                               event_id=event_id,
                               event_url=event_url,
                               venue_name=venue_name,
                               venue_lon=venue_lon,
                               venue_lat=venue_lat,
                               venue_id=venue_id,
                               venue_zip=venue_zip,
                               venue_city=venue_city,
                               venue_state=venue_state,
                               venue_country=venue_country,
                               event_last_modified_time=event_last_modified_time,
                               group_id=group_id,
                               group_name=group_name,
                               group_url=group_url,
                               venue_addy1=venue_addy1,
                               venue_addy2=venue_addy2,
                               venue_addy3=venue_addy3,
                               venue_phone=venue_phone,
                               category_id=category_id,
                               cat_name=cat_name,
                               cat_shortname=cat_shortname,
                               yes_rsvp_count=yes_rsvp_count
                               )


if __name__ == '__main__':
    fetch_data()
