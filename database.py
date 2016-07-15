#!/usr/bin/env python

# Code adapted from https://github.com/pushlog/RsvpAnalytics

from cqlengine import columns
from cqlengine.models import Model

# This code creates a class that allows us to access the Cassandra table

# Model Definition
class Eventstream(Model):
    event_id = columns.Text(primary_key=True)
    venue_city = columns.Text(partition_key=True)
    status = columns.Text(partition_key=True)
    venue_name = columns.Text()
    venue_lon = columns.Decimal(required=True)
    venue_lat = columns.Decimal(required=True)
    venue_id = columns.Integer()
    venue_zip = columns.Text()
    venue_state = columns.Text()
    venue_country = columns.Text()
    event_name = columns.Text()
    event_last_modified_time = columns.DateTime(required=False)
    event_url = columns.Text()
    group_name = columns.Text()
    group_url = columns.Text()
    group_id = columns.Integer()
    event_time = columns.DateTime()
    venue_addy1 = columns.Text()
    venue_addy2 = columns.Text()
    venue_addy3 = columns.Text()
    venue_phone = columns.Text()
    cat_name = columns.Text()
    cat_shortname = columns.Text()
    category_id = columns.Integer()
    yes_rsvp_count = columns.Integer()
