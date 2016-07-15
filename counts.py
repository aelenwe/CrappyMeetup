#!/usr/bin/env python

from __future__ import division
import psycopg2
from psycopg2.extensions import AsIs
from cqlengine import connection
from database import Eventstream
import datetime
from datetime import timedelta

# Cassandra connection
connection.setup(['127.0.0.1'], "meetup", protocol_version=3)

# Postgres connection
pg_conn = psycopg2.connect("dbname='sf311' user='aelenwe' host='localhost'")
# Postgres Cursor
pg_cur = pg_conn.cursor()

# Query Cassandra for Meetups in San Francisco
meetup_loc = Eventstream.objects(venue_city='San Francisco', status='upcoming')


def fetch_poop(months=6, radius=100, days=30):
    """
    Count the number of poops with in the radius during the timeframe.
    This uses Postgres' point-based earth distances which returns distance in
    miles. For more information visit: https://goo.gl/u5wnX7
    Also, creates the pop-up content for the maps.
    :param months: integer # of months of 311 data to use
    :param radius: integer # of feet radius around each meetup location to look for poop
    :param days: integer # of days of meetups from now to include in the query
    :return: list of tuples for each meetup
    """
    today = datetime.datetime.today()
    # Postgres point-based earth distance returns miles. There are 5280 feet in a mile.
    feet = radius / 5280
    meetup_poops = []
    # For meetup in our Cassandra database
    for meetup in meetup_loc:
        # Event time is in UTC time. This converts to PDT.
        etime = meetup.event_time - timedelta(hours=7)
        # Check to see if the meetup is between now and our end period
        if today < etime < today + timedelta(days=days):  # meetup.venue_lat and
            meetup_lat = float(meetup.venue_lat)
            meetup_lon = float(meetup.venue_lon)
            loc = (meetup_lat, meetup_lon)
            # Handle encoding issues
            name = meetup.event_name.encode(encoding='ascii', errors='ignore')
            # Truncate name for pop-up
            name = '{:.50}'.format(name) + '...' if len(name) > 47 else name
            event_cat = meetup.cat_name
            url = meetup.event_url
            rsvps = meetup.yes_rsvp_count
            # Postgres query for poops within the radius of the meetup location
            # and in the time period
            pg = ("SELECT COUNT(*) FROM sf311 "
                  "WHERE point(lon,lat) <@> point(%s,%s) <= %s "
                  "and Opened > current_date - interval '1 month' * %s;")
            # Execute the query
            pg_cur.execute(pg, (AsIs(meetup_lon), AsIs(meetup_lat), AsIs(feet), AsIs(months)))
            # Fetch the results
            poops = pg_cur.fetchall()
            # Loop over the results. Count the number of poops and create text for map pop-up
            for poop in poops:
                poop_count = int(poop[0])
                html = ("Meetup: <a href=" + url + " target='_blank'>" + name + "</a><br>" +
                        "Event Time: " + str(etime) + " PDT <br>" +
                        "Category: " + str(event_cat) + "<br>" +
                        "RSVPS: " + str(rsvps) + "<br>" +
                        "Reports of Human Waste: " +
                        str(poop_count) +
                        "<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;distance: {0} feet <br>".format(radius) +
                        "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;time frame: {0} months ".format(months))
                meetup_poops.append((loc, html, poop_count))
    return meetup_poops


def fetch_all_poop(months=6):
    """
    Returns a list of [latitude, longitude] pairs for use in the heatmap

    :param months: integer # of months of 311 data to include
    :return: list of lists for each meetup
    """
    all_poop = []
    pg_poop = ("SELECT lat, lon FROM sf311 "
               "where opened > current_date - interval '1 month' * %s;")
    pg_cur.execute(pg_poop, (months,))
    poop = pg_cur.fetchall()
    for poo in poop:
        all_poop.append([poo[0], poo[1]])
    return all_poop


def counts_by_year():
    """
    Return a list of lists that contains the year and the count of
    reports of human waste in that year
    :return: list of lists
    """
    year_count = []
    # Postgres query that pulls the year out of a datetime field
    # and returns the year and the count
    pg_year = ("select extract(year from opened), count(*) "
               "from sf311 group by extract(year from opened);")

    pg_cur.execute(pg_year)
    years = pg_cur.fetchall()
    for year in years:
        year_count.append([year[0], year[1]])
    return year_count

if __name__ == '__main__':
    fetch_poop()
    fetch_all_poop()
