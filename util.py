#!/usr/bin/env python

# Code adapted from https://github.com/pushlog/RsvpAnalytics

import json
import datetime


def get_dict_val(d, key_name):
    """
    Receives the arguments = dict, key_name
    Returns the value
    """
    return d.get(key_name)


def load_json(msg):
    """
    Load the message value to json.
    """
    return json.loads(msg)


def datetime_from_epoch(time):
    """
    converts datetime from cassandra to datetime from epoch
    :param time: datetime from cassandra
    :return: python datetime value
    """
    return datetime.datetime.utcfromtimestamp(float(time) / 1000.0)

# Silly little functions for the slider bar
def radius(radius):
    return radius

def months(months):
    return months

def days(days):
    return days