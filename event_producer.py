#!/usr/bin/env python

# Code adapted from https://github.com/pushlog/RsvpAnalytics


# Kafka Library
from kafka import SimpleProducer, SimpleClient

# import requests for loading stream
import requests

# Kafka Producer
kafka = SimpleClient("localhost:9092")
producer = SimpleProducer(kafka)


def event_source():
    """
    1. Connects to meetup open_events stream.
    2. Receives the stream.
    3. Sends the stream as a producer.
    """

    while True:
        try:
            r = requests.get('http://stream.meetup.com/2/open_events',
                             stream=True)
            for line in r.iter_lines():
                # filter out keep-alive new lines
                if line:
                    # Send the stream to the topic "event_stream"
                    producer.send_messages("eventstream", line)
        # No matter what the Exception is keep calling the function recursively
        except Exception:
            event_source()


if __name__ == '__main__':
    event_source()
