#!/usr/bin/env python
from __future__ import print_function

import json
import os
import random
import time
import sys
from kafka import KafkaProducer
from random import uniform
from card_generator import generate_card

KAFKA_TOPIC = os.getenv("EVENTADOR_KAFKA_TOPIC", "payment_auths")
KAFKA_BROKERS = os.getenv("EVENTADOR_BOOTSTRAP_SERVERS", "localhost:9092")

# Setup producer connection
producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                         bootstrap_servers=KAFKA_BROKERS)

print("connected to {} topic {}".format(KAFKA_BROKERS, KAFKA_TOPIC))

def get_lat():
    """ return random lat """
    return round(uniform(-180,180), 7)

def get_lon():
    """ return random lon """
    return round(uniform(-90, 90), 7)

def purchase():
    """Return a random amount in cents """
    return random.randrange(1000, 90000)

def get_user():
    """ return a random user """
    return random.randrange(0, 999)

def make_fraud():
    """ return a fraudulent transaction """
    lat = get_lat()
    lon = get_lon()
    user = get_user()
    amount = "942"
    card = generate_card("visa16")
    payload = {"userid": user,
               "amount": amount,
               "lat": lat,
               "lon": lon,
               "card": card
              }
    return payload

def sendto_eventador(payload):
    print(payload)
    """Add a message to the produce buffer asynchronously to be sent to Eventador."""
    try:
        producer.send(KAFKA_TOPIC, payload)
    except:
        print("unable to produce to {} topic {}".format(KAFKA_BROKERS, KAFKA_TOPIC))


payload = {}
fraud_trigger = 15
i = 1
while True:
    try:

        # normal activity
        payload = {"userid": get_user(),
                   "amount": purchase(),
                   "lat": get_lat(),
                   "lon": get_lon(),
                   "card": generate_card("visa16")
                   }
        sendto_eventador(payload)

        # make some fake fraud
        if i % fraud_trigger == 0:
            print("making fraud")
            payload = make_fraud()
            for r in range(3):
                sendto_eventador(payload)

        # Flush the produce buffer and send to kafka
        producer.flush()
        i += 1
        time.sleep(3)

    except KeyboardInterrupt:
        sys.exit()
