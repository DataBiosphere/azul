#!/bin/bash

pip install SQLAlchemy
pip install psycopg2

for i in {1..1000} 
do
    echo "Running ddos.py $i"

    # Overrun db with connections that get left open
    #python spawn_open_connection.py &

    # Swarm of connections that close before sleeping
    #python spawn_closed_connection.py &
done
