#!/bin/bash

pip install SQLAlchemy
pip install psycopg2

for i in {1..1000} 
do
    # Overrun db with connections that get left open
    python spawn_open_connection.py &
    echo "Evil swarm approaching number $i"

    # Swarm of connections that close before sleeping
    #python spawn_closed_connection.py &
    #echo "Courteous swarm approaching number $i"
done
