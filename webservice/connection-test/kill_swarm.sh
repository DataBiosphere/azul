#!/bin/bash

for pid in $(ps -ef | grep "python *connection.py" | awk '{print $2}'); do kill -9 $pid; done
ps aux

