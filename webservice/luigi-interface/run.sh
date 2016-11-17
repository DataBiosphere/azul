#!/bin/bash
for i in {1..10}
do
	PYTHONPATH='' luigi --module spawned spawnFlop --integer $i 
done
