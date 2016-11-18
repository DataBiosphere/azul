#!/bin/bash
for i in {1..10}
do
	PYTHONPATH='' luigi --module spawned spawnFlop --project treehouse --donor-id $i --sample-id $i --pipeline-name RNAseq
done
