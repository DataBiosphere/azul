#!/bin/bash
WD=`pwd`
cd /app/test/data_generator
python3 /app/test/data_generator/make_fake_data.py /app/test/data_generator/fake_data_template.json 50000 1
cd ${WD}