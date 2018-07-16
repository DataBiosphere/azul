#!/bin/bash
WD=`pwd`
cd /app/test/service/data_generator
python3 /app/test/service/data_generator/make_fake_data.py /app/test/service/data_generator/fake_data_template.json 5000 6
cd ${WD}
