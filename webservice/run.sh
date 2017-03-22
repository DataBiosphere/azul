#!/bin/sh

while ! pg_isready -U postgres -h db
do
echo "$(date) - waiting for database to start"
    sleep 2
done

#sed -i "s/___name___/$AWS_PROFILE/g" /etc/boto.cfg/credentials
#sed -i "s/___awskey___/$AWS_ACCESS_KEY_ID/g" /etc/boto.cfg/credentials
#sed -i "s/___secretkey___/$AWS_SECRET_ACCESS_KEY/g" /etc/boto.cfg/credentials
