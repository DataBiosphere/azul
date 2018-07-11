# Sample usage
# make testme -- Run tests using chalice in localmode against an ES instance as defined in docker-compose.yml
#
# make travistest -- Run tests using chalice in localmode against an ES instance residing on the docker host, as defined in docker-compose-hostnetworking.yml

# make -- Run using chalice in localmode against an ES instance as defined in docker-compose.yml.  No test data will be populated.

all: stop reset run
	# Run the dashboard service and elastic search within local docker 
	# containers.  Do not populate the ES instance with test data.
	# docker-compose.yml can be edited to specify a different (e.g. remote)
	# ES instance

run-travis:
	# Start chalice in localmode with host-mode networking
	docker-compose -f docker-compose-hostnetworking.yml up -d --build --force-recreate

populate:
	docker-compose exec dcc-dashboard-service /app/test/data_generator/make_fake_data.sh

reset:
	docker-compose stop
	docker-compose rm -f

stop:
	docker-compose down --rmi 'all'

travistest: stop reset run-travis populate
	# Run tests locally, against an already-existing ES instance located
	# on the docker host and listening on 127.0.0.1.  Test data will be 
	# generated and loaded into the db. (ES connection configured in 
	# docker-compose-hostnetworking.yml)
	echo "Sleeping 60 seconds before unit testing"
	sleep 60
	docker-compose exec dcc-dashboard-service py.test -p no:cacheprovider -s -x

run:
	# Run a chalice instance locally, in local mode, with bridge mode networking
	docker-compose up -d --build --force-recreate

testme: stop reset run
	# Run tests locally, against an already-existing ES instance populated with data, as
	# set in the ES_DOMAIN variable of docker-compose.yml.  (e.g. this could be at AWS)
	echo "Sleeping 30 seconds before populating ES"
	sleep 30
	$(MAKE) populate
	echo "Sleeping 60 seconds before unit testing"
	sleep 60
	docker-compose exec dcc-dashboard-service py.test -p no:cacheprovider -s -x
