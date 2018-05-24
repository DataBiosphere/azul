build:
	docker build -t quay.io/ucsc_cgl/dcc-dashboard-service .

run:
	# Apply migrations and then run using the built image in daemon mode
	docker-compose up -d --build --force-recreate

populate:
	# Populate the ElasticSearch index
	docker-compose exec dcc-dashboard-service /app/test/populator.sh

unittests:
	# Run pytest inside the running container from run
	docker-compose exec dcc-dashboard-service py.test -p no:cacheprovider -s -x

stop:
	docker-compose down --rmi 'all'

# This is to set up the environment for some quick prototyping. This will setup ElasticSearch and Kibana bounded on the
# local host loaded with some testing files
play: stop reset run
#	echo "Finished Sleeping 60 Seconds"
#	$(MAKE) populate
#	echo "Kibana is now available on localhost:5601"

# Clears the whole system, reloads indexes, and runs the tests
testme: play
	echo "Sleeping 60 seconds before unit testing"
	sleep 60
	echo "Finished sleeping 60 seconds."
	docker-compose exec dcc-dashboard-service py.test -p no:cacheprovider -s -x

reset:
	docker-compose stop
	docker-compose rm -f
