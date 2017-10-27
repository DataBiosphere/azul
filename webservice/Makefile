build:
	docker build -t quay.io/ucsc_cgl/dcc-dashboard-service .

run:
	# Apply migrations and then run using the built image in daemon mode
	docker-compose up -d --build --force-recreate

populate:
	# Populate the ElasticSearch index
	docker-compose exec dcc-dashboard-service /app/test/populator.sh

test:
	# Run pytest inside the running container from run
	docker-compose exec dcc-dashboard-service py.test -p no:cacheprovider -s -x

stop:
	docker-compose down --rmi 'all'

reset:
	docker-compose stop
	docker-compose rm -f