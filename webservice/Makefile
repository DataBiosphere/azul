build:
	docker build -t quay.io/ucsc_cgl/dcc-dashboard-service .

run:
	# Apply migrations and then run using the built image in daemon mode
	docker-compose up -d

test:
	# Run pytest inside the running container from run
	docker-compose exec dcc-dashboard-service py.test -p no:cacheprovider -s -x

stop:
	docker-compose down

reset:
	docker-compose stop
	docker-compose rm -f