.PHONY: all
all: docker_push

include ../../../common.mk

image := docker.gitlab.$(AZUL_DOMAIN_NAME)/ucsc/azul/runner

.PHONY:
docker: check_docker
	docker build \
	       --platform linux/amd64 \
	       --build-arg azul_docker_registry=$(azul_docker_registry) \
	       --tag $(image) .

.PHONY:
docker_push: docker
	docker push $(image)

.PHONY:
docker_run: docker
	docker run --rm -it \
	       --volume $(project_root):/azul \
	       --workdir /azul \
	       --volume /var/run/docker.sock:/var/run/docker.sock \
	       $(image)
