.PHONY: all
all: docker_push

include ../../../common.mk

image := docker.gitlab.$(AZUL_DOMAIN_NAME)/ucsc/azul/runner

.PHONY: docker_image
docker_image: check_docker
	docker build \
	       --platform linux/amd64 \
	       --build-arg azul_docker_registry=$(azul_docker_registry) \
	       --build-arg azul_docker_image=$$(AZUL_DEBUG=0 python -m azul 'docker.resolve_docker_image_for_pull("docker")[0]') \
	       --tag $(image) .

.PHONY: docker_push
docker_push: docker_image
	docker push $(image)

.PHONY: docker_run
docker_run: docker_image
	docker run --rm -it \
	       --volume $(project_root):/azul \
	       --workdir /azul \
	       --volume /var/run/docker.sock:/var/run/docker.sock \
	       $(image)
