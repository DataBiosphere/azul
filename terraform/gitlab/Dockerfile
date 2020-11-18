FROM docker:18.03.1-ce
RUN apk -U add make bash

# This is the image for the Gitlab runner for Azul builds. It needs to be
# built locally and pushed to the Gitlab-integrated Docker registry on the
# Gitlab instance hosting the Azul project before that project can be built on
# Gitlab. This should happen once after the EBS volume attached to the
# instance is initially provisioned. Additionally, the Gitlab runner on that
# instance needs to be configured to use it. The major version in the tag of
# the base image should match that of the docker-dind image used on the
# instance (look for the EC2 instance's cloud-config in the TF config
# template). To build and push the image, and configure the runner to use it,
# perform the following steps:

# Login to the Gitlab instance's web UI.

# Unless you haven't done so already, create a group called `UCSC` and a project
# called Azul underneath it. This will ensure that the `ucsc/azul` image
# repository is created underneath in Gitlab's image registry.

# Go to your user profile settings and generate a personal access token.
# The token needs to have `read_registry` and `write_registry` permissions.

# Login with `docker` (the domain name might be different in your case):
#
# docker login docker.gitlab.dev.singlecell.gi.ucsc.edu
#
# Specify your Gitlab user name and the access token as the password.

# From the directory containing this file, build the image:
#
# docker build -t docker.gitlab.dev.singlecell.gi.ucsc.edu/ucsc/azul/runner .

# Push the image:
#
# docker push docker.gitlab.dev.singlecell.gi.ucsc.edu/ucsc/azul/runner

# SSH into Gitlab instance and edit `/mnt/gitlab/runner/config/config.toml`.
# Make sure that the runner(s) reserved for Azul refer to this image.
