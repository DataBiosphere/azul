#!/usr/bin/env bash

usage='Usage: ./release.sh STAGE [NUMBER_OF_RELEASES]'

case $1 in
  dev)
  stage=dev
  ;;
  integration)
  stage=integration
  ;;
  staging)
  stage=staging
  ;;
  prod)
  stage=prod
  ;;
  *)
  echo $usage
  echo 'Create the release notes for STAGE where:'
  echo '  - STAGE is one of dev, integration, staging, or prod'
  echo '  - NUMBER_OF_RELEASES is optionally the number of releases back'
  echo '    into the past that you want to include.'
  exit 1
esac

if [ $# -gt 1 ]
then
  releases_ago=$2
else
  releases_ago=0
fi
# Add one to adjust for tail
releases_ago=$(expr $releases_ago + 1)

git fetch --tags &> /dev/null
last_tag="$(git tag -l deployed/$stage/* | tail -$releases_ago | head -1)"
echo Last release tag: $last_tag
printf "\nrelease notes:\n"

git log $last_tag..HEAD --format="%C(auto) %h %s" --graph --merges \
  | grep -v '.* Merge branch .* into .*' \
  | sed 's/^[\*\/\\ |_]*//' \
  | grep .
