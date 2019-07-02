#!/usr/bin/env bash

DOCKER_TAG=latest

if [[ -n "$TRAVIS_TAG" || "$TRAVIS_BRANCH" == "master" && "$TRAVIS_EVENT_TYPE" == "push" ]]; then
  docker login -u "$DOCKER_USERNAME" -p "$DOCKER_PASSWORD";

  if [[ -n "${TRAVIS_TAG}" ]]; then
    DOCKER_TAG=${TRAVIS_TAG}
  fi

  mvn -B -Pdocker-build-and-push -Ddocker.tag="$TRAVIS_TAG" install;
fi
