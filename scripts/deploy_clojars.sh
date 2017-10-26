#!/usr/bin/env bash

set -e

if [ "$TRAVIS_PULL_REQUEST" != "false" ]; then
    exit 0
fi

if [ "$TRAVIS_BRANCH" != "master" ]; then
    exit 0
fi

mkdir -p ${HOME}/.lein
echo "{#\"https://clojars.org/repo\" {:username \"${clojars_username}\" :password \"${clojars_password}\"}}" > ${HOME}/.lein/credentials.clj

lein with-profile release deploy clojars
