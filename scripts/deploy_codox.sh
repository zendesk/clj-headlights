#!/usr/bin/env bash

set -e
set -x

if [ "$TRAVIS_PULL_REQUEST" != "false" ]; then
    exit 0
fi

if [ "$TRAVIS_BRANCH" != "master" ]; then
    exit 0
fi

git config user.name "Travis CI"
git config user.email "travis_ci@zendesk.com"

rm -rf target/doc
mkdir -p target
git clone git@github.com:zendesk/clj-headlights.git target/doc
cd target/doc
git checkout gh-pages
rm -rf ./*
cd ../..
lein codox
cd target/doc
git add .
git commit -am "New documentation for $TRAVIS_COMMIT"

openssl aes-256-cbc -K $encrypted_7b8432f5ae93_key -iv $encrypted_7b8432f5ae93_iv -in ../../travis-github-key.enc -out ../../travis-github-key -d
chmod 600 ../../travis-github-key
eval `ssh-agent -s`
ssh-add ../../travis-github-key

git push origin gh-pages
cd ../..
