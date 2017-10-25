#!/usr/bin/env bash


if [ "$TRAVIS_PULL_REQUEST" != "false" ]; then
    exit 0
fi

if [ "$TRAVIS_BRANCH" != "master" ]; then
    exit 0
fi

rm -rf target/doc
mkdir -p target
git clone https://github.com/zendesk/clj-headlights target/doc
cd target/doc
git checkout gh-pages
rm -r *
cd ../..
lein codex
cd target/doc
git add .
git commit -am "New documentation for $TRAVIS_COMMIT"
git push -u origin gh-pages
cd ../..
