#!/usr/bin/env bash


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
git clone https://github.com/zendesk/clj-headlights target/doc
cd target/doc
git checkout gh-pages
rm -r *
cd ../..
lein codex
cd target/doc
git add .
git commit -am "New documentation for $TRAVIS_COMMIT"

ENCRYPTED_KEY_VAR="encrypted_${ENCRYPTION_LABEL}_key"
ENCRYPTED_IV_VAR="encrypted_${ENCRYPTION_LABEL}_iv"
ENCRYPTED_KEY=${!ENCRYPTED_KEY_VAR}
ENCRYPTED_IV=${!ENCRYPTED_IV_VAR}
openssl aes-256-cbc -K $ENCRYPTED_KEY -iv $ENCRYPTED_IV -in ../travis-github-key.enc -out ../travis-github-key -d
chmod 600 ../travis-github-key
eval `ssh-agent -s`
ssh-add travis-github-key

git push -u origin gh-pages
cd ../..
