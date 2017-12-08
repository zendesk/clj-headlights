#!/bin/bash

# Release a new version of clj-headlights.
#
# Usage: ./scripts/release.sh X.Y.Z

set -e

function print_usage {
    echo "Usage: ./scripts/release.sh X.Y.Z"
}

VERSION=$1

if [ "${VERSION}" = "" ]; then
    echo "Please provide the new version number." >&2
    print_usage >&2
    exit 1
fi

if [[ ! "${VERSION}" =~ ^[0-9]\.[0-9]\.[0-9]$ ]]; then
    echo "Incorrect version number. Must match ^[0-9]\.[0-9]\.[0-9]$" >&2
    exit 2
fi

CURRENT_VERSION=`git tag --sort=-version:refname|head -n1|tail -c +2`

echo -e "Current version: ${CURRENT_VERSION}"
echo -e "New vesion:      ${VERSION}"

read -r -p "Procceed? [y/N] " response
if [ "${response}" != "y" ]; then
    echo "Abort."
    exit 3
fi

set -x

git stash
git checkout master
git pull
git reset HEAD --hard
git ls-files|xargs sed -i '' "s/${CURRENT_VERSION}/${VERSION}/g" || true # sed returns non-zero exit code if some binary files are in the list (e.g. travis-github-key.enc)
lein test
lein with-profile release uberjar
git add -u
git commit -m "Version ${VERSION}"
git tag "v${VERSION}"
git push --tags
sed -E -i '' "s/(defproject .+) \"master-SNAPSHOT\"/\1 \"${VERSION}\"/" project.clj
lein with-profile release deploy clojars
git checkout -- project.clj
git checkout -
git stash pop

set +x

echo "Version ${VERSION} released!"
