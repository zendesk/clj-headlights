#!/bin/bash
set -e

if [ "$CI_PART" = "unit" ]; then
    lein test
elif [ "$CI_PART" = "eastwood" ]; then
    lein eastwood
else
    echo "Invalid CI_PART: $CI_PART"
    exit 1
fi
