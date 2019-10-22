#!/bin/bash

CURRENT_BRANCH=$(git branch | grep '\*' | cut -d' ' -f 2)

echo $CURRENT_BRANCH

generate_criteo_patch()
{
    CRITEO_VERSION=$(git describe --tags --match="v$BASE_VERSION-criteo*" 2>/dev/null)
    echo "last criteo patch was described as $CRITEO_VERSION"
    if [[ -z $CRITEO_VERSION ]]
    then
        CRITEO_INCREMENT=1
    elif [[ $CRITEO_VERSION =~ criteo[0-9]+-[0-9]+ ]]
    then
        CURRENT_INCREMENT=$(echo $CRITEO_VERSION | sed 's/^.*criteo\([0-9]\+\)-.*$/\1/')
        CRITEO_INCREMENT=$((CURRENT_INCREMENT + 1))
    else
        echo "commit already tagged"
        exit
    fi
}

function updateVersion() {
  TAG=$1
  VERSION=$(echo $TAG | sed 's/^v//')
  echo "version in ThisBuild := \"$VERSION\"" > version.sbt
}

if [[ $CURRENT_BRANCH =~ ^criteo\/ ]]
then
    BASE_VERSION=${CURRENT_BRANCH#*/}
    echo "criteo branch forked from base version: $BASE_VERSION"
    generate_criteo_patch
    CRITEO_PATCH="criteo${CRITEO_INCREMENT:-1}"
    echo "new criteo patch: $CRITEO_PATCH"
    CRITEO_TAG="v$BASE_VERSION-$CRITEO_PATCH"
    echo "tagging current commit with $CRITEO_TAG"
    git tag $CRITEO_TAG
    git remote add github https://${GH_TOKEN}@github.com/criteo-forks/marathon > /dev/null 2>&1
    git push --quiet github --tags
    updateVersion $CRITEO_TAG
else
    echo current branch is not a criteo branch
fi
