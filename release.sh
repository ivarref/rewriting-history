#!/bin/bash

set -ex

clojure -Spom
clojure -M:jar

COMMIT_COUNT="$(git rev-list --count HEAD)"
let "NEXT_PATCH=COMMIT_COUNT+1"

sed -i 's|^  <version>\(.*\..*\.\).*</version>$|  <version>\1'$NEXT_PATCH'</version>|' pom.xml

RAW_VERSION="$(grep "^  <version>" pom.xml | sed 's|^.*<version>\(.*\)</version>.*$|\1|')"

NEW_VERSION="v$RAW_VERSION"
echo "Releasing >$NEW_VERSION< ..."

sed -i 's|^    <tag>.*$|    <tag>'$NEW_VERSION'</tag>|' pom.xml

git add pom.xml
git commit -m "Release $NEW_VERSION"
git tag -a $NEW_VERSION -m "Release $NEW_VERSION"
git push --follow-tags

clojure -M:deploy

echo "Released $RAW_VERSION!"