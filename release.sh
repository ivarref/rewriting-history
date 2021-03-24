#!/bin/bash

set -ex

clojure -Spom
clojure -M:test
clojure -M:jar

COMMIT_COUNT="$(git rev-list --count HEAD)"
let "NEXT_PATCH=COMMIT_COUNT+1"

clojure -X:release ivarref.pom-patch/clojars-repo-only!
VERSION=$(clojure -X:release ivarref.pom-patch/set-patch-version! :patch "$NEXT_PATCH")
clojure -X:release ivarref.pom-patch/update-tag!

git add pom.xml
git commit -m "Release $VERSION"
git tag -a v$VERSION -m "Release v$VERSION"
git push --follow-tags

clojure -M:deploy

echo "Released $VERSION"

rm *.pom.asc