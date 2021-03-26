#!/bin/bash


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

rm -r "$DIR/temp" || true
mkdir -p $DIR/temp

sed $DIR/README.md -ne '/```clojure/,/```/p' | \
sed 's/^```$//g' | \
awk -v RS='```clojure' '{ print $0 > "'$DIR'/temp/temp" NR }'

VERSION=$(clojure -X:release ivarref.pom-patch/get-version)

cd temp
rm temp1
for entry in temp*
do
  perl -pe 's|\QREAL_JDBC_URL_HERE\E|'$(cat ../.stage-url.txt)'|g' $entry > $entry.clj
  echo "Running $entry.clj ..."
  clojure -Sdeps '{:deps {no.nsd/rewriting-history             {:mvn/version "'$VERSION'"}
                          com.datomic/datomic-pro              {:mvn/version "1.0.6202"}
                          org.mariadb.jdbc/mariadb-java-client {:mvn/version "2.4.3"}}
                   :mvn/repos {"my.datomic.com" {:url "https://my.datomic.com/repo"}}}'\
  -M \
  -e "$(cat $entry.clj)\n(d/release conn)\n(shutdown-agents)\n(System/exit 0)" || { echo "$entry.clj Failed!"; exit 1; }
  echo "Running $entry.clj ... OK!"
done