(ns no.nsd.schema
  (:require [clojure.test :refer :all]
            [datomic.api :as d]
            [clojure.string :as str]
            [datomic.function]
            [datomic.db]
            [datomic.codec]
            [no.nsd.log-init]
            [clojure.edn :as edn]
            [no.nsd.utils :as u]))

(defn get-user-schema [db]
  (->> (d/q '[:find ?e
              :where
              [?e :db/ident ?ident]
              [(namespace ?ident) ?ns]
              (not (or [(contains? #{"db" "fressian"} ?ns)]
                       [(.startsWith ?ns "db.")]))]
            db)
       (map #(->> % first (d/entity db) d/touch (into (sorted-map))))
       (sort-by :db/ident)
       (vec)))

(comment
  (let [uri (str/trim (slurp ".pvo-stage-url.txt"))
        conn (d/connect uri)
        s (get-user-schema (d/db conn))])
  (spit "stage-schema.edn" (pr-str s)))

(def schema (edn/read-string {:readers {'db/id  datomic.db/id-literal
                                        'db/fn  datomic.function/construct
                                        'base64 datomic.codec/base-64-literal}}
                             (slurp "stage-schema.edn")))

(comment
  (let [conn (u/empty-conn)]
    @(d/transact conn schema)))