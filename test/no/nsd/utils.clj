(ns no.nsd.utils
  (:require [datomic.api :as d]
            [datomic-schema.core]
            [clojure.pprint :as pprint]))

(defn empty-conn []
  (let [uri "datomic:mem://hello-world"]
    (d/delete-database uri)
    (d/create-database uri)
    (let [conn (d/connect uri)]
      conn)))

(defn pprint [x]
  (pprint/pprint x)
  x)