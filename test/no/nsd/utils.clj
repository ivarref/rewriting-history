(ns no.nsd.utils
  (:require [datomic.api :as d]
            [datomic-schema.core]
            [clojure.pprint :as pprint])
  (:import (java.time.format DateTimeFormatter)
           (java.time LocalDate ZoneId)
           (java.util Date UUID)))

(defn empty-conn []
  (let [uri "datomic:mem://hello-world"]
    (d/delete-database uri)
    (d/create-database uri)
    (let [conn (d/connect uri)]
      conn)))

(defn empty-conn-unique []
  (let [uri (str "datomic:mem://hello-world" (UUID/randomUUID))]
    (d/delete-database uri)
    (d/create-database uri)
    (let [conn (d/connect uri)]
      conn)))

(defn year->Date [yr]
  (Date/from
    (-> (LocalDate/parse (str yr "-01-01") (DateTimeFormatter/ofPattern "yyyy-mm-DD"))
        (.atStartOfDay (ZoneId/of "UTC"))
        (.toInstant))))

(defn tx-fn! [conn]
  (let [yr (atom 1970)]
    (fn [data]
      @(d/transact conn (conj data {:db/id "datomic.tx"
                                    :db/txInstant (year->Date (swap! yr inc))})))))

(defn pprint [x]
  (pprint/pprint x)
  x)