(ns no.nsd.utils
  (:require [datomic.api :as d]
            [datomic-schema.core]
            [clojure.pprint :as pprint]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import (java.time.format DateTimeFormatter)
           (java.time LocalDate ZoneId)
           (java.util Date UUID)))

(defn- empty-conn-for-uri [uri]
  (d/delete-database uri)
  (d/create-database uri)
  (let [conn (d/connect uri)]
    conn))

(defn empty-conn
  ([]
   (empty-conn-for-uri (str "datomic:mem://hello-world-" (UUID/randomUUID))))
  ([schema]
   (let [conn (empty-conn)]
     @(d/transact conn schema)
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

(defn empty-stage-conn [db-name]
  (assert (string? db-name))
  (assert (str/includes? db-name "-test"))
  (when (.exists (io/file ".stage-url.txt"))
    (let [jdbc-uri (str/trim (slurp (io/file ".stage-url.txt")))]
      (assert (str/starts-with? jdbc-uri "jdbc:"))
      (let [uri (str "datomic:sql://" db-name "?" jdbc-uri)]
        (d/delete-database uri)
        (d/create-database uri)
        (d/connect uri)))))

(defn clear []
  (.print System/out "\033[H\033[2J")
  (.flush System/out))