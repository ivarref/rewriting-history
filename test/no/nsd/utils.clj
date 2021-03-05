(ns no.nsd.utils
  (:require [datomic.api :as d]
            [datomic-schema.core]
            [clojure.pprint :as pprint]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import (java.time.format DateTimeFormatter)
           (java.time LocalDate ZoneId)
           (java.util Date UUID)
           (datomic Connection)))

(defn year->Date [yr]
  (Date/from
    (-> (LocalDate/parse (str yr "-01-01") (DateTimeFormatter/ofPattern "yyyy-mm-DD"))
        (.atStartOfDay (ZoneId/of "UTC"))
        (.toInstant))))

(defn conn-with-fake-tx-time [conn]
  (let [yr (atom 1970)]
    (reify Connection
      (requestIndex [_] (.requestIndex conn))
      (db [_] (.db conn))
      (log [_] (.log conn))
      (sync [_] (.sync conn))
      (sync [_ var1] (.sync conn var1))
      (syncIndex [_ var1] (.syncIndex conn var1))
      (syncSchema [_ var1] (.syncSchema conn var1))
      (syncExcise [_ var1] (.syncExcise conn var1))
      (transact [_ var1] (.transact conn (conj var1
                                               {:db/id        "datomic.tx"
                                                :db/txInstant (year->Date (swap! yr inc))})))
      (transactAsync [_ var1] (.transactAsync conn (conj var1
                                                         {:db/id        "datomic.tx"
                                                          :db/txInstant (year->Date (swap! yr inc))})))
      (txReportQueue [_] (.txReportQueue conn))
      (removeTxReportQueue [_] (.removeTxReportQueue conn))
      (gcStorage [_ var1] (.gcStorage conn var1))
      (release [_] (.release conn)))))

(defn empty-conn-for-uri [uri]
  (d/delete-database uri)
  (d/create-database uri)
  (conn-with-fake-tx-time (d/connect uri)))

(defn empty-conn
  ([]
   (empty-conn-for-uri (str "datomic:mem://hello-world-" (UUID/randomUUID))))
  ([schema]
   (let [conn (empty-conn)]
     @(d/transact conn schema)
     conn)))

(defn pprint [x]
  (binding [pprint/*print-right-margin* 120]
    (pprint/pprint x))
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
        (conn-with-fake-tx-time (d/connect uri))))))

(defn clear []
  (.print System/out "\033[H\033[2J")
  (.flush System/out))