(ns no.nsd.faketime2
  (:require [clojure.test :refer :all]
            [datomic.api :as d]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import (datomic Connection)
           (java.time LocalDate ZoneId)
           (java.time.format DateTimeFormatter)
           (java.util UUID Date)))

(defn days->Date [year minutes days]
  (Date/from
    (-> (LocalDate/parse (str "1970-01-01") (DateTimeFormatter/ofPattern "yyyy-mm-DD"))
        (.atStartOfDay (ZoneId/of "UTC"))
        (.plusMinutes minutes)
        (.plusDays days)
        (.plusYears year)
        (.toInstant))))

(defn conn-with-fake-tx-time [conn]
  (let [minutes (atom 0)
        days (atom 0)
        years (atom 0)
        uuid-counter (atom 0)]
    (with-meta
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
                                                  :db/txInstant (days->Date @years @minutes @days)})))
        (transactAsync [_ var1] (.transactAsync conn (conj var1
                                                           {:db/id        "datomic.tx"
                                                            :db/txInstant (days->Date @years @minutes @days)})))
        (txReportQueue [_] (.txReportQueue conn))
        (removeTxReportQueue [_] (.removeTxReportQueue conn))
        (gcStorage [_ var1] (.gcStorage conn var1))
        (release [_] (.release conn)))
      {:minutes minutes
       :days days
       :years years})))

(defn next-day! [conn]
  (reset! (:minutes (meta conn)) 0)
  (swap! (:days (meta conn)) inc))

(defn next-year! [conn]
  (reset! (:minutes (meta conn)) 0)
  (reset! (:days (meta conn)) 0)
  (swap! (:years (meta conn)) inc))

(defn empty-stage-conn
  ([db-name]
   (assert (string? db-name))
   (assert (str/includes? db-name "-test"))
   (when (.exists (io/file ".stage-url.txt"))
     (let [jdbc-uri (str/trim (slurp (io/file ".stage-url.txt")))]
       (assert (str/starts-with? jdbc-uri "jdbc:"))
       (let [uri (str "datomic:sql://" db-name "?" jdbc-uri)]
         (d/delete-database uri)
         (d/create-database uri)
         (conn-with-fake-tx-time (d/connect uri))))))
  ([]
   (empty-stage-conn "ivr-test")))