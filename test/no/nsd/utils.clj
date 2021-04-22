(ns no.nsd.utils
  (:require [datomic.api :as d]
            [datomic-schema.core]
            [clojure.pprint :as pprint]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.test :as test]
            [no.nsd.log-init :as log-init]
            [no.nsd.shorter-stacktrace]
            [no.nsd.rewriting-history :as rh])
  (:import (java.time.format DateTimeFormatter)
           (java.time LocalDate ZoneId)
           (java.util Date UUID)
           (datomic Connection)))

(defn pprint [x]
  (binding [pprint/*print-right-margin* 120]
    (log/info (str "\n" (str/trim (with-out-str (pprint/pprint x))))))
  x)

(defn year->Date [yr]
  (Date/from
    (-> (LocalDate/parse (str yr "-01-01") (DateTimeFormatter/ofPattern "yyyy-mm-DD"))
        (.atStartOfDay (ZoneId/of "UTC"))
        (.toInstant))))

(defn transform-excise [tx]
  (if (and (map? tx)
           (= [:db/excise] (vec (keys tx))))
    [:db/retractEntity (:db/excise tx)]
    tx))

(defn conn-with-fake-tx-time [conn transform-excise?]
  (let [yr (atom 1970)]
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
        (transact [_ var1]
          (let [finaltx (conj (mapv (if transform-excise? transform-excise identity) var1)
                              {:db/id        "datomic.tx"
                               :db/txInstant (year->Date (swap! yr inc))})]
            (.transact conn finaltx)))
        (transactAsync [_ var1]
          (.transactAsync conn (conj (mapv (if transform-excise? transform-excise identity) var1)
                                     {:db/id        "datomic.tx"
                                      :db/txInstant (year->Date (swap! yr inc))})))
        (txReportQueue [_] (.txReportQueue conn))
        (removeTxReportQueue [_] (.removeTxReportQueue conn))
        (gcStorage [_ var1] (.gcStorage conn var1))
        (release [_] (.release conn)))
      {:now-fn (fn [] (year->Date (inc @yr)))})))

(defn days->Date [days]
  (Date/from
    (-> (LocalDate/parse (str"1970-01-01") (DateTimeFormatter/ofPattern "yyyy-mm-DD"))
        (.atStartOfDay (ZoneId/of "UTC"))
        (.plusDays days)
        (.toInstant))))

(defn conn-with-fake-tx-time-2 [conn transform-excise?]
  (let [days (atom 0)]
    (reify Connection
      (requestIndex [_] (.requestIndex conn))
      (db [_] (.db conn))
      (log [_] (.log conn))
      (sync [_] (.sync conn))
      (sync [_ var1] (.sync conn var1))
      (syncIndex [_ var1] (.syncIndex conn var1))
      (syncSchema [_ var1] (.syncSchema conn var1))
      (syncExcise [_ var1] (.syncExcise conn var1))
      (transact [_ var1] (.transact conn (conj (mapv (if transform-excise? transform-excise identity) var1)
                                               {:db/id        "datomic.tx"
                                                :db/txInstant (days->Date (swap! days inc))})))
      (transactAsync [_ var1] (.transactAsync conn (conj (mapv (if transform-excise? transform-excise identity) var1)
                                                         {:db/id        "datomic.tx"
                                                          :db/txInstant (days->Date (swap! days inc))})))
      (txReportQueue [_] (.txReportQueue conn))
      (removeTxReportQueue [_] (.removeTxReportQueue conn))
      (gcStorage [_ var1] (.gcStorage conn var1))
      (release [_] (.release conn)))))

(defn empty-conn-days-txtime []
  (let [uri (str "datomic:mem://hello-world-" (UUID/randomUUID))]
    (d/delete-database uri)
    (d/create-database uri)
    (conn-with-fake-tx-time-2 (d/connect uri) true)))

(defn empty-conn
  ([]
   (let [uri (str "datomic:mem://hello-world-" (UUID/randomUUID))]
     (d/delete-database uri)
     (d/create-database uri)
     (conn-with-fake-tx-time (d/connect uri) true)))
  ([schema]
   (let [conn (empty-conn)]
     @(d/transact conn schema)
     conn)))

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
         (conn-with-fake-tx-time (d/connect uri) false)))))
  ([]
   (empty-stage-conn "ivr-test")))

(defn break []
  (log/info (str/join "*" (repeat 60 ""))))

(defn clear []
  (.print System/out "\033[H\033[2J")
  (.flush System/out)
  (break))

(defmacro highlight [& body]
  `(let [v# (atom nil)]
     (binding [log-init/*override-color* true]
       (break)
       (reset! v# (do ~@body))
       (break))
     @v#))

(defn pull-id [db id]
  (d/pull db '[:*] id))

(defmacro ex-msg [& body]
  `(try
     ~@body
     (log/error "no exception!")
     ""
     (catch Throwable t#
       (let [t# (loop [e# t#]
                  (if-let [cause# (ex-cause e#)]
                    (recur cause#)
                    e#))]
         (log/debug "error message was" (ex-message t#))
         (ex-message t#)))))

(defn ignore-txInstant [eavtos]
  (filterv #(not= :tx/txInstant (second %)) eavtos))

(defmacro is-assert-msg [msg & body]
  `(test/is (let [emsg# (ex-msg ~@body)
                  v# (true? (str/includes? emsg# ~msg))]
              (when-not v#
                (log/error "got error message" emsg#))
              v#)))

(defn rewrite-noop! [conn lookup-ref]
  (rh/schedule-replacement! conn lookup-ref "" "")
  (rh/rewrite-scheduled! conn))