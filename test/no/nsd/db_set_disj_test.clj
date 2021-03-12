(ns no.nsd.db-set-disj-test
  (:require [clojure.test :refer :all]
            [no.nsd.datomic-generate-fn :as genfn]
            [no.nsd.utils :as u]
            [no.nsd.log-init]
            [datomic-schema.core]
            [datomic.api :as d]
            [clojure.tools.logging :as log]
            [clojure.string :as str]))

(defn db-fn
  ([] (db-fn false))
  ([generate]
   (genfn/generate-function
     'no.nsd.rewriting-history.dbfns.set-disj/set-disj
     :set/disj
     generate)))

(deftest write-fn
  (db-fn true)
  (is (= 1 1)))

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

(defmacro is-assert-msg [msg & body]
  `(is (let [emsg# (ex-msg ~@body)
             v# (true? (str/includes? emsg# ~msg))]
         (when-not v#
           (log/error "got error message" emsg#))
         v#)))

(deftest set-disj-tests
  (testing "fails on non-map input"
    (let [conn (u/empty-conn)]
      @(d/transact conn (into [(db-fn)] #d/schema[[:m/id :one :string :id]
                                                  [:m/ref :many :ref :component]]))
      (is-assert-msg "Assert failed: (map? value)"
                     @(d/transact conn [[:set/disj [:m/id "id"] :m/ref "asdf"]]))))

  (testing "fails on not-many attribute"
    (let [conn (u/empty-conn)]
      @(d/transact conn (into [(db-fn)] #d/schema[[:m/id :one :string :id]
                                                  [:m/ref :one :ref :component]]))
      (is-assert-msg "expected attribute to have cardinality :db.cardinality/many"
                     @(d/transact conn [[:set/disj [:m/id "id"] :m/ref {:x :x}]]))))

  (testing "fails on non-ref attribute"
    (let [conn (u/empty-conn)]
      @(d/transact conn (into [(db-fn)] #d/schema[[:m/id :one :string :id]
                                                  [:m/ref :many :string]]))
      (is-assert-msg "expected :m/ref to be of valueType :db.type/ref"
                     @(d/transact conn [[:set/disj [:m/id "id"] :m/ref {:x :x}]])))))



