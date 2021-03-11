(ns no.nsd.db-some-retract-fn-test
  (:require [clojure.test :refer :all]
            [no.nsd.datomic-generate-fn :as genfn]
            [no.nsd.rewriting-history.db-some-retract-fn :as s]
            [no.nsd.utils :as u]
            [no.nsd.shorter-stacktrace]
            [no.nsd.log-init]
            [datomic-schema.core]
            [datomic.api :as d]))

(defn db-fn
  ([] (db-fn false))
  ([generate]
   (genfn/generate-function
     'no.nsd.rewriting-history.db-some-retract-fn/some-retract
     :some/retract
     generate)))

(def empty-conn u/empty-conn)

(deftest some-retract-test
  (let [conn (empty-conn)]
    @(d/transact conn (into [(db-fn)]
                            #d/schema[[:m/id :one :string :id]
                                      [:m/now :one :instant]]))

    ; missing data is no-op:
    (is (nil? (s/some-retract conn [:m/id "id"] :m/now)))
    @(d/transact conn [[:some/retract [:m/id "id"] :m/now]])

    ; add sample data
    @(d/transact conn [{:m/id "id" :m/now #inst"1970"}])

    (is (= (s/some-retract conn [:m/id "id"] :m/now)
           [[:db/retract [:m/id "id"] :m/now #inst "1970-01-01T00:00:00.000-00:00"]]))

    @(d/transact conn [[:some/retract [:m/id "id"] :m/now]])

    (is (nil? (d/q '[:find ?now .
                     :in $
                     :where
                     [?e :m/id "id"]
                     [?e :m/now ?now]]
                    (d/db conn))))))


(deftest gen-fn
  (db-fn true)
  (is (= 1 1)))