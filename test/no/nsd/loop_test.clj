(ns no.nsd.loop-test
  (:require [clojure.test :refer :all]
            [no.nsd.utils :as u]
            [datomic.api :as d]
            [datomic-schema.core]
            [no.nsd.rewriting-history :as rh]))

(deftest loops-supported
  (testing "Verify that a simple loop reference works"
    (let [conn (u/empty-conn)]
      @(d/transact conn #d/schema[[:m/id :one :string :id]
                                  [:m/ref :one :ref]])

      @(d/transact conn [{:db/id "self"
                          :m/id  "id"
                          :m/ref "self"}])

      @(d/transact conn #d/schema[[:tx/txInstant :one :instant]])

      (is (= (rh/pull-flat-history conn [:m/id "id"])
             [[1 :tx/txInstant #inst "1972" 1 true]
              [2 :m/id "id" 1 true]
              [2 :m/ref 2 1 true]]))))

  (testing "Verify that a nested loop reference works"
    (let [conn (u/empty-conn)]
      @(d/transact conn #d/schema[[:m/id :one :string :id]
                                  [:m/comp :one :ref :component]
                                  [:m/info :one :string]
                                  [:m/ref :one :ref]])

      @(d/transact conn [{:db/id  "self"
                          :m/id   "id"
                          :m/comp {:db/id  "comp"
                                   :m/info "asdf"
                                   :m/ref  "self"}}])

      @(d/transact conn #d/schema[[:tx/txInstant :one :instant]])

      (is (= (rh/pull-flat-history conn [:m/id "id"])
             [[1 :tx/txInstant #inst "1972-01-01T00:00:00.000-00:00" 1 true]
              [2 :m/comp 3 1 true]
              [2 :m/id "id" 1 true]
              [3 :m/info "asdf" 1 true]
              [3 :m/ref 2 1 true]])))))
