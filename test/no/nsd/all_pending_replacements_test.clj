(ns no.nsd.all-pending-replacements-test
  (:require [clojure.test :refer :all]
            [no.nsd.utils :as u]
            [no.nsd.rewriting-history :as rh]
            [datomic.api :as d]))

(deftest all-pending-replacements-test
  (let [conn (u/empty-conn)]
    @(d/transact conn rh/schema)
    @(d/transact conn #d/schema[[:m/id :one :string :id]
                                [:m/info :one :string]])

    @(d/transact conn [{:m/id "id1" :m/info "original-data"}])
    @(d/transact conn [{:m/id "id1" :m/info "bad-data"}])
    @(d/transact conn [{:m/id "id1" :m/info "good-data"}])

    @(d/transact conn [{:m/id "id2" :m/info "really-bad-data"}])

    (rh/schedule-replacement! conn [:m/id "id1"] "bad" "correct")
    (rh/schedule-replacement! conn [:m/id "id1"] "data" "dataa")
    (rh/schedule-replacement! conn [:m/id "id2"] "a" "really-good")

    (is (= [{:attr :m/id, :ref "id1", :match "bad", :replacement "correct"}
            {:attr :m/id, :ref "id1", :match "data", :replacement "dataa"}
            {:attr :m/id, :ref "id2", :match "a", :replacement "really-good"}]
           (rh/all-pending-replacements conn)))))

(deftest no-match-should-be-a-noop
  (let [conn (u/empty-conn)]
    @(d/transact conn rh/schema)
    @(d/transact conn #d/schema[[:m/id :one :string :id]
                                [:m/info :one :string]])

    @(d/transact conn [{:m/id "id" :m/info "no match"}])

    (rh/schedule-replacement! conn [:m/id "id"] "missing" "missing2")

    (is (= [] (rh/all-pending-replacements conn)))))