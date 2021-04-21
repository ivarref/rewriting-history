(ns no.nsd.patch-test
  (:require [clojure.test :refer :all]
            [no.nsd.utils :as u]
            [no.nsd.rewriting-history :as rh]
            [clojure.tools.logging :as log]
            [datomic.api :as d]
            [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.rewriting-history.replay-impl :as replay]
            [no.nsd.rewriting-history.init :as init]
            [taoensso.timbre :as timbre]
            [no.nsd.rewriting-history.rewrite :as rewrite]))

(deftest patch-test
  (let [conn (u/empty-conn)
        fil-uuid #uuid"f3a0530b-6645-475f-b5ab-4000849fc2b9"]
    @(d/transact conn #d/schema[[:m/id :one :string :id]
                                [:m/files :many :ref :component]
                                [:fil/id :one :uuid]
                                [:fil/name :one :string]])
    @(d/transact conn rh/schema)

    @(d/transact conn [{:m/id "id" :m/files
                              {:db/id "fil"
                               :fil/id fil-uuid
                               :fil/name "secret.txt"}}])

    (let [hist (rh/pull-flat-history conn [:m/id "id"])
          fil-eid (d/q '[:find ?e .
                         :in $ ?fil-uuid
                         :where
                         [?e :fil/id ?fil-uuid]]
                       hist fil-uuid)
          new-hist (-> hist
                       (rh/assoc-eid fil-eid :fil/id #uuid"00000000-0000-0000-0000-000000000000")
                       (rh/assoc-eid fil-eid :fil/name "deleted.txt"))]
      (is (= [[1 :tx/txInstant #inst "1973" 1 true]
              [2 :m/files 3 1 true]
              [2 :m/id "id" 1 true]
              [3 :fil/id fil-uuid 1 true]
              [3 :fil/name "secret.txt" 1 true]]
             hist))
      (is (= [[1 :tx/txInstant #inst "1973" 1 true]
              [2 :m/files 3 1 true]
              [2 :m/id "id" 1 true]
              [3 :fil/id #uuid"00000000-0000-0000-0000-000000000000" 1 true]
              [3 :fil/name "deleted.txt" 1 true]]
             new-hist))
      (rh/schedule-patch! conn [:m/id "id"] hist new-hist)
      (rh/rewrite-scheduled! conn)
      (is (= new-hist
             (rh/pull-flat-history conn [:m/id "id"]))))))
