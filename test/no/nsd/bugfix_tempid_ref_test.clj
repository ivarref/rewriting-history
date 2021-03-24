(ns no.nsd.bugfix-tempid-ref-test
  (:require [clojure.test :refer :all]
            [no.nsd.utils :as u]
            [no.nsd.log-init]
            [datomic-schema.core]
            [datomic.api :as d]
            [clojure.tools.logging :as log]
            [no.nsd.shorter-stacktrace]
            [no.nsd.rewriting-history :as rh]
            [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.rewriting-history.replay-impl :as replay]
            [no.nsd.rewriting-history.rewrite :as rewrite]
            [no.nsd.rewriting-history.tx :as tx]))

(deftest datomic-tx-backward-ref-test
  (let [conn (u/empty-conn)]
    @(d/transact conn impl/schema)
    @(d/transact conn #d/schema[[:m/id :one :string :id]
                                [:m/ref :one :ref]])

    @(d/transact conn [{:m/id "id" :m/ref "datomic.tx"}])
    @(d/transact conn [{:m/id "id" :m/ref "datomic.tx"}])

    (is (= (rh/pull-flat-history conn [:m/id "id"])
           [[1 :tx/txInstant #inst "1973" 1 true]
            [3 :m/id "id" 1 true]
            [3 :m/ref 1 1 true]
            [2 :tx/txInstant #inst "1974" 2 true]
            [3 :m/ref 1 2 false]
            [3 :m/ref 2 2 true]]))
    (rh/schedule-replacement! conn [:m/id "id"] "" "")
    (replay/process-job-step! conn [:m/id "id"])
    (replay/process-until-state conn [:m/id "id"] :rewrite-history)
    (is (= (tx/generate-tx conn
                           [:m/id "id"]
                           (impl/get-new-history conn [:m/id "id"]))
           [[[:db/cas [:rh/lookup-ref "[:m/id \"id\"]"] :rh/tx-index 0 1]
             [:db/add "datomic.tx" :tx/txInstant #inst "1973-01-01T00:00:00.000-00:00"]
             [:db/add "3" :m/id "id"]
             [:db/add "3" :m/ref "datomic.tx"]
             [:set/union
              [:rh/lookup-ref "[:m/id \"id\"]"]
              :rh/tempids
              #{#:rh{:tempid-str "1", :tempid-ref "datomic.tx"} #:rh{:tempid-str "3", :tempid-ref "3"}}]
             [:cas/contains [:rh/lookup-ref "[:m/id \"id\"]"] :rh/state #{:rewrite-history} :rewrite-history]]
            [[:db/add "datomic.tx" :tx/txInstant #inst "1974-01-01T00:00:00.000-00:00"]
             [:db/retract [:tempid "3"] :m/ref [:tempid "1"]]
             [:db/add [:tempid "3"] :m/ref "datomic.tx"]
             [:set/union [:rh/lookup-ref "[:m/id \"id\"]"] :rh/tempids #{#:rh{:tempid-str "2", :tempid-ref "datomic.tx"}}]
             [:db/cas [:rh/lookup-ref "[:m/id \"id\"]"] :rh/state :rewrite-history :verify]]]))

    (is (= {} (rewrite/get-tempids conn [:m/id "id"])))
    (rewrite/rewrite-history! conn [:m/id "id"])
    (is (= #{"1" "3"} (into #{} (keys (rewrite/get-tempids conn [:m/id "id"])))))
    (rewrite/rewrite-history! conn [:m/id "id"])

    (is (= (rh/pull-flat-history conn [:m/id "id"])
           [[1 :tx/txInstant #inst "1973" 1 true]
            [3 :m/id "id" 1 true]
            [3 :m/ref 1 1 true]
            [2 :tx/txInstant #inst "1974" 2 true]
            [3 :m/ref 1 2 false]
            [3 :m/ref 2 2 true]]))))