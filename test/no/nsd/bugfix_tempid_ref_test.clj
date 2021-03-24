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
            [no.nsd.schema :as schema]
            [no.nsd.rewriting-history.replay-impl :as replay]
            [no.nsd.rewriting-history.add-rewrite-job :as add-job]
            [no.nsd.rewriting-history.rewrite :as rewrite]
            [no.nsd.rewriting-history.init :as init]
            [no.nsd.big-data :as data]))

#_(deftest test-conn
    (let [conn (c/conn)]
      (let [{:keys [tempids]} @(d/transact conn [{:db/id  "e"
                                                  :db/doc "doc"}])]
        @(d/transact conn [{:db/excise (get tempids "e")}]))
      #_@(d/transact conn impl/schema)
      #_@(d/transact conn #d/schema[[:m/id :one :string :id]
                                    [:m/ref :one :ref]])))

(deftest db-prepare-test
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
    #_(replay/process-until-state conn [:m/id "id"] :done)

    #_(is (= (impl/get-new-history conn [:m/id "id"])
             [[1 :tx/txInstant #inst "1973" 1 true]
              [3 :m/id "id" 1 true]
              [3 :m/ref 1 1 true]
              [2 :tx/txInstant #inst "1974" 2 true]
              [3 :m/ref 1 2 false]
              [3 :m/ref 2 2 true]]))

    #_@(d/transact conn [[:db/add "jalla" :db/id "datomic.tx"]])

    #_(is (= (u/pprint (impl/history->transactions
                         conn
                         (impl/get-new-history conn [:m/id "id"])))
             #_[[[:db/add "datomic.tx:1" :tx/txInstant #inst "1973"]
                 [:db/add "3" :m/id "id"]
                 [:db/add "3" :m/ref "datomic.tx"]]
                [[:db/add "datomic.tx:2" :tx/txInstant #inst "1974"]
                 [:db/retract [:tempid "3"] :m/ref [:tempid "datomic.tx:1"]]
                 [:db/add [:tempid "3"] :m/ref "datomic.tx"]]]))))




#_(deftest datomic-tx-ref-test
    (u/clear)
    (let [conn (u/empty-stage-conn)]
      #_@(d/transact conn [{:db/doc "asdf"}
                           [:db/excise 123]])
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
      (is (= :init (replay/process-job-step! conn [:m/id "id"])))
      (is (= :rewrite-history (replay/process-job-step! conn [:m/id "id"])))
      (u/break)

      #_(is (= (:new-hist-tx (rewrite/rewrite-history-get-tx conn [:m/id "id"]))
               [[:db/add "datomic.tx" :tx/txInstant #inst "1973"]
                [:db/add "3" :m/id "id"]
                [:db/add "3" :m/ref "datomic.tx"]]))

      #_(let [tempids (:tempids (rewrite/rewrite-history! conn [:m/id "id"]))]
          (is (= (u/pprint (:new-hist-tx (rewrite/rewrite-history-get-tx conn [:m/id "id"])))
                 [[:db/add "datomic.tx" :tx/txInstant #inst "1974"]
                  [:db/retract (get tempids "3") :m/ref (get tempids "datomic.tx")]
                  [:db/add (get tempids "3") :m/ref "datomic.tx"]])))

      #_(rewrite/rewrite-history! conn [:m/id "id"])

      #_(u/pprint (:tx (rewrite/rewrite-history-get-tx conn [:m/id "id"])))

      #_(add-job/add-job! conn1)
      #_(replay/process-until-state conn1 [:m/id "id"] :rewrite-history)

      #_(impl/apply-txes!
          (u/empty-conn)
          (impl/history->transactions
            conn1
            (rh/pull-flat-history conn1 [:m/id "id"])))))