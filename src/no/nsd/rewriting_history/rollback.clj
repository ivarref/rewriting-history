(ns no.nsd.rewriting-history.rollback
  (:require [datomic.api :as d]
            [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.rewriting-history.add-rewrite-job :as add-job]
            [clojure.tools.logging :as log])
  (:import (datomic Connection)))

(defn rollback! [conn lookup-ref t]
  (assert (instance? Connection conn))
  (assert (some? (impl/resolve-lookup-ref conn [:rh/lookup-ref (pr-str lookup-ref)])))
  (assert (pos-int? t))
  (log/info "checking t ...")
  (assert (= t (d/q '[:find ?t .
                      :in $ ?lookup-ref ?t
                      :where
                      [?e :rh/lookup-ref ?lookup-ref]
                      [?e :rh/state :init ?t true]]
                    (impl/history conn)
                    (pr-str lookup-ref)
                    t))
          "Expected to find t in history with :rh/state set to :init")
  (log/info "checking t ... OK")
  (let [new-history (impl/pull-flat-history-simple (impl/as-of conn t) lookup-ref)]
    (log/info "adding rollback job...")
    (add-job/add-job! conn lookup-ref :ok-rollback :pending-rollback new-history)
    (log/info "adding rollback job... OK!")))







