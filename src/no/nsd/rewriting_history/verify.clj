(ns no.nsd.rewriting-history.verify
  (:require [no.nsd.rewriting-history.impl :as impl]
            [datomic.api :as d]
            [clojure.tools.logging :as log])
  (:import (java.util Date)))

(defn verify-history! [conn lookup-ref]
  (let [expected-history (impl/get-new-history conn lookup-ref)
        current-history (some->>
                          (impl/pull-flat-history-simple (d/db conn) lookup-ref)
                          (take (count expected-history))
                          (vec)
                          (impl/simplify-eavtos conn lookup-ref))
        ok-replay? (= expected-history current-history)
        db-id [:rh/lookup-ref (pr-str lookup-ref)]
        tx (if ok-replay?
             [[:db/cas db-id :rh/state :verify :done]
              {:db/id db-id :rh/done (Date.)}]
             [[:db/cas db-id :rh/state :verify :error]
              {:db/id db-id :rh/error (Date.)}])]
    (if ok-replay?
      (do
        @(d/transact conn tx))
      (do
        (log/error "replay of history for lookup ref" lookup-ref "got something wrong")
        (log/error "expected history:" expected-history)
        @(d/transact conn tx)))))