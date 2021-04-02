(ns no.nsd.stage-data-test
  (:require [clojure.test :refer :all]
            [no.nsd.utils :as u]
            [datomic.api :as d]
            [no.nsd.rewriting-history :as rh]
            [no.nsd.schema :as schema]
            [no.nsd.stage-data :as sd]
            [clojure.tools.logging :as log]
            [no.nsd.rewriting-history.replay-impl :as replay]
            [no.nsd.rewriting-history.add-rewrite-job :as add-job]
            [no.nsd.rewriting-history.impl :as impl]))

(deftest stage-data-test
  (let [conn (u/empty-conn-days-txtime)]
    @(d/transact conn rh/schema)
    @(d/transact conn schema/schema)
    (let [new-hist sd/data
          mref [:Meldeskjema/ref "be0bc167-8015-4104-af21-0727ffb3d95d"]]
      (add-job/add-job! conn mref #{nil} :testing new-hist)
      (replay/process-job-step! conn mref)
      (replay/process-until-state conn mref :done)
      (is (= new-hist
             (impl/pull-flat-history-simple conn mref))))))

