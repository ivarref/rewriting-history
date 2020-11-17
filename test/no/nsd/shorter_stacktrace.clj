(ns no.nsd.shorter-stacktrace
  (:require [io.aviso.repl :as repl]))

(alter-var-root
  #'io.aviso.exception/*default-frame-rules*
  (constantly [[:package "clojure.lang" :omit]
               [:package #"sun\.reflect.*" :hide]
               [:package "java.lang.reflect" :omit]
               [:name #"clojure\.test.*" :hide]
               [:name #"clojure\.core.*" :hide]
               [:name #"clojure\.main.*" :hide]
               [:name #"io\.pedestal.*" :hide]
               [:name #"clj-http.*" :hide]
               [:file "REPL Input" :hide]
               [:name #"nrepl.*" :hide]
               [:package #"datomic\.*" :hide]
               [:name #"com\.walmartlabs.*" :hide]
               [:name "" :hide]
               [:name #"speclj\..*" :terminate]
               [:name #"clojure\.main/repl/read-eval-print.*" :terminate]]))

(defonce _init (repl/install-pretty-exceptions))
