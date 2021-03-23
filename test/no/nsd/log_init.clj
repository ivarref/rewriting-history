(ns no.nsd.log-init
  (:require [clojure.test :refer :all]
            [taoensso.timbre :as timbre]
            [clojure.term.colors :as colors]
            [clojure.tools.reader.edn :as edn]
            [clojure.string :as str]))

(def level-colors
  {:warn colors/red
   :error colors/red})

(defn local-console-format-fn
  "A simpler log format, suitable for readable logs during development. colorized stacktraces"
  [data]
  (let [{:keys [level ?err  msg_ ?ns-str ?file hostname_
                timestamp_ ?line context]} data
        ts     (force timestamp_)]
    (let [color-f (get level-colors level str)]
      (cond-> (str #_(str ?ns-str ":" ?line)
                   (color-f (str/upper-case (name level)))
                   " "
                   (color-f (force msg_)))
              ?err
              (str " "(timbre/stacktrace ?err))))))

(timbre/merge-config!
  {:min-level [[#{"datomic.*" "com.datomic.*" "org.apache.*"} :warn]
               [#{"*"} (edn/read-string
                         (System/getProperty "TAOENSSO_TIMBRE_MIN_LEVEL_EDN" ":info"))]]
   :output-fn local-console-format-fn
   :appenders {:println (timbre/println-appender {:stream :std-out})}})

