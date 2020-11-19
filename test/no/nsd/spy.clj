(ns no.nsd.spy
  (:require
    [sc.api.logging]
    [sc.api]
    [clojure.tools.logging :as log]))

;;;; defining custom loggers
(defn my-log-spy-1
  [cs-data]
  #_(log/info "At Code Site" (:sc.cs/id cs-data)
              "will save scope with locals" (:sc.cs/local-names cs-data)
              (str "(" (:sc.cs/file cs-data) "." (:sc.cs/line cs-data) ":" (:sc.cs/column cs-data) ")")))

(sc.api.logging/register-cs-logger
  ::log-spy-cs-with-timbre
  #(my-log-spy-1 %))

(defn my-log-spy-2
  [ep-data]
  (let [cs-data (:sc.ep/code-site ep-data)]
    #_(log/info "Wooho At Execution Point"
                [(:sc.ep/id ep-data) (:sc.cs/id cs-data)]
                (:sc.cs/expr cs-data) "=>" (:sc.ep/value ep-data))))

;;;; defining our own spy macro
(def my-spy-opts
  ;; mind the syntax-quote '`'
  `{:sc/spy-cs-logger-id        ::log-spy-cs-with-timbre
    :sc/spy-ep-pre-eval-logger  my-log-spy-2
    :sc/spy-ep-post-eval-logger my-log-spy-2})

(defmacro spy
  ([] (sc.api/spy-emit my-spy-opts nil &env &form))
  ([expr] (sc.api/spy-emit my-spy-opts expr &env &form))
  ([opts expr] (sc.api/spy-emit (merge my-spy-opts opts) expr &env &form)))

(defmacro defsc []
  `(->> (sc.api/defsc ~(sc.api/last-ep-id))
        (mapv (comp symbol name symbol))
        (println "variables:")))

(defmacro spy! []
  (if (= "true" (System/getProperty "DISABLE_SPY"))
    (do (let [fm (meta &form)]
          (println "WARNING: Spy called from test! File:" (str *file* ":" (:line fm)) ", col:" (:column fm)))
        nil)
    `(do ~(sc.api/spy-emit my-spy-opts nil &env &form)
         (eval '(defsc)))))