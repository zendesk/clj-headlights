(ns clj-headlights.system
  "Companion to the clj-headlights/System class.

  About callbacks:

  Callbacks are fixed extension points in clj-headlights. In order to use them, you need to create the namespace
  clj-headlights.callbacks, and implement any of the following functions in it:

  * pre-namespace-require [namespace-symbol]
      executed right before a namespace is required for the first time."
  (:require [clojure.tools.logging :as log])
  (:import (java.io FileNotFoundException)))

(def callbacks-loaded (atom nil))

(defn load-callbacks! [])
  (try
    (require 'clj-headlights.callbacks)
    true
    (catch FileNotFoundException _
      (log/debug "No clj-headlights.callbacks namespace available")
      false))

(defn ensure-callbacks-loaded []
  (when (nil? @callbacks-loaded)
    (swap! callbacks-loaded
           (fn [already-loaded?]
             (when (nil? already-loaded?)
               (load-callbacks!))))))

(defn callback [name args]
  (ensure-callbacks-loaded)
  (when @callbacks-loaded
    (when-let [callback-var (find-var (symbol "clj-headlights.callbacks" name))]
      (apply (var-get callback-var) args))))
