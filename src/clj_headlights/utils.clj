(ns clj-headlights.utils
  (:require [clojure.string :as string]
            [clojure.tools.logging :as log]))

(defn normalize-key [key]
  (string/replace (name key) "-" "_"))

(defn normalize-map
  "normalizes all the keys of input-map."
  [input-map]
  (->> input-map
       (map (fn [[k v]] [(normalize-key k) v]))
       (into {})))

(defn retry-on-error*
  [n e function]
  (let [[exception result] (try
                             [nil (function)]
                             (catch Exception e
                               [e nil]))]
    (cond
      (nil? exception) result
      (zero? n) (throw exception)
      (not (instance? e exception)) (throw exception)
      :else (do
              (log/info "Retrying because exception raised:" exception)
              (Thread/sleep 30000)
              (recur (dec n) e function)))))

(defmacro retry-on-error
  ([n e & body]
   `(retry-on-error* ~n ~e (fn [] ~@body))))
