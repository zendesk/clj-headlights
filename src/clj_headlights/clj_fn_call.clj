(ns clj-headlights.clj-fn-call
  "Helpers to have Dataflow call Clojure functions"
  (:require [schema.core :as s]
            [clojure.tools.logging :as log]
            [clojure.string :as string])
  (:import (clj_headlights CljSerializableFunction)
           (clojure.lang Var ExceptionInfo)
           (java.io ByteArrayOutputStream ObjectOutputStream Serializable)))

(defn serializable? [^Serializable obj]
  (try
    (-> (ObjectOutputStream. (ByteArrayOutputStream.)) (.writeObject obj))
    true
    (catch Exception _
      false)))

(def CljCallArgument
  (s/constrained s/Any serializable?))

(def CljCall (s/cond-pre Var [(s/one Var "processing-var") CljCallArgument]))

; TODO: document me
(defn to-serializable-clj-call [clj-call]
  (if (var? clj-call)
    (to-serializable-clj-call [clj-call])
    (let [[var & params] clj-call
          metadata (meta var)
          ns-name (ns-name (:ns metadata))
          name (:name metadata)
          full-name (symbol (str ns-name "/" name))]
      {:creation-stack (string/join "\n" (into [] (comp (map str) (drop-while #(re-find #"pardo" %))) (rest (.getStackTrace (Thread/currentThread)))))
       :full-name full-name
       :ns-name ns-name
       ;; the lazyseq created by destructuring can accidentally close over things, so convert to vec
       :params (vec params)})))

(defn clj-call-invoke
  [{:keys [full-name params ns-name creation-stack]} & args]
  (clj_headlights.System/ensureInitialized ns-name)
  (try
    (apply (var-get (find-var full-name)) (into (vec args) params))
    (catch ExceptionInfo e
      (log/error "An exception happened, here is some extra information " {:creation-stack creation-stack :params params :data (mapv pr-str args) :exception e})
      (throw e))))

(s/defn serializable-function :- CljSerializableFunction
  [clj-call :- CljCall]
  (CljSerializableFunction. (to-serializable-clj-call clj-call)))

(defn serializable-function-apply [clj-call input]
  (clj-call-invoke clj-call input))

; TODO: try to find a way to get rid of that function, maybe by better delimiting the usees of CljCall and serializable
; formats.
(s/defn append-argument-to-clj-call :- CljCall
  [clj-call :- CljCall
   arg :- CljCallArgument]
  (conj
    (if (vector? clj-call)
      clj-call
      (vector clj-call))
    arg))
