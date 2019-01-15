(ns clj-headlights.pardo
  (:require [schema.core :as s]
            [clojure.tools.logging :as log]
            [clojure.string :as string]
            [clj-headlights.clj-fn-call :as clj-fn-call]
            [clj-headlights.pcollections :as pcollections])
  (:import (java.util List)
           (clojure.lang Reflector)
           (org.apache.beam.sdk.transforms.join CoGbkResult$CoGbkResultCoder CoGbkResult)
           (org.apache.beam.sdk.coders Coder IterableCoder KvCoder StringUtf8Coder)
           (org.apache.beam.sdk.transforms.windowing Window)
           (org.apache.beam.sdk.transforms Create DoFn$ProcessContext ParDo ParDo$SingleOutput)
           (org.apache.beam.sdk.values TupleTag KV TupleTagList PCollectionTuple PCollection)
           (clj_headlights CljDoFn NippyCoder EdnCoder)))

(defn ^TupleTag get-tag [tag] (TupleTag. (name tag)))

(s/defn kv-value-restructurer :- s/Any
  "If the output of the previous stage was a KV, then it may
  have been the result of a GroupBy or CoGroupBy, which means
  we need to transform the output of those operations into more idiomatic
  clojure data structures.
  For a GroupBy we wrap the list of results in a seq, so that it acts like a normal list.
  For a CoGroupBy we create a positional vector, where the results for each pcoll that
  was grouped are in the same position they were sent to co-group-by-key."
  ^:private
  [coder :- Coder]
  (cond
    ;; co-group-by-key just happened
    (instance? CoGbkResult$CoGbkResultCoder coder)
    (fn [^CoGbkResult val]
      (let [tuple-tags (-> val .getSchema .getTupleTagList .getAll)]
        (mapv (fn [tuple-tag] (seq (.getAll val tuple-tag))) tuple-tags)))

    ;; group-by-key just happened
    (instance? IterableCoder coder)
    seq

    ;; no aggregation yet happened
    :else
    identity))

(s/defn input-restructurer :- s/Any
  "Given the coder of the input, create a function which pulls the input value
  from the context and turns it into clojure data structures."
  ^:private
  [coder :- Coder]
  (cond
    (instance? KvCoder coder)
    (let [value-restructurer (kv-value-restructurer (.getValueCoder ^KvCoder coder))]
      (fn [^DoFn$ProcessContext c]
        (let [kv ^KV (.element c)]
          [(.getKey kv) (value-restructurer (.getValue kv))])))

    :else
    (fn [^DoFn$ProcessContext c] (.element c))))

(defn setup [input-coder, serializable-clj-call]
  ;; The DoFn is a wrapper around a clojure function, to be executed
  ;; on compute nodes. The function is being executed on a new jvm, so
  ;; we find out what namespace that function came from, and require
  ;; it, so all its dependencies are met.
  ;; Only do that once per bundle (though no-op reqs are fast anyway).
  (clj_headlights.System/ensureInitialized (:ns-name serializable-clj-call))
  (input-restructurer input-coder))

(defn process-element [^DoFn$ProcessContext c
                       ^Window window
                       serialized-clj-call
                       creation-stack
                       input-extractor
                       & states]
  (clj_headlights.System/ensureInitialized (:ns-name serialized-clj-call))
  (try
    (apply clj-fn-call/clj-call-invoke serialized-clj-call c (input-extractor c) window states)
    (catch IllegalStateException e
      (log/error
        "Illegal state exception in ParDo, probably because var-ns"
        (:ns-name serialized-clj-call)
        "stack during creation:" (apply str (interpose "\n" creation-stack)))
      (throw e))))

(s/defn make-tags-list :- TupleTagList
  [tags :- [s/Keyword]]
  (TupleTagList/of ^List (map get-tag tags)))

(defn get-side-output ; TODO: is about pcollections, should be moved to pcollections ns
  "Retrieve the pcollection associated with a given tag in the output of a df-map-with-side-outputs."
  [^PCollectionTuple pcoll
   tag]
  (.get pcoll (get-tag tag)))

(defn get-side-outputs ; TODO: is about pcollections, should be moved to pcollections ns
  "Retrieve the map of Tags to PCollections."
  [^PCollectionTuple pcoll]
  (.getAll pcoll))

(s/defn set-side-output-coder :- PCollection  ; TODO: is about pcollections, should be moved to pcollections  ns
  "Sets the coder for the pcollection associated with a given tag in the output of a df-map-with-side-outputs."
  [pcoll :- PCollectionTuple
   tag :- s/Keyword
   coder :- Coder]
  (-> (get-side-output pcoll tag)
      (.setCoder coder)))

(defn default-coder []
  (NippyCoder.))

(s/defn create-and-apply :- pcollections/PCollectionType
  "Create a ParDo operation from a DoFn class, and apply it to a PCollection.

  It accepts an `options` map parameter that describes how the ParDo is created. You can specify the following keys:
  :dofn-cls - which DoFn class is created for the ParDo. For now, it is required that the class inherits from
              `clj_headlights.AbstractCljDoFn`, because the constructor invocation is hardcoded. Create a CljDoFn by
              default.
  :outputs - a map associating the (tagged) outputs of the ParDo with their respective coders. This map should always
             contain at least a :main key, that is used for the main output coder.
  :side-inputs - a collection of side-inputs the ParDo needs. As Beam requires, the given side-inputs need to extend
                 PCollectionView. Note: this only attaches the views to the ParDo; it is up to you to carry around the
                 same view object in your code to access the view data."
  [pcoll :- pcollections/PCollectionType
   name :- s/Str
   clj-call :- clj-fn-call/CljCall
   options :- {s/Keyword s/Any}] ; TODO: make me more precise, and optional
  (let [default-options {:dofn-cls CljDoFn ; maybe rename to :dofn and accept either DoFn instance or class inheriting AbstractCljDoFn
                         :outputs {:main (default-coder)}
                         :side-inputs []}
        options (merge default-options options)
        input-coder (.getCoder pcoll)
        pardo (cond-> (ParDo/of (Reflector/invokeConstructor (:dofn-cls options) (to-array [name input-coder (clj-fn-call/to-serializable-clj-call clj-call)])))
                      (not-empty (:side-inputs options)) (.withSideInputs ^Iterable (:side-inputs options)))]
    (if (= 1 (count (:outputs options)))
      (.setCoder (.apply pcoll name pardo) (get-in options [:outputs :main]))
      (let [extra-tags (remove #{:main} (keys (:outputs options)))
            applied-pcoll (.apply pcoll name (.withOutputTags pardo (get-tag :main) (make-tags-list extra-tags)))]
        (doseq [[tag coder] (:outputs options)]
          (set-side-output-coder applied-pcoll tag coder))
        applied-pcoll))))

(defn emit-side-output
  "Emit a value to a side output."
  [^DoFn$ProcessContext context
   tag
   value]
  (.output context (get-tag tag) value))

(defn emit-main-output
  "Emit the main output value"
  [^DoFn$ProcessContext context
   value]
  (.output context value))

; TODO: having that function is a smell. find a way to get rid of it.
(defn invoke-with-optional-state [& args]
  (let [state (last args)]
    (if (nil? state)
      (apply clj-fn-call/clj-call-invoke (butlast args))
      (apply clj-fn-call/clj-call-invoke args))))
