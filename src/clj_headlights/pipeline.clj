(ns clj-headlights.pipeline
  (:require [schema.core :as s]
            [clj-headlights.pardo :as pardo]
            [clj-headlights.clj-fn-call :as clj-fn-call]
            [clj-headlights.pcollections :as pcollections]
            [clojure.tools.logging :as log]
            [clj-headlights.utils :as utils])
  (:import (java.io Serializable ByteArrayOutputStream ObjectOutputStream)
           (clojure.lang Var IFn)
           (org.apache.beam.sdk Pipeline)
           (org.apache.beam.sdk.values PCollection PCollectionList TupleTag PDone PCollectionTuple POutput PBegin KV)
           (org.apache.beam.sdk.transforms Create$Values Flatten PTransform DoFn$ProcessContext GroupByKey)
           (org.apache.beam.sdk.transforms.windowing Window GlobalWindows)
           (org.apache.beam.sdk.transforms.join KeyedPCollectionTuple CoGroupByKey)
           (org.apache.beam.sdk.testing TestPipeline)
           (org.apache.beam.sdk.options PipelineOptionsFactory)
           (org.apache.beam.sdk.coders KvCoder IterableCoder)
           (org.apache.beam.sdk.extensions.protobuf ByteStringCoder ProtoCoder)
           (com.google.bigtable.v2 Mutation)
           (clj_headlights PipelineOptions EdnCoder NippyCoder)))

(def get-side-output pardo/get-side-output)
(def get-side-outputs pardo/get-side-outputs)

(s/defn create :- Pipeline
  "Create a Pipeline object. This is the first building block of any Beam pipeline."
  [options :- PipelineOptions]
  (utils/retry-on-error 3 IllegalStateException (Pipeline/create options)))

(s/defn make-pipeline-options :- PipelineOptions
  "Create a PipelineOptions object."
  []
  (PipelineOptionsFactory/as PipelineOptions))

(defn side-output-function-wrapper [context val _window state clj-call]
  (let [outputs (filter some? (pardo/invoke-with-optional-state clj-call val state))]
    (doseq [[tuple-tag-key output] outputs]
      (if (= :main tuple-tag-key)
        (pardo/emit-main-output context output)
        (pardo/emit-side-output context tuple-tag-key output)))))

(s/defn df-map-cat-with-side-outputs :- PCollectionTuple
  "Acts like a map-cat but returns a `PCollectionTuple` partitioned by the tag of the
  outputs of `f`. `f` must return a sequence of tuples in the form `[:tag value]`.
  The main output should use the tag `:main`. Nil values are filtered out"
  [pcoll :- pcollections/PCollectionType
   name :- s/Str
   clj-call :- clj-fn-call/CljCall
   tags :- [s/Keyword]]
  (pardo/create-and-apply
    pcoll
    name
    [#'side-output-function-wrapper (clj-fn-call/to-serializable-clj-call clj-call)]
    {:outputs (into {} (map (fn [tag] [tag (pardo/default-coder)]) (conj tags :main)))}))

(defn process-pcoll [pcoll name wrapping-call clj-call]
  (pardo/create-and-apply
    pcoll
    name
    (clj-fn-call/append-argument-to-clj-call wrapping-call (clj-fn-call/to-serializable-clj-call clj-call))
    {}))

(defn apply-to-value-and-output
  [^DoFn$ProcessContext context value _window state clj-call]
  (let [result (pardo/invoke-with-optional-state clj-call value state)]
    (.output context result)))

(s/defn df-map :- PCollection
  "Returns a `PCollection` of the return values of function `clj-call` being applied to the
  input `pcoll` - used for strictly 1-to-1 transformations"
  [pcoll :- pcollections/PCollectionType
   name :- s/Str
   clj-call :- clj-fn-call/CljCall]
  (process-pcoll pcoll name #'apply-to-value-and-output clj-call))

(s/defn df-map-with-side-input :- PCollection
  [pcoll :- pcollections/PCollectionType
   name :- s/Str
   side-input-view
   clj-call :- clj-fn-call/CljCall]
  (pardo/create-and-apply
    pcoll
    name
    (clj-fn-call/append-argument-to-clj-call clj-call side-input-view)
    {:side-inputs [side-input-view]}))

(defn apply-to-value-and-seq-outputs
  [^DoFn$ProcessContext context value _window state clj-call]
  (doseq [v (pardo/invoke-with-optional-state clj-call value state)]
    (.output context v)))

(s/defn df-mapcat :- PCollection
  "Similar to `df-map` but the return-values of `f` are flattened in the output
  `PCollection` - used when the transformation is not strictly 1-to-1"
  [pcoll :- pcollections/PCollectionType
   name :- s/Str
   clj-call :- clj-fn-call/CljCall]
  (process-pcoll pcoll name #'apply-to-value-and-seq-outputs clj-call))

(defn filterer
  [^DoFn$ProcessContext context value _window state clj-call]
  (when (pardo/invoke-with-optional-state clj-call value state)
    (.output context value)))

(s/defn df-filter :- PCollection
  "Returns a `PCollection` containing the elements of `pcoll` where `(f element)` is truthy"
  [pcoll :- pcollections/PCollectionType
   name :- s/Str
   clj-call :- clj-fn-call/CljCall]
  (process-pcoll pcoll name #'filterer clj-call))

(defn apply-to-value-and-output-kv
  [^DoFn$ProcessContext context value _window state clj-call]
  (let [result (pardo/invoke-with-optional-state clj-call value state)]
    (.output context (KV/of (first result) (second result)))))

(defn process-pcoll-kv
  [pcoll name wrapping-call clj-call]
  (pardo/create-and-apply
    pcoll
    name
    (clj-fn-call/append-argument-to-clj-call wrapping-call (clj-fn-call/to-serializable-clj-call clj-call))
    {:outputs {:main (KvCoder/of (EdnCoder.) (NippyCoder.))}}))

(s/defn df-map-kv
  "Same as df-map but expects the output of `f` to be a sequence of length 2 which is converted
  to a Dataflow KV object. Mostly used before a `GroupBy`"
  [pcoll :- pcollections/PCollectionType
   name :- s/Str
   clj-call :- clj-fn-call/CljCall]
  (process-pcoll-kv pcoll name #'apply-to-value-and-output-kv clj-call))

(defn apply-to-value-and-seq-outputs-kv
  [^DoFn$ProcessContext context value _window state clj-call]
  (doseq [[k v] (pardo/invoke-with-optional-state clj-call value state)]
    (.output context (KV/of k v))))

(s/defn df-mapcat-kv
  "Same as `df-map-kv` except `f` returns a sequence of lenght-2-sequences to be flattened
  and converted into KV objects"
  [pcoll :- pcollections/PCollectionType
   name :- s/Str
   clj-call :- clj-fn-call/CljCall]
  (process-pcoll-kv pcoll name #'apply-to-value-and-seq-outputs-kv clj-call))

(s/defn df-apply-dofn [pcoll name clj-call]
  (pardo/create-and-apply pcoll name clj-call {}))

(defn filterer-kv
  [^DoFn$ProcessContext context value _window state clj-call]
  (when (pardo/invoke-with-optional-state clj-call value state)
    (.output context (KV/of (first value) (second value)))))

(s/defn df-filter-kv :- PCollection
  "returns a `PCollection` containing the elements of `pcoll` where `(f element)` is true. coerces to KV"
  [pcoll :- pcollections/PCollectionType
   name :- s/Str
   clj-call :- clj-fn-call/CljCall]
  (process-pcoll-kv pcoll name #'filterer-kv clj-call))

(defn apply-to-value-and-output-with-timestamp
  [^DoFn$ProcessContext context value _window state clj-call]
  (let [[result instant] (pardo/invoke-with-optional-state clj-call value state)]
    (.outputWithTimestamp context result instant)))

(s/defn df-map-with-timestamp
  "Assigns a timestamp to pcoll rows"
  [pcoll :- pcollections/PCollectionType
   name :- s/Str
   clj-call :- clj-fn-call/CljCall]
  (process-pcoll pcoll name #'apply-to-value-and-output-with-timestamp clj-call))

(defn ^PCollection output-is-kv [^PCollection pcoll] (.setCoder pcoll (KvCoder/of (EdnCoder/of) (NippyCoder/of))))

(s/defn flatten-pcollections :- PCollection
  "Merge a list of pcolls."
  [pcolls :- [PCollection]
   name :- s/Str]
  (-> (PCollectionList/of ^Iterable pcolls)
      (.apply name (Flatten/pCollections))))

(s/defn co-group-by-key :- PCollection
  "Takes a list of PCollections which are already in KV form, and joins them on their keys.
  Output pcollection will look like: [key [first-pcoll-vals second-pcoll-vals third-pcoll-vals &c]]"
  [pcolls :- (s/cond-pre PCollection [PCollection])
   name :- s/Str]
  (let [pcolls (if (vector? pcolls) pcolls [pcolls])] ; TODO: do not allow co-group-by-key of one pcollection
    (loop [keyed-pcoll-tuple (KeyedPCollectionTuple/of (TupleTag. "0") (first pcolls))
           remaining-pcolls (rest pcolls)
           counter 1]
      (if (empty? remaining-pcolls)
        (.apply keyed-pcoll-tuple name (CoGroupByKey/create))
        (recur (.and keyed-pcoll-tuple (TupleTag. (str counter)) (first remaining-pcolls)) (rest remaining-pcolls) (inc counter))))))

(s/defn group-by-key :- PCollection
  [pcoll :- PCollection
   name :- s/Str]
  (.apply pcoll name (GroupByKey/create)))

(s/defn composite
  "Nests transforms that happen in f into a composite transform"
  [name :- s/Str
   inputs :- [pcollections/PCollectionType]
   f :- IFn]
  (let [transform (proxy [PTransform] []
                    (expand [pcoll-like]
                      (let [transform-result (if (= (class pcoll-like) PCollectionList)
                                               (apply f (.getAll pcoll-like))
                                               (f pcoll-like))]
                        (if (coll? transform-result)
                          (PCollectionList/of ^Iterable transform-result)
                          transform-result))))
        application-result (.apply
                             (if (= 1 (count inputs))
                               (first inputs)
                               (PCollectionList/of ^Iterable inputs))
                             name
                             transform)]
    (if (= PCollectionList (class application-result))
      (into [] (.getAll ^PCollectionList application-result))
      application-result)))

(defn context->ms [^DoFn$ProcessContext c]
  (.getMillis (.timestamp c)))
