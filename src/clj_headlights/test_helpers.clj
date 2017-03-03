(ns clj-headlights.test-helpers
  "Tools to test dataflow jobs"
  (:use [clojure.test])
  (:require [clj-headlights.pipeline :as df]
            [schema.core :as s]
            [clojure.tools.reader.edn :as edn]
            [clojure.set])
  (:import (org.apache.beam.sdk.transforms Create)
           (org.apache.beam.sdk.testing TestPipeline)
           (org.apache.beam.sdk.values PCollection)
           (clojure.lang IFn)
           (org.apache.beam.sdk.io TextIO$Write TextIO)
           (java.io File FileNotFoundException)
           (org.apache.beam.sdk.transforms.windowing Window GlobalWindows DefaultTrigger)
           (clj_headlights NippyCoder)))

(defn create-test-pipeline []
  (doto (TestPipeline/create)
        (.enableAbandonedNodeEnforcement true)))

(def pcoll-counter (atom 0))

(defn create-pcoll-KVs
  ([elems]
   (create-pcoll-KVs (create-test-pipeline) elems))
  ([pipeline elems]
   (swap! pcoll-counter inc)
   (-> pipeline
       (.apply (str "test-pcoll-" @pcoll-counter) (.withCoder (Create/of elems) (NippyCoder/of)))
       (df/df-map-kv (str "map-kv-" @pcoll-counter) #'identity))))

(defn create-pcoll
  ([elems]
   (create-pcoll (create-test-pipeline) elems))
  ([pipeline elems]
   (let [pcoll-num (swap! pcoll-counter inc)]
     (.apply pipeline (str "test-pcoll-" pcoll-num) (if (empty? elems) (Create/empty (NippyCoder/of)) (.withCoder (Create/of elems) (NippyCoder/of)))))))

(defn assign-dummy-key [val]
  [nil val])

(defn try-read-file [file-name]
  (try
    (slurp file-name)
    (catch FileNotFoundException _
      (println "file did not exist at" file-name))))

(defn cat-files-like [tempfile]
  (apply str (map slurp (filter #(.contains % (.getName tempfile)) (map #(.getAbsolutePath %) (.listFiles (.getParentFile tempfile)))))))

(defn prn-cogbkresult [[id co-gbkresult]]
  (prn-str [id (mapv #(seq %) co-gbkresult)]))

(def sink-counter (atom 0))

(defn sink-pcoll [pcoll]
  (let [sink-number (swap! sink-counter inc)
        dummy-pcoll (create-pcoll-KVs (.getPipeline pcoll) [[nil nil]])
        tempfile (File/createTempFile (str "test-output" sink-number) ".tmp")
        row-with-nil-keys (-> pcoll
                              (.apply (str "rewindow" sink-number) (-> (Window/into (GlobalWindows.)) (.triggering (DefaultTrigger/of)) .discardingFiredPanes))
                              (df/df-map-kv (str "set-key-nil-test-" sink-number) #'assign-dummy-key))]
    (-> (df/co-group-by-key [dummy-pcoll row-with-nil-keys] (str "group-by-key-test-" sink-number))
        (df/df-map (str "to-string" sink-number) #'prn-cogbkresult)
        (.apply (str "out-to-tmp-" sink-number) (-> (TextIO/write) (.to (.getAbsolutePath tempfile)))))
    tempfile))

(defn sink-pcolls [pcolls]
  (mapv sink-pcoll pcolls))

(defn get-data-from-sinks [sinks]
  (mapv
    (fn [sink]
      (let [[_ [_ result]] (edn/read-string (cat-files-like sink))]
        result))
    sinks))

(defn pcoll-to-data [pcoll]
  (let [sink (sink-pcoll pcoll)]
    (.run (.getPipeline pcoll))
    (first (get-data-from-sinks [sink]))))

(defn pipeline-to-data [pcolls]
  (let [sinks (sink-pcolls pcolls)
        _ (.run (.getPipeline (first pcolls)))]
    (get-data-from-sinks sinks)))

(s/defn pcoll-results-match :- IFn
  "Checker that runs a pcoll's pipeline, gather its output, and passes it to `func`. If func throws or returns
  false, the test failed.

  Example: see `pcoll-contains`"
  [func :- IFn]
  (s/fn :- s/Bool
    [pcoll :- PCollection]
    (func (pcoll-to-data pcoll))))

(defmacro pcoll-is [expected-pcoll result-pcoll]
  `(let [expected# (frequencies ~expected-pcoll)
         real# (frequencies (pcoll-to-data ~result-pcoll))]
     (is (= expected# real#))))

(defn pcoll-matches
  [expected-f pcoll]
  (is ((pcoll-results-match expected-f) pcoll)))

(defmacro pcoll-contains
  [pcoll expected]
  `(let [actual# (set (pcoll-to-data ~pcoll))
         expected-set# (set ~expected)]
     (is (= expected-set# (clojure.set/intersection expected-set# actual#)))))
