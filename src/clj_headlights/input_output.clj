(ns clj-headlights.input-output
  "Tools for pipeline data input and output"
  (:require [clojure.string :as str]
            [clj-headlights.pubsub :as pubsub]
            [clj-headlights.pipeline :as df]
            [cheshire.core :as json]
            [clojure.string :as string]
            [schema.core :as s]
            [clj-headlights.pcollections :as pcollections])
  (:import (org.apache.beam.sdk.io TextIO)
           (clj_headlights PartitionedFileSink)
           (org.apache.beam.sdk.io.gcp.pubsub PubsubIO)))

(defn absolute-path? [resource-string]
  (re-matches #"^(?:file:///|gs://).*" resource-string))

(defn file-url? [resource-string]
  (str/starts-with? resource-string "file://"))

(defn gcs-url? [resource-string]
  (str/starts-with? resource-string "gs://"))

(defn pubsub-input? [resource-string]
  (re-matches #"^projects\/[^\/]+\/(?:subscriptions|topics)\/[^\/]+$" resource-string))

(defn drop-file-url-protocol [resource-string]
  (str/replace-first resource-string  "file://", ""))

(defn resource-string->source
  "Construct a Dataflow source transform to read from a resource-string.
   Supported are:
   * Local files (file://)
   * GCS (gs://)
   * PubSub topics / subscriptions"
  [resource-string]
  (cond
    (nil? resource-string) nil
    (file-url? resource-string) (.from (TextIO/read) ^String (drop-file-url-protocol resource-string))
    (gcs-url? resource-string) (.from (TextIO/read) ^String resource-string)
    (pubsub-input? resource-string) (pubsub/read-stream resource-string)
    :else (throw (ex-info "resource-string not identified" {:resource-string resource-string}))))

(defn resource-string->pcollection
  [pipeline name resource-string]
  (.apply pipeline name (resource-string->source resource-string)))

(defn url->sink
  "Construct a Dataflow sink transform to write text to a url.
  Supported are:
  * Local files (file://)
  * GCS (gs://)
  * PubSub topics"
  [url]
  (cond
    (file-url? url) (.to (TextIO/write) ^String (drop-file-url-protocol url))
    (gcs-url? url) (.to (TextIO/write) ^String url)
    :else (.to (PubsubIO/writeStrings) ^String url)))

(defn write-to-sink
  "Construct a Dataflow transform to write text to a sink and apply it to a pcoll.
  Supported are:
  * Local files (file://)
  * GCS (gs://)
  * PubSub topics"
  [pcoll name sink-url]
  (.apply pcoll name (url->sink sink-url)))

(s/defn read-json-source
  "Like resource-string->source, but maps elements from json-strings to objects."
  [pcoll :- pcollections/PCollectionType
   composite-name :- s/Str
   resource-string :- s/Str]
  (df/composite
    composite-name
    [pcoll]
    (fn [pcoll]
      (-> pcoll
          (.apply (str composite-name "-source-input") (resource-string->source resource-string))
          (df/df-map (str composite-name "-deserialize-json") [#'json/parse-string true])))))

(defn write-json-to-sink
  "Like write-to-sink, but maps elements to json before."
  [pcoll name url]
  (df/composite name [pcoll]
    (fn [pcoll]
      (-> pcoll
           (df/df-map "serialize-to-json" #'json/encode)
           (write-to-sink "output" url)))))

(defn multi-source
  "Take collection of resource strings and return a composite transform
  which contains all those resources.
  If collection is empty, return an empty pcollection."
  [pipeline name resource-strings]
  (df/composite
    name
    [pipeline]
    (fn [pipeline]
      (if (not-empty resource-strings)
        (df/flatten-pcollections
          (mapv #(resource-string->pcollection pipeline (string/replace % #"\/+" ".") %) resource-strings)
          (str name "-combine-sources"))
        (.apply pipeline name (pcollections/or-empty-str-pcoll nil))))))

(s/defn write-groups-to-partitioned-files :- pcollections/PCollectionType
  [pipeline :- pcollections/PCollectionType
   name :- s/Str
   destination :- s/Str
   suffix :- s/Str]
  (when-not (absolute-path? destination)
    (throw (ex-info "destination must be an absolute path" {:path destination})))
  (if (file-url? destination)
    ; this ns expects files prefixed with file://, but PartitionedFileSink does not
    (.apply pipeline name (PartitionedFileSink. (drop-file-url-protocol destination) suffix))
    (.apply pipeline name (PartitionedFileSink. destination suffix))))
