(ns clj-headlights.pcollections
  "Helpers to manipulate PCollections in general"
  (:require [schema.core :as s])
  (:import (org.apache.beam.sdk.values POutput PBegin PDone PCollectionList PCollectionTuple PCollection)
           (org.apache.beam.sdk.testing TestPipeline)
           (org.apache.beam.sdk.transforms Create$Values Create)
           (org.apache.beam.sdk Pipeline)
           (org.apache.beam.sdk.coders StringUtf8Coder)))

(def PCollectionType
  (s/cond-pre POutput PCollection PCollectionTuple Pipeline PCollectionList Create$Values PDone TestPipeline PBegin))

(defn empty-str-pcoll []
  (.withCoder (Create/of []) (StringUtf8Coder/of)))

(defn or-empty-str-pcoll [pcoll]
  (or pcoll (empty-str-pcoll)))
