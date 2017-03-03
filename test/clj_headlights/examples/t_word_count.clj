(ns clj-headlights.examples.t-word-count
  "Example of a simple word-count job.
  Showcases mapping, key/values, and grouping."
  (:use [clojure.test])
  (:require [clj-headlights.pipeline :as hl]
            [clj-headlights.test-helpers :as hl-test]
            [schema.test :as schema-test]
            [clojure.pprint :as pprint]))

(use-fixtures :once schema-test/validate-schemas)

(defrecord WordCount [word count])

(defn split-words [line]
  (clojure.string/split line #"[^a-zA-Z']+"))

(defn add-value [word]
  [word 1])

(defn sum-values [[word values-gbkresult]]
  [word (apply + values-gbkresult)])

(defn to-record [[word count]]
  (WordCount. word count))

(defn print-record [word-count]
  (with-out-str (pprint/pprint word-count)))

(defn build-pipeline [pipeline]
  (-> pipeline
      (hl/df-mapcat "split-words" #'split-words)
      (hl/df-map-kv "add-value" #'add-value)
      (hl/group-by-key "group-by-word")
      (hl/df-map-kv "sum-values" #'sum-values)
      (hl/df-map "to-record" #'to-record)
      (hl/df-map "print-record" #'print-record)))

(def words ["hi there" "hi" "hi sue bob" "hi sue" "" "bob hi"])

(def counts ["{:word \"bob\", :count 2}\n"
             "{:word \"sue\", :count 2}\n"
             "{:word \"\", :count 1}\n"
             "{:word \"there\", :count 1}\n"
             "{:word \"hi\", :count 5}\n"])

(deftest word-count
  (testing "returns counts of each word in the input collection of strings"
    (let [words (hl-test/create-pcoll words)
          pcoll (build-pipeline words)]
      (hl-test/pcoll-is counts pcoll))))
