(ns clj-headlights.t-pipeline
  (:use clojure.test)
  (:require [clj-headlights.pipeline :as df]
            [clj-headlights.test-helpers :as df-test]
            [schema.test :as schema-test])
  (:import [clj_headlights PipelineOptions]
           [org.apache.beam.runners.dataflow DataflowRunner]
           [org.apache.beam.runners.dataflow.options DataflowPipelineWorkerPoolOptions$AutoscalingAlgorithmType]))

(use-fixtures :once schema-test/validate-schemas)

(deftest flatten-pcollections
  (testing "it does nothing when using just one pcoll"
    (let [pcoll (df-test/create-pcoll [42])]
      (df-test/pcoll-is [42] (df/flatten-pcollections [pcoll] "flatten"))))
  (testing "it merges two pcolls"
    (let [pipeline (df-test/create-test-pipeline)
          pcoll1 (df-test/create-pcoll pipeline [42])
          pcoll2 (df-test/create-pcoll pipeline [99])]
      (df-test/pcoll-is [42 99] (df/flatten-pcollections [pcoll1 pcoll2] "flatten")))))

(deftest last-apply
  (testing "call .apply on a pcoll to perform chaining"
    (let [pcoll (df-test/create-pcoll [42])]
      (df-test/pcoll-is [42] pcoll))))

(defn add-hello-world [x] [[x "hello"] [x "world"]])
(deftest df-mapcat-kv
  (testing "it outputs multiple k/vs"
    (let [pcoll (-> (df-test/create-pcoll [1 2])
                    (df/df-mapcat-kv "mapcat-kv" #'add-hello-world))]
      (df-test/pcoll-is [[1 "hello"] [1 "world"] [2 "hello"] [2 "world"]] pcoll))))

(defn output-map [x] [x {:a x :b {:c x}}])
(deftest df-map-kv
  (testing "it outputs one k/v per item"
    (let [pcoll (-> (df-test/create-pcoll [1 2])
                    (df/df-map-kv "map-kv" #'add-hello-world))]
      (df-test/pcoll-is [[[1 "hello"] [1 "world"]] [[2 "hello"] [2 "world"]]] pcoll)))
  (testing "outputs value correctly when value is map"
    (let [pcoll (-> (df-test/create-pcoll [1 2])
                    (df/df-map-kv "map-kv" #'output-map))]
      (df-test/pcoll-is [[1 {:a 1 :b {:c 1}}] [2 {:a 2 :b {:c 2}}]] pcoll))))

(defn greater-than-one? [x] (> x 1))
(deftest df-filter
  (testing "it selects elements matching the predicate"
    (let [pcoll (-> (df-test/create-pcoll [1 2])
                    (df/df-filter "filter" #'greater-than-one?))]
      (df-test/pcoll-is [2] pcoll))))

(defn times-ten [x] [x (* 10 x)])
(deftest df-mapcat
  (testing "it outputs multiple values"
    (let [pcoll (-> (df-test/create-pcoll [1 2])
                    (df/df-mapcat "mapcat" #'times-ten))]
      (df-test/pcoll-is [1 10 2 20] pcoll))))

(deftest df-map
  (testing "it outputs one value"
    (let [pcoll (-> (df-test/create-pcoll [1 2])
                    (df/df-map "map" #'times-ten))]
      (df-test/pcoll-is [[1 10] [2 20]] pcoll))))

(defn add-keyword [x kw] [1 [kw x]])

(defn realise-iterators [[id co-gbkresult]]
  [id (mapv #(seq %) co-gbkresult)])

(deftest co-group-by-key
  (testing "groups different pcolls by a common key and presents them in a sane way"
    (let [test-pcoll (df-test/create-pcoll [1 2 3])
          left-pcoll (df/df-map-kv test-pcoll "map-kv-cgbk1" [#'add-keyword :left])
          middle-pcoll (df/df-map-kv test-pcoll "map-kv-cgbk3" [#'add-keyword :middle])
          right-pcoll (df/df-map-kv test-pcoll "map-kv-cgbk2" [#'add-keyword :right])
          grouped (-> (df/co-group-by-key [left-pcoll middle-pcoll right-pcoll] "cogroupbykey")
                      (df/df-map "realise-iterators1" #'realise-iterators))]
      (df-test/pcoll-matches
        (fn [[[key [left-vals middle-vals right-vals]]]]
          (and (= 1 key)
               (= {[:left 1] 1 [:left 2] 1 [:left 3] 1} (frequencies left-vals))
               (= {[:middle 1] 1 [:middle 2] 1 [:middle 3] 1} (frequencies middle-vals))
               (= {[:right 1] 1 [:right 2] 1 [:right 3] 1} (frequencies right-vals))))
        grouped))))

(defn process [word]
  [(when (.startsWith word "MARKER")
     [:marked-words word])
   (if (<= (count word) 3)
     [:main word]
     [:words-lengths-above-cut (count word)])])

(deftest df-map-with-side-outputs
  (testing "emits side outputs"
    (let [output (-> (df-test/create-pcoll ["a" "b" "c" "hello" "MARKER world"])
                     (df/df-map-cat-with-side-outputs "step" #'process [:marked-words :words-lengths-above-cut])
                     (df/get-side-outputs))
          output-keys (-> output keys)
          [main marker counts] (df-test/pipeline-to-data (vals output))
                     ]
      (is (= (map (fn [tt] (keyword (.getId tt))) output-keys) '(:main :marked-words :words-lengths-above-cut)))
      (is (= (sort main) '("a" "b" "c")))
      (is (= marker  '("MARKER world")))
      (is (= (sort counts)  '(5 12))))))

