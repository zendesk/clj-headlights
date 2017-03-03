(ns clj-headlights.t-pardo
  (:use clojure.test)
  (:require [clj-headlights.pardo :as pardo]
            [clj-headlights.test-helpers :as df-test]
            [schema.test :as schema-test])
  (:import (org.apache.beam.sdk.values TupleTagList TupleTag PCollectionTuple)))

(use-fixtures :once schema-test/validate-schemas)

(deftest get-tag
  (testing "creates a TupleTag"
    (is (instance? TupleTag (pardo/get-tag :foo))))
  (testing "creates the same TupleTag for a give keyword"
    (is (= (pardo/get-tag :hello) (pardo/get-tag :hello))))
  (testing "creates two different TupleTags for two distinct keywords"
    (is (not (= (pardo/get-tag :abc) (pardo/get-tag :def))))))

(deftest make-tags-list
  (testing "creates a TupleTagList from given sequence of keywords"
    (let [tags-list (pardo/make-tags-list [:foo :bar])]
      (is (instance? TupleTagList tags-list))
      (is (= (.get tags-list 0) (pardo/get-tag :foo)))
      (is (= (.get tags-list 1) (pardo/get-tag :bar))))))

(deftest get-side-output
  (testing "gets PCollection from PCollectionTuple with given tag"
    (let [pipeline (df-test/create-test-pipeline)
          int-pcoll (df-test/create-pcoll pipeline [1 2])
          str-pcoll (df-test/create-pcoll pipeline ["foo" "bar"])
          pcoll-tuple (-> (PCollectionTuple/of (pardo/get-tag :int) int-pcoll)
                          (.and (pardo/get-tag :str) str-pcoll))]
      (is (identical? str-pcoll (pardo/get-side-output pcoll-tuple :str))))))
