(ns clj-headlights.t-bigtable-io
  (:require [clojure.test :refer :all]
            [clj-headlights.bigtable-io :as bigtable-io]
            [clojure.string :as str])
  (:import (com.google.cloud.bigtable.config BigtableOptions$Builder)))

(deftest options-builder
  (testing "builds bigtable options"
    (is (= BigtableOptions$Builder
           (class (bigtable-io/options-builder "project" "instance" "agent" (BigtableOptions$Builder.)))))))

(deftest mutation-builder
  (testing "turns a clojure map into a mutation"
    (let [mutation-builder (bigtable-io/build-mutation {:column-family "family"
                                                        :column-qualifier "qualifier"
                                                        :value "value"})]
      (is (= "set_cell {\n  family_name: \"family\"\n  column_qualifier: \"qualifier\"\n  value: \"value\"\n}"
             (str/trim (.toString mutation-builder))))))
  (testing "sets timestamp when provided"
    (let [mutation-builder (bigtable-io/build-mutation {:column-family "family"
                                                        :column-qualifier "qualifier"
                                                        :value "value"
                                                        :timestamp 1})]
      (is (= "set_cell {\n  family_name: \"family\"\n  column_qualifier: \"qualifier\"\n  timestamp_micros: 1000000\n  value: \"value\"\n}"
             (str/trim (.toString mutation-builder)))))))
