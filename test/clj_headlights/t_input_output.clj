(ns clj-headlights.t-input-output
  (:require [clojure.test :refer :all]
            [schema.test :as schema-test]
            [clj-headlights.test-helpers :as df-test]
            [clj-headlights.input-output :as io])
  (:import (java.io File)))

(use-fixtures :once schema-test/validate-schemas)


(defn tempfile-with-contents
  "Rerurns a tempfile with specified contents which will delete itself on exit"
  [filename content]
  (let [file (File/createTempFile filename ".tmp")]
    (.deleteOnExit file)
    (spit file content)
    file))

(deftest read-json-source
  (testing "deserializes data from a json file"
    (let [file (tempfile-with-contents "file" "{\"json\":1}")]
      (df-test/pcoll-is [{:json 1}] (io/read-json-source (df-test/create-test-pipeline) "test" (str "file://" (.getPath file)))))))

(deftest multi-source
    (testing "creates creates composite source from source-strings"
        (let [pipeline (df-test/create-test-pipeline)
              file1 (tempfile-with-contents "file1" "line1")
              file2 (tempfile-with-contents "file2" "line2")]
            (df-test/pcoll-is ["line1" "line2"]
                              (io/multi-source pipeline
                                                "test-source"
                                                [(str "file://" (.getPath file1))
                                                 (str "file://" (.getPath file2))])))))
