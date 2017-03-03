(ns clj-headlights.t-json-coder
  (:use clojure.test)
  (:require [schema.test :as schema-test])
  (:import [clj_headlights JsonCoder]
           (java.util Map HashMap)
           (org.apache.beam.sdk.testing CoderProperties)))

(use-fixtures :once schema-test/validate-schemas)

(deftest json-coder-test
  (let [string-coder (JsonCoder. String)
        int-coder (JsonCoder. Integer)
        map-coder (JsonCoder. Map)]

    (is (nil? (CoderProperties/coderDecodeEncodeEqual string-coder "hello")))
    (is (nil? (CoderProperties/coderDecodeEncodeEqual int-coder (int 42))))
    (is (nil? (CoderProperties/coderDecodeEncodeEqual map-coder (HashMap. {"hello" (int 42), "world" (int 13)}))))))
