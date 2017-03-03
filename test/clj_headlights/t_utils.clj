(ns clj-headlights.t-utils
  (:require [clj-headlights.utils :as utils])
  (:use [clojure.test]))

(deftest normalize-key
  (testing "turns dashes to underscores"
    (is (= (utils/normalize-key "some-key_here") "some_key_here"))))

(deftest normalize-map
  (testing "makes keyworded map into strings"
    (is (= (utils/normalize-map {:dashed-key 1}) {"dashed_key" 1})))
  (testing "works with string keys"
    (is (= (utils/normalize-map {"string-key" 1 "string_key2" 2})
           {"string_key" 1 "string_key2" 2}))))
