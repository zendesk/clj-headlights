(ns clj-headlights.t-bigquery-io
  (:use clojure.test)
  (:require [clj-headlights.bigquery-io :as bigquery-io]
            [schema.test :as schema-test])
  (:import (com.google.api.services.bigquery.model TableRow)))

(use-fixtures :once schema-test/validate-schemas)

(deftest schema-field->bq-field
  (testing "maps schema to TableFieldSchema object"
    (is (= (bigquery-io/schema-field->bq-field {:name "id" :type "INTEGER"})
           {"name" "id", "type" "INTEGER"})))
  (testing "maps schema to TableFieldSchema object with mode"
    (is (= (bigquery-io/schema-field->bq-field {:name "id" :type "INTEGER" :mode "REQUIRED"})
           {"name" "id", "type" "INTEGER", "mode" "REQUIRED"}))))

(deftest table-row-maker
  (testing "Creates a TableRow object from a map"
    (let [table-row (doto (TableRow.) (.set "id" 1) (.set "company" "zendesk"))]
      (is (= (bigquery-io/table-row-maker {"id" 1 "company" "zendesk"} [{:name "id" :type "INTEGER"} {:name "company" :type "STRING"}]) table-row))
      (is (= (bigquery-io/table-row-maker {"id" 1 "company" "zendesk"} [{:name "id" :type "INTEGER"} {:name "company" :type "STRING"}]) {"id" 1 "company" "zendesk"})))))
