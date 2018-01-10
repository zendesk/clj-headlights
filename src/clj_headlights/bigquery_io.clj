(ns clj-headlights.bigquery-io
  "Common bigquery utilites for clojure dataflow jobs."
  (:require [schema.core :as s]
            [cheshire.core :as json]
            [clj-headlights.pipeline :as df]
            [clj-headlights.utils :as utils]
            [clojure.tools.logging :as log]
            [clj-headlights.pcollections :as pcollections])
  (:import (org.apache.beam.sdk.io.gcp.bigquery BigQueryIO$Write BigQueryIO$Write$CreateDisposition BigQueryIO$Write$WriteDisposition BigQueryIO$Read BigQueryIO)
           (com.google.api.services.bigquery.model TableSchema TableFieldSchema TableRow)
           (org.apache.beam.sdk.transforms SerializableFunction PTransform)))

(def WriteDispostions
  {:write-append BigQueryIO$Write$WriteDisposition/WRITE_APPEND
   :write-empty BigQueryIO$Write$WriteDisposition/WRITE_EMPTY
   :write-truncate BigQueryIO$Write$WriteDisposition/WRITE_TRUNCATE})

(def CreateDispositions
  {:create-if-needed BigQueryIO$Write$CreateDisposition/CREATE_IF_NEEDED
   :create-never BigQueryIO$Write$CreateDisposition/CREATE_NEVER})

(s/defn read-from-query :- PTransform
  [query :- s/Str]
  (log/info "reading from query " query)
  (-> (BigQueryIO/readTableRows) (.fromQuery query)))

(s/defn read-from-query-and-extract :- pcollections/PCollectionType
  [pipeline :- pcollections/PCollectionType
   name :- s/Str
   query :- s/Str
   field :- s/Str]
  (df/composite
    name
    [pipeline]
    (fn [pipeline]
      (-> pipeline
          (.apply (str name "-bq-input") (read-from-query query))
          (df/df-map (str name "-extract-field") [#'get field])))))

(defn to-tablerow [str]
  (-> (TableRow.) (.set "data" str)))

(s/defn write :- pcollections/PCollectionType
  "Write elements of a pcoll to bigquery in json format"
  [output :- (s/cond-pre s/Str SerializableFunction)
   pcoll :- pcollections/PCollectionType]
  (let [schema (-> (TableSchema.) (.setFields [(-> (TableFieldSchema.) (.setName "data") (.setType "STRING"))]))]
    (-> pcoll
        (df/df-map "jsonify" #'json/generate-string)
        (df/df-map "make-table-row" #'to-tablerow)
        (.apply "write-to-bigquery" (-> (BigQueryIO/writeTableRows)
                                        (.to output)
                                        (.withSchema schema)
                                        (.withWriteDisposition BigQueryIO$Write$WriteDisposition/WRITE_APPEND)
                                        (.withCreateDisposition BigQueryIO$Write$CreateDisposition/CREATE_IF_NEEDED))))))

(defn schema-field->bq-field [schema-field]
  (-> (TableFieldSchema.)
    (.setName (:name schema-field))
    (.setType (:type schema-field))
    (cond-> (:mode schema-field) (.setMode (:mode schema-field)))))

(defn table-row-maker [row-as-map schema]
  (s/validate {(s/cond-pre s/Keyword s/Str) s/Any} row-as-map)
  (let [row (utils/normalize-map row-as-map)
        table-row (TableRow.)]
    (doseq [field schema] (.set table-row (:name field) (row (:name field))))
    table-row))

(s/defn write-with-schema :- pcollections/PCollectionType
  "Write elements of a pcoll to bigquery in json format"
  [pcoll :- pcollections/PCollectionType
   name :- s/Str
   output :- (s/cond-pre s/Str SerializableFunction)
   schema :- [{:name s/Str :type s/Str (s/optional-key :mode) s/Str}]
   write-options :- {s/Keyword s/Any}]
  (let [bq-schema (-> (TableSchema.) (.setFields (mapv schema-field->bq-field schema)))
        {:keys [write-disposition create-disposition] :or
         {write-disposition :write-append create-disposition :create-if-needed}} write-options]
    (-> pcoll
        (df/df-map (str "make-table-row-" name) [#'table-row-maker schema])
        (.apply (str "write-to-bq-" name) (-> (BigQueryIO/writeTableRows)
                                              (.to output)
                                              (.withSchema bq-schema)
                                              (.withWriteDisposition (write-disposition WriteDispostions))
                                              (.withCreateDisposition (create-disposition CreateDispositions)))))))
