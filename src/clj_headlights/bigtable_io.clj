(ns clj-headlights.bigtable-io
  (:require [schema.core :as s]
            [clj-headlights.pipeline :as df]
            [clj-headlights.clj-fn-call :refer[serializable-function]]
            [clj-headlights.pcollections :as pcollections])
  (:import (com.google.cloud.bigtable.config BigtableOptions$Builder)
           (org.apache.beam.sdk.io.gcp.bigtable BigtableIO BigtableIO$Write)
           (org.apache.beam.sdk.values KV)
           (com.google.protobuf ByteString)
           (com.google.bigtable.v2 Mutation$SetCell Mutation Mutation$SetCell$Builder Mutation$Builder Row Family Column Cell)))

(s/def TableMutation
  {:column-family s/Str
   :column-qualifier s/Str
   :value s/Str
   (s/optional-key :timestamp) s/Int})

(s/def BigtableOptionsMap
  {:instance s/Str
   :user-agent s/Str
   :table s/Str})

(s/def BigtableReadRow
  [(s/one s/Str "key")
   (s/one {s/Keyword   ; Column Family
           {s/Keyword   ; Column Qualifier
            [{:value s/Str :timestamp-micro s/Int}]}} "value")])

(s/def BigtableWriteRow
  [(s/one s/Str "key")
   [TableMutation]])

(defn options-builder
  [project instance-id user-agent builder]
  (-> builder
      (.setProjectId project)
      (.setInstanceId instance-id)
      (.setUserAgent user-agent)
      (.setUseCachedDataPool true)))

(s/defn build-mutation :- Mutation
  [mutation :- TableMutation]
  (let [{:keys [column-family column-qualifier value ^long timestamp]} mutation
        ^Mutation$SetCell$Builder set-cell (-> ^Mutation$SetCell$Builder (Mutation$SetCell/newBuilder)
                                               (.setFamilyName column-family)
                                               (.setColumnQualifier (ByteString/copyFromUtf8 column-qualifier))
                                               (.setValue (ByteString/copyFromUtf8 ^"[B" value))
                                               (#(if timestamp (.setTimestampMicros ^Mutation$SetCell$Builder % (unchecked-multiply timestamp 1000000)) %)))]
    (-> ^Mutation$Builder (Mutation/newBuilder)
        (.setSetCell set-cell)
        .build)))

(s/defn row->bigtable-kv
  [row :- BigtableWriteRow]
  (let [[key mutations] row]
    (KV/of (ByteString/copyFromUtf8 key)
           (mapv build-mutation mutations))))

(defn bigtable-io-write
  [pcoll name project {:keys [instance user-agent table]}]
  (df/composite
    name
    [pcoll]
    (fn [pcoll]
      (-> pcoll
          (df/df-map "make-bigtable-kv" #'row->bigtable-kv)
          (.apply "write"
                  (-> ^BigtableIO$Write (BigtableIO/write)
                      (.withBigtableOptionsConfigurator (serializable-function (partial options-builder project instance user-agent)))
                      (.withTableId table)))))))

(defn get-cells-data
  [cell-list]
  (map (fn [^Cell cell]
         {:value (-> cell .getValue .toStringUtf8)
          :timestamp-micro (.getTimestampMicros cell)})
       cell-list))

(defn get-columns-data
  [column-list]
  (let [qualifier-names (mapv (fn [^Column c] (-> c .getQualifier .toStringUtf8 keyword)) column-list)
        values (->> column-list
                    (map #(.getCellsList ^Column %))
                    (map get-cells-data))]
    (zipmap qualifier-names values)))

(s/defn bigtable-row->clj-map :- BigtableReadRow
  [row :- Row]
  (let [key (-> row .getKey .toStringUtf8)
        familiy-names (->> row
                           .getFamiliesList
                           (map #(.getName ^Family %))
                           (map keyword))]
    [key
     (->> row
          .getFamiliesList
          (map #(.getColumnsList ^Family %))
          (map get-columns-data)
          (zipmap familiy-names))]))

(s/defn bigtable-io-read-table-raw
  [pipeline :- pcollections/PCollectionType
   name :- s/Str
   project :- s/Str
   {:keys [instance user-agent table]}]
  (-> pipeline
      (.apply (str "read-from-bigtable-" name)
              (-> (BigtableIO/read)
                  (.withBigtableOptionsConfigurator (serializable-function (partial options-builder project instance user-agent)))
                  (.withTableId table)))))

(s/defn bigtable-io-read-table
  [pipeline :- pcollections/PCollectionType
   name :- s/Str
   project :- s/Str
   bigtable-options :- BigtableOptionsMap]
  (df/composite
    name
    [pipeline]
    (fn [pipeline]
      (-> pipeline
          (bigtable-io-read-table-raw "read-from-bigtable" project bigtable-options)
          (df/df-map "extract-columns" #'bigtable-row->clj-map)))))
