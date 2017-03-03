(ns myproject.core
  (:require [clj-headlights.pipeline :as hl]
            [clj-headlights.input-output :as io]))

(defn print-line [line]
  (println "line:" line)
  line)

(defn run []
  (spit "file.txt" "hello world\nthis is dataflow")
  (let [pipeline (hl/create (doto (hl/make-pipeline-options)
                                  (.setJobName "my-job")
                                  (.setProject "my-project")))]
    (-> pipeline
        (io/resource-string->pcollection "read-file" "file://file.txt")
        (hl/df-map "print-line" #'print-line))
    (.run pipeline)))
