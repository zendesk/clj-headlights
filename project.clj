(defproject com.zendesk/clj-headlights "master-SNAPSHOT"
  :description "Clojure on Beam"
  :url "https://github.com/zendesk/clj-headlights"
  :license {:name "Apache License, Version 2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/tools.logging "0.4.0"]
                 [org.clojure/tools.reader "1.1.0"]
                 [clj-time "0.14.0"]
                 [cheshire "5.7.0" :exclusions [com.fasterxml.jackson.core/jackson-core]]
                 [prismatic/schema "1.1.7"]
                 [com.taoensso/nippy "2.13.0"]
                 [org.apache.beam/beam-sdks-java-core "2.2.0"]
                 [org.apache.beam/beam-runners-google-cloud-dataflow-java "2.2.0" :exclusions [io.grpc/grpc-core io.netty/netty-codec-http2]]
                 [com.google.cloud.bigtable/bigtable-hbase "1.0.0-pre3" :exclusions [io.grpc/grpc-core io.netty/netty-codec-http2 com.google.code.findbugs/jsr305]]
                 [com.google.cloud.bigtable/bigtable-protos "1.0.0-pre3" :exclusions [com.google.code.findbugs/jsr305]]
                 [com.google.cloud.bigtable/bigtable-hbase-1.x-shaded "1.0.0-pre3"]
                 [com.google.guava/guava "20.0"]
                 [io.grpc/grpc-core "1.2.0" :exclusions [com.google.code.findbugs/jsr305]]
                 [io.netty/netty-codec-http2 "4.1.8.Final"]]
  :java-source-paths ["src-java"]
  :profiles {:release {:dependencies [[org.clojure/clojure "1.8.0"]]}
             :dev [:release {:dependencies [[org.slf4j/slf4j-simple "1.7.21"]
                                            [org.slf4j/log4j-over-slf4j "1.7.21"]
                                            [org.apache.beam/beam-runners-direct-java "2.2.0"]]}]
             :test [:dev {:dependencies [[junit "4.12"]
                                         [org.hamcrest/hamcrest-all "1.3"]]}]
             :uberjar [:release {:aot :all}]}
  :codox {:source-uri "https://github.com/zendesk/clj-headlights/blob/master/{filepath}#L{line}"
          :metadata {:doc/format :markdown}}
  :plugins [[jonase/eastwood "0.2.5"]
            [lein-codox "0.10.3"]])
