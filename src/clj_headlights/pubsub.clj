(ns clj-headlights.pubsub
  "Helpers to use Pub/Sub in DataFlow jobs"
  (:require [clojure.string :as str]
            [schema.core :as s])
  (:import (org.apache.beam.sdk.transforms PTransform)
           (org.apache.beam.sdk.io.gcp.pubsub PubsubIO PubsubIO$Read)))

(s/defn pubsub-url->type :- s/Str
  "Extract the type from a PubSub URL, e.g. topics in projects/my-project/topics/my-topic"
  [url :- s/Str]
  (nth (str/split url #"/" 4) 2))

(s/defn read-stream :- (s/maybe PTransform)
  "Create a PTransform that reads either a Pub/Sub topic or subscription, based on the given URL."
  [url :- (s/maybe s/Str)]
  (when url
    (case (pubsub-url->type url)
      "topics" (.fromTopic (PubsubIO/readStrings) ^String url)
      "subscriptions" (.fromSubscription (PubsubIO/readStrings) ^String url)
      (throw (ex-info "invalid pub/sub url" {:pubsub-url url})))))

(s/defn read-stream-with-ts :- (s/maybe PTransform)
  "Create a PTransform that reads a Pub/Sub topic or subscription, based on
  the given URL. Uses .timestampLabel to assign timestamps to each message.
  Defaults to looking up the 'ts' attribute if no other name is provided."
  ([url :- (s/maybe s/Str)]
   (read-stream-with-ts url "ts"))

  ([url :- (s/maybe s/Str)
    attr-name :- s/Str]
   (when-let [stream (read-stream url)]
     (.withTimestampAttribute ^PubsubIO$Read stream attr-name))))
