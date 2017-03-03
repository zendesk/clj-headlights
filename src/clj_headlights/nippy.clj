(ns clj-headlights.nippy
  (:require [taoensso.nippy])
  (:import [java.io OutputStream InputStream DataInputStream DataOutputStream]))

(defn ^"[B" freeze [obj]
  (taoensso.nippy/freeze obj {:compressor taoensso.nippy.compression/lz4-compressor}))

(defn thaw [^"[B" bytes]
  (taoensso.nippy/thaw bytes {:compressor taoensso.nippy.compression/lz4-compressor}))

(defn fast-decode-stream [^InputStream stream]
  (let [dais (DataInputStream. stream)
        num-bytes (.readInt dais)
        payload (byte-array num-bytes)]
    (.read stream payload)
    (thaw payload)))

(defn fast-encode-stream [^OutputStream stream data]
  (let [daos (DataOutputStream. stream)
        bytes ^"[B" (freeze data)]
    (.writeInt daos (alength bytes))
    (.write daos bytes)))
