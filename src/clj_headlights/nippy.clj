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

(defn ^Integer fast-encode-stream
  "Encode the given data with nippy, write the length of the resulting byte
   array on the stream as an integer, then write the serialized data. Returns
   the number of bytes written."
  [^OutputStream stream data]
  (let [daos (DataOutputStream. stream)
        bytes ^"[B" (freeze data)
        bytes-count (alength bytes)]
    (.writeInt daos bytes-count)
    (.write daos bytes)
    (+ 4 bytes-count)))
