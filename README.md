# clj-headlights

Clj-headlights is a toolset for [Apache Beam](https://beam.apache.org/) to use Clojure code and construct pipelines.

It is not intended as a full replacement for the [Beam Java SDK](https://beam.apache.org/documentation/sdks/java/), nor a complete abstraction layer. As a clj-headlights user, you are expected to know [the Beam programming model](https://beam.apache.org/documentation/programming-guide/). Its intent is to make Clojure play nice enough with the official SDK and provide helpers for tedious operations.

## Usage

```clojure
[com.zendesk/clj-headlights "0.1.0"]
```

### Examples

```clojure
(defn split-words [line]
  (str/split line #" "))

(let [pipeline (hl/create (hl/options {:job-name "wordcount"}))]
  (-> pipeline
      (io/resource-string->pcollection "read-file" "file://file.txt")
      (hl/df-mapcat "split-words" #'split-words)))
```

You can find example projects in the [examples](./examples) directory, and in the [test/clj_headlights/examples](test/clj_headlights/examples) directory.

## Versioning

[Semver](http://semver.org/) is used as versioning scheme.

## Security vulnerabilities

If you would like to report a security vulnerability issue, please follow the [Zendesk vulnerability disclosure process](https://hackerone.com/zendesk).

## Copyright and license

Copyright 2017 Zendesk, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
