# Contributing to clj-headlights

Thank you for your interest in clj-headlights!

## Reporting a bug or a suggestion

The [GitHub issues section](https://github.com/zendesk/clj-headlights/issues) is the right place to do so. File an issue there an the team will review and discuss the report with you. Try to be as precise and detailed as possible. For both bug reports and suggestions, providing code snippets come a long way. Always include the version you are using. For bugs, reproduction steps are mandatory.

## Getting set up

1. Make sure you have a working Clojure development environment. This include a JDK, Clojure, and [leiningen](https://github.com/technomancy/leiningen).
2. Fork and clone the repository.
3. Hack away.
4. Make sure `lein test` and `lein eastwood` pass.
5. Submit a [pull-request on GitHub](https://github.com/zendesk/clj-headlights/pulls).

## Submitting changes

When creating your pull-request, please include an explanation of why you would like the change to happen. For bugs it is usually trivial (nobody likes bugs in software), but for new features we need to understand why the addition would be helpful to you and everyone else.

###### Breaking changes

Our objective is to avoid breaking changes as much as possible. We need a very good reason to do so. As a result, if you want to change the behavior or signature of a function, consider writing a new function with a different name.

###### Code style

As a general rule, abide by [bbatsov's Clojure Style Guide](https://github.com/bbatsov/clojure-style-guide).

###### Review

In order to be merged, a pull-request requires two positive reviews from the team. Changes submitted by team members only require one approval.

###### Tests and documentation

Any new code change need to have associated tests. New functions need to have associated documentation.


## Release a new version

This assumes you are a maintainer with push access to the `com.zendesk` group on Clojars.

```bash
./scripts/release.sh X.Y.Z
```
