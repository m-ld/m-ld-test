# **m-ld** Compliance Tests
The tests (`*.spec.js`) in this directory are not meant to be run from this
project.

They assume the existence of a running 'orchestrator' of **m-ld** clones. An
orchestrator is a REST server that exposes the interface defined in
[clone.ts](../src/clone.ts). The orchestrator's responsibility is to manipulate
clones on behalf of the compliance tests. It can do so in any way it chooses:
* Child processes (node.js)
* Actors (Vert.x)
* Images (Docker)

So, the pattern is for the compliance test build step of a clone engine is to
start an orchestrator, and then run the tests in this package.
