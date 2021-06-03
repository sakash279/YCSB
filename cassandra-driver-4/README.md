<!--
Copyright (c) 2015 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

# Apache Cassandra 2.x CQL binding using DataStax Java Driver 4 with Cosmos DB awareness features

Binding for [Apache Cassandra](http://cassandra.apache.org), using the CQL API via 
[DataStax Java Driver 4](https://docs.datastax.com/en/developer/java-driver/4.11/) with 
[Cosmos DB awareness features](https://github.com/Azure/azure-cosmos-cassandra-extensions/tree/develop/java-driver-4).

## Cassandra Configuration Parameters

- `cassandra.datastax-java-driver.config-file` (**required**)
  - Location of the application configuration file. See [DataStax Java Driver 4 Configuration][1] and `application.conf`
    in the `cassandra-driver-4-binding` sources for the settings that you must provide.
  - Default: `application.conf`.
  
- `cassandra.datastax-java-driver.request-tracing` (**&optional**)
  - Tracing is supposed to be run on a small percentage of requests only. This setting will enable or disable tracing
    on every query. We make it available here for debugging and troubleshooting. As a rule do not enable it on YCSB 
    runs as it will risk overwhelming your Cosmos Cassandra API instance. See [DataStax Java Driver 4 Configuration][2].
  - Default: `false`.

- [1]: https://docs.datastax.com/en/developer/java-driver/4.11/manual/core/configuration/
- [2]: https://docs.datastax.com/en/developer/java-driver/4.2/manual/core/tracing/