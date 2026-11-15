# spelaiondb-plugin-hive

HiveServer2's protocol for [SpelaionDB](https://github.com/spelaion-dev/spelaiondb), the desktop
database client: one driver for the engines that speak it, served to SpelaionDB as a plugin.

SpelaionDB's engines are programs it starts and talks to over their standard input and output
(its ADR-041). This one serves Apache Hive (`hive`), Spark's Thrift server (`spark`), Apache
Kyuubi (`kyuubi`) and Apache Impala (`impala`) through HiveServer2's Thrift service. It is
built here, in its own repository, and SpelaionDB pins by hash the exact release it was tested
with (its ADR-042, ADR-055).

- **Protocol:** [SpelaionDB's plugin protocol](https://github.com/spelaion-dev/spelaiondb/blob/dev/docs/plugin-protocol.md),
  version 1.1. Experimental until SpelaionDB 1.0.
- **Engines:** `hive`, `spark`, `kyuubi` and `impala`, over HiveServer2's binary transport
  (Apache Thrift, with rustls for TLS).
- **Logging in:** SASL's `PLAIN` (no authentication, LDAP or a custom provider), or no SASL for
  Impala without a password. Kerberos and the HTTP transport are not supported yet.
- **License:** Apache-2.0. `thrift/TCLIService.thrift` is Apache Hive's, under the same
  license; `src/tcliservice.rs` is written from it by Thrift's compiler.

## Working on it

```bash
ci/services.sh
```

starts the test servers (Hive, Spark's Thrift server and Kyuubi in Docker), and

```bash
SPELAION_TEST_HIVE=127.0.0.1:11000 SPELAION_TEST_SPARK=127.0.0.1:11001 SPELAION_TEST_KYUUBI=127.0.0.1:11009 cargo test
```

runs the tests and the protocol's conformance set against this plugin's own program, for each
engine. Without the variables the tests that need a server are skipped, which does not count as
tested. Kyuubi starts its Spark engine on the first session, which takes a minute.

Impala is not among CI's test servers (four services and a metastore). It is tested by hand
with its quickstart, trimmed, in `testing/impala/`:

```bash
docker compose -f testing/impala/compose.yaml up -d
SPELAION_TEST_IMPALA=127.0.0.1:21050 cargo test
```

To try a build with SpelaionDB, point a development build of it at the program
(`SPELAION_DEV_PLUGIN_HIVE`, `…_SPARK`, `…_KYUUBI`, `…_IMPALA`, in SpelaionDB's repository; a
release build ignores those variables).

`AGENTS.md` has the rules. Reports and questions go to
[SpelaionDB's issues](https://github.com/spelaion-dev/spelaiondb/issues).
