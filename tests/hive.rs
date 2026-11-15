//! Tests against real HiveServer2 servers: Apache Hive, Spark's Thrift server and Kyuubi (with
//! its Spark engine), all from `testing/compose.yaml`.
//!
//! They need `SPELAION_TEST_HIVE=host:port`, `SPELAION_TEST_SPARK=host:port` and
//! `SPELAION_TEST_KYUUBI=host:port` (no authentication: any user, any password). Without a
//! variable its engine is skipped with a notice, which does not count as tested.
//! `SPELAION_TEST_IMPALA=host:port` adds Impala (its quickstart, with no authentication: no
//! SASL), which is not among the test servers of `testing/compose.yaml` (see the README).

use std::time::{Duration, Instant};

use futures_util::TryStreamExt;
use spelaion_driver_api::{
    ConnectParams, Connection, Driver, DriverError, ExecEvent, ExecOptions, LogicalType,
    ObjectKind, ResultSchema, Secret, TlsMode, Value,
};
use spelaion_driver_hive::HiveDriver;

struct Server {
    name: &'static str,
    driver: HiveDriver,
    host: String,
    port: u16,
}

fn servers() -> Vec<Server> {
    let mut out = Vec::new();
    for (variable, name, driver) in [
        ("SPELAION_TEST_HIVE", "hive", HiveDriver::hive()),
        ("SPELAION_TEST_SPARK", "spark", HiveDriver::spark()),
        ("SPELAION_TEST_KYUUBI", "kyuubi", HiveDriver::kyuubi()),
        ("SPELAION_TEST_IMPALA", "impala", HiveDriver::impala()),
    ] {
        match std::env::var(variable) {
            Ok(addr) => {
                let (host, port) = addr.rsplit_once(':').expect("host:port");
                out.push(Server {
                    name,
                    driver,
                    host: host.into(),
                    port: port.parse().expect("a port"),
                });
            }
            Err(_) => eprintln!("{variable} not set: {name} skipped"),
        }
    }
    out
}

impl Server {
    fn params(&self) -> ConnectParams {
        ConnectParams {
            host: self.host.clone(),
            port: self.port,
            user: "spelaion".into(),
            // Impala with no authentication speaks no SASL: no password says so.
            password: (self.name != "impala").then(|| Secret::new("not-checked-here")),
            tls: TlsMode::Disable,
            // Kyuubi starts its engine on the first session.
            connect_timeout: Some(Duration::from_secs(120)),
            ..Default::default()
        }
    }

    async fn connect(&self) -> Box<dyn Connection> {
        self.driver
            .connect(&self.params())
            .await
            .unwrap_or_else(|e| panic!("{}: {e}", self.name))
    }

    /// Spark SQL behind both Spark's Thrift server and Kyuubi.
    fn is_spark(&self) -> bool {
        matches!(self.name, "spark" | "kyuubi")
    }

    fn is_impala(&self) -> bool {
        self.name == "impala"
    }

    /// Two statements that run for minutes: one that runs on the server before any row comes,
    /// and one the server runs while it is fetched (Hive and Spark run a query without `FROM`
    /// inside the fetch; Impala streams the rows of a join). Impala's `sleep()` does not stop on a
    /// cancel, so its statements are a join of ten billion rows.
    fn slow_statements(&self) -> [String; 2] {
        let sleep = match self.name {
            "spark" | "kyuubi" => "java_method('java.lang.Thread', 'sleep', 600000L)",
            "impala" => {
                let join = format!(
                    "{DIGITS} select a.n from d a cross join d b cross join d c cross join d e \
                     cross join d f cross join d g cross join d h cross join d i cross join d j \
                     cross join d k"
                );
                return [format!("select count(*) from ({join}) t"), join];
            }
            _ => "reflect('java.lang.Thread', 'sleep', 600000L)",
        };
        [
            format!("select count(*) from (select {sleep} as s) t group by s"),
            format!("select {sleep}"),
        ]
    }
}

/// Ten rows, `0` to `9`, as `d`.
const DIGITS: &str = concat!(
    "with d as (select 0 as n union all select 1 union all select 2 union all select 3 ",
    "union all select 4 union all select 5 union all select 6 union all select 7 ",
    "union all select 8 union all select 9)"
);

async fn run(conn: &dyn Connection, sql: &str) -> Result<Vec<ExecEvent>, DriverError> {
    conn.execute(sql, &ExecOptions::default())
        .await?
        .try_collect()
        .await
}

async fn query(conn: &dyn Connection, sql: &str) -> (ResultSchema, Vec<Vec<Value>>) {
    let events = run(conn, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}\n→ {e}"));
    let mut schema: Option<ResultSchema> = None;
    let mut rows = Vec::new();
    for event in events {
        match event {
            ExecEvent::Columns(s) => schema = Some((*s).clone()),
            ExecEvent::Batch(batch) => {
                let columns = &schema.as_ref().expect("columns come first").columns;
                for r in 0..batch.num_rows() {
                    rows.push(
                        (0..batch.num_columns())
                            .map(|c| {
                                Value::from_array(batch.column(c), r, columns[c].logical)
                                    .expect("a cell")
                            })
                            .collect(),
                    );
                }
            }
            ExecEvent::Notice(_) | ExecEvent::Done { .. } => {}
        }
    }
    (schema.unwrap_or_else(|| panic!("{sql}: no result")), rows)
}

/// Every type each engine has, as its logical type, its native type (as `DESCRIBE` writes it)
/// and its value as SpelaionDB shows it.
#[tokio::test]
async fn values_arrive_as_their_logical_types() {
    for server in servers() {
        let conn = server.connect().await;
        let conn = conn.as_ref();
        let name = server.name;
        let spark = server.is_spark();
        let mut cases: Vec<(&str, LogicalType, &str, &str)> = vec![
            ("true", LogicalType::Bool, "boolean", "true"),
            ("cast(-7 as tinyint)", LogicalType::Int, "tinyint", "-7"),
            (
                "cast(-300 as smallint)",
                LogicalType::Int,
                "smallint",
                "-300",
            ),
            ("cast(70000 as int)", LogicalType::Int, "int", "70000"),
            (
                "cast(9007199254740993 as bigint)",
                LogicalType::Int,
                "bigint",
                "9007199254740993",
            ),
            ("cast(1.5 as float)", LogicalType::Float, "float", "1.5"),
            ("cast(2.25 as double)", LogicalType::Float, "double", "2.25"),
            (
                "cast(-12.34 as decimal(10,2))",
                LogicalType::Decimal,
                "decimal(10,2)",
                "-12.34",
            ),
            ("'tab\tñ'", LogicalType::Text, "string", "tab\tñ"),
            (
                "cast('v' as varchar(10))",
                LogicalType::Text,
                if spark { "string" } else { "varchar(10)" },
                "v",
            ),
            (
                "cast('2024-02-29' as date)",
                LogicalType::Date,
                "date",
                "2024-02-29",
            ),
            (
                "cast('2024-02-29 23:59:58.123456' as timestamp)",
                LogicalType::Timestamp,
                "timestamp",
                "2024-02-29 23:59:58.123456",
            ),
            (
                "cast('ab' as binary)",
                LogicalType::Bytes,
                "binary",
                "\\x6162",
            ),
        ];
        // Impala builds nested values only in tables.
        if !server.is_impala() {
            cases.extend([
                (
                    "array(1, 2)",
                    LogicalType::Array,
                    // A result describes a nested type by its kind only.
                    "array",
                    "[1,2]",
                ),
                ("map('k', 1)", LogicalType::Native, "map", "{\"k\":1}"),
                (
                    "named_struct('a', 1, 'b', 'x')",
                    LogicalType::Native,
                    "struct",
                    "{\"a\":1,\"b\":\"x\"}",
                ),
            ]);
        }
        for (expr, logical, native, text) in cases {
            let (schema, rows) = query(conn, &format!("select {expr} as v")).await;
            assert_eq!(
                schema.columns[0].logical, logical,
                "{name}: {expr}: {:?}",
                schema.columns[0]
            );
            assert_eq!(schema.columns[0].native_type, native, "{name}: {expr}");
            assert_eq!(rows[0][0].to_string(), text, "{name}: {expr}");
        }
        let (_, rows) = query(conn, "select cast(null as int) as v").await;
        assert_eq!(rows[0][0], Value::Null, "{name}");
        // Dates and times far from 1970, both ways.
        let (_, rows) = query(
            conn,
            "select cast('0001-01-01' as date) as a, cast('9999-12-31 23:59:59.999999' as timestamp) as b",
        )
        .await;
        assert_eq!(rows[0][0].to_string(), "0001-01-01", "{name}");
        assert_eq!(
            rows[0][1].to_string(),
            "9999-12-31 23:59:59.999999",
            "{name}"
        );
    }
}

#[tokio::test]
async fn a_large_result_comes_in_batches() {
    for server in servers() {
        let conn = server.connect().await;
        // Ten rows, five times over: every engine here runs it.
        let sql = format!(
            "{DIGITS} select a.n from d a cross join d b cross join d c cross join d e \
             cross join d f limit 50000"
        );
        let events = conn
            .execute(
                &sql,
                &ExecOptions {
                    batch_size: 4096,
                    ..Default::default()
                },
            )
            .await
            .expect("runs")
            .try_collect::<Vec<_>>()
            .await
            .unwrap_or_else(|e| panic!("{}: {e}", server.name));
        let batches: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                ExecEvent::Batch(b) => Some(b.num_rows()),
                _ => None,
            })
            .collect();
        assert_eq!(batches.iter().sum::<usize>(), 50_000, "{}", server.name);
        assert!(batches.len() > 1, "{}: {batches:?}", server.name);
        assert!(batches.iter().all(|&n| n <= 4096), "{}", server.name);
    }
}

#[tokio::test]
async fn a_refused_statement_is_the_servers_error_and_the_session_goes_on() {
    for server in servers() {
        let conn = server.connect().await;
        let error = run(conn.as_ref(), "select * from a_table_that_is_not_there")
            .await
            .expect_err("refused");
        assert!(
            matches!(&error, DriverError::Database { message, .. } if message.to_ascii_lowercase().contains("a_table_that_is_not_there")),
            "{}: {error:?}",
            server.name
        );
        assert!(!conn.is_closed(), "{}", server.name);
        let (_, rows) = query(conn.as_ref(), "select 1").await;
        assert_eq!(rows[0][0], Value::Int(1), "{}", server.name);
    }
}

/// The explorer: databases (the schemas), tables and views with their comments, and columns in
/// their order with their types, in a database of the test's own.
#[tokio::test]
async fn the_catalog_is_browsed() {
    for server in servers() {
        let conn = server.connect().await;
        let conn = conn.as_ref();
        let name = server.name;
        let db = format!("spelaion_{name}_catalog");
        run(conn, &format!("drop database if exists {db} cascade"))
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        for statement in [
            format!("create database {db}"),
            format!(
                "create table {db}.orders (id bigint comment 'the key', amount decimal(12,2), placed timestamp, tags array<string>) comment 'every order'"
            ),
            format!("create view {db}.big_orders as select id from {db}.orders where amount > 100"),
        ] {
            run(conn, &statement)
                .await
                .unwrap_or_else(|e| panic!("{name}: {statement}: {e}"));
        }

        let schemas = conn.list_schemas().await.expect("schemas");
        assert!(schemas.iter().any(|s| s.name == db), "{name}: {schemas:?}");
        assert!(schemas.iter().any(|s| s.name == "default"), "{name}");

        let objects = conn.list_objects(&db).await.expect("objects");
        let kinds: Vec<_> = objects.iter().map(|o| (o.name.as_str(), o.kind)).collect();
        assert_eq!(
            kinds,
            vec![
                ("big_orders", ObjectKind::View),
                ("orders", ObjectKind::Table)
            ],
            "{name}"
        );

        let table = conn.describe_table(&db, "orders").await.expect("describe");
        let columns: Vec<_> = table
            .columns
            .iter()
            .map(|c| (c.name.as_str(), c.native_type.as_str()))
            .collect();
        assert_eq!(
            columns,
            vec![
                ("id", "bigint"),
                ("amount", "decimal(12,2)"),
                ("placed", "timestamp"),
                ("tags", "array<string>"),
            ],
            "{name}"
        );
        assert_eq!(
            table.columns[0].comment.as_deref(),
            Some("the key"),
            "{name}"
        );

        let all = conn.list_columns(&db).await.expect("columns");
        assert!(
            all.iter()
                .any(|c| c.table == "big_orders" && c.name == "id"),
            "{name}: {all:?}"
        );

        conn.select_database(&db).await.expect("use");
        assert_eq!(
            conn.current_database().await.expect("current"),
            Some(db.clone()),
            "{name}"
        );
        let definition = conn
            .relation_definition(&db, "big_orders")
            .await
            .expect("definition");
        assert!(
            definition.to_ascii_lowercase().contains("amount"),
            "{name}: {definition}"
        );
        // Impala does not drop the session's database.
        conn.select_database("default").await.expect("use");
        run(conn, &format!("drop database {db} cascade"))
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
    }
}

/// A cancel stops a statement that runs on the server (seen between two polls) and one that the
/// server runs inside a fetch (a query without `FROM` is run that way), in seconds, and leaves
/// the session usable.
#[tokio::test]
async fn a_cancel_stops_the_statement() {
    for server in servers() {
        let conn = server.connect().await;
        let name = server.name;
        for sql in server.slow_statements() {
            let handle = conn.cancel_handle();
            let stream = conn
                .execute(&sql, &ExecOptions::default())
                .await
                .expect("starts");
            let started = Instant::now();
            let cancel = tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(3)).await;
                handle.cancel().await
            });
            let outcome =
                tokio::time::timeout(Duration::from_secs(60), stream.try_collect::<Vec<_>>())
                    .await
                    .unwrap_or_else(|_| panic!("{name}: the cancel was lost: {sql}"));
            cancel.await.expect("joins").expect("cancels");
            assert!(
                matches!(outcome, Err(DriverError::Cancelled)),
                "{name}: {sql}: {outcome:?}"
            );
            assert!(started.elapsed() < Duration::from_secs(60), "{name}");
        }
        let (_, rows) = query(conn.as_ref(), "select 2").await;
        assert_eq!(rows[0][0], Value::Int(2), "{name}");
    }
}

/// The version is what the server says of itself.
#[tokio::test]
async fn the_server_says_what_it_is() {
    for server in servers() {
        let conn = server.connect().await;
        let version = conn.server_version().await.expect("version");
        let expected = match server.name {
            "spark" | "kyuubi" => "Spark",
            "impala" => "Impala",
            _ => "Hive",
        };
        assert!(version.contains(expected), "{}: {version}", server.name);
    }
}

/// Nothing listens: the connection fails in the time it is given, saying so.
#[tokio::test]
async fn a_server_that_is_not_there_fails_quickly() {
    let params = ConnectParams {
        host: "127.0.0.1".into(),
        port: 9,
        user: "spelaion".into(),
        tls: TlsMode::Disable,
        connect_timeout: Some(Duration::from_secs(3)),
        ..Default::default()
    };
    let started = Instant::now();
    let error = HiveDriver::hive()
        .connect(&params)
        .await
        .err()
        .expect("fails");
    assert!(matches!(error, DriverError::Connection(_)), "{error:?}");
    assert!(started.elapsed() < Duration::from_secs(10));
}
