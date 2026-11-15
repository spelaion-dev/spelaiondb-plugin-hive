//! The conformance set of SpelaionDB's plugin protocol, run against this plugin's own program
//! (ADR-042), once per engine it serves: it starts, speaks the protocol's version, opens
//! sessions, returns results in the order the contract says, fails without hanging, obeys a
//! cancel while a statement runs and ends when its input closes.
//!
//! It needs the test servers (`ci/services.sh`, `SPELAION_TEST_HIVE=127.0.0.1:11000`,
//! `SPELAION_TEST_SPARK=127.0.0.1:11001`, `SPELAION_TEST_KYUUBI=127.0.0.1:11009`). Without a
//! variable its engine is skipped, which does not count as tested.

use std::time::Duration;

use spelaion_driver_api::{ConnectParams, Secret, TlsMode};
use spelaion_plugin_host::conformance::{Target, assert_conforms};

const PROGRAM: &str = env!("CARGO_BIN_EXE_spelaiondb-plugin-hive");

#[tokio::test]
async fn the_plugin_conforms() {
    let hive_sleep = "select count(*) from (select reflect('java.lang.Thread', 'sleep', 600000L) as s) t group by s";
    let spark_sleep = "select count(*) from (select java_method('java.lang.Thread', 'sleep', 600000L) as s) t group by s";
    // Impala's `sleep()` does not stop on a cancel: a join of ten billion rows does.
    let impala_join = "with d as (select 0 as n union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) select count(*) from d a, d b, d c, d e, d f, d g, d h, d i, d j, d k";
    for (variable, engine, slow) in [
        ("SPELAION_TEST_HIVE", "hive", hive_sleep),
        ("SPELAION_TEST_SPARK", "spark", spark_sleep),
        ("SPELAION_TEST_KYUUBI", "kyuubi", spark_sleep),
        // Not among the test servers: `testing/impala/`, by hand.
        ("SPELAION_TEST_IMPALA", "impala", impala_join),
    ] {
        let Ok(addr) = std::env::var(variable) else {
            eprintln!("{variable} not set: {engine} skipped");
            continue;
        };
        let (host, port) = addr.rsplit_once(':').expect("host:port");
        let target = Target {
            program: PROGRAM.into(),
            engine: engine.into(),
            params: ConnectParams {
                host: host.into(),
                port: port.parse().expect("a port"),
                user: "spelaion".into(),
                // Impala with no authentication speaks no SASL: no password says so.
                password: (engine != "impala").then(|| Secret::new("not-checked-here")),
                tls: TlsMode::Disable,
                // Kyuubi starts its engine on the first session.
                connect_timeout: Some(Duration::from_secs(120)),
                ..Default::default()
            },
            one_row: "select 1".into(),
            refused: "select * from a_table_that_is_not_there".into(),
            slow: Some(slow.to_string()),
        };
        assert_conforms(&target).await;
    }
}

/// `plugin.toml` is what SpelaionDB's pin says of this plugin: it must be what the program
/// says of itself. The program says it without a server.
#[tokio::test]
async fn the_manifest_is_what_the_program_says() {
    let manifest = include_str!("../plugin.toml");
    let value = |key: &str| -> String {
        manifest
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{key} = ")))
            .unwrap_or_else(|| panic!("plugin.toml has no `{key}`"))
            .trim()
            .to_string()
    };
    let plugin = spelaion_plugin_host::Plugin::new(PROGRAM);
    let info = plugin.describe().await.expect("the program starts");

    assert_eq!(value("protocol"), format!("\"{}\"", info.protocol));
    let engines: Vec<String> = info.engines.iter().map(|e| format!("\"{e}\"")).collect();
    assert_eq!(value("engines"), format!("[{}]", engines.join(", ")));
    assert_eq!(info.plugin.name, env!("CARGO_PKG_NAME"));
    assert_eq!(info.plugin.version, env!("CARGO_PKG_VERSION"));
    let min_app = value("min_app");
    let numbers = min_app.trim_matches('"').split('.').count();
    assert_eq!(numbers, 3, "min_app = {min_app}");
}
