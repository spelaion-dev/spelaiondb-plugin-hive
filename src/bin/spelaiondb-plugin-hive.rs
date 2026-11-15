//! The plugin's program: the HiveServer2 driver, served to SpelaionDB over its plugin protocol on
//! the program's standard input and output (SpelaionDB's ADR-041).
//!
//! It does nothing else. Standard output is the protocol: nothing here, and nothing in the
//! driver, ever prints to it.

use std::sync::Arc;

use spelaion_driver_hive::HiveDriver;
use spelaion_plugin_sdk::{Plugin, run};

#[tokio::main]
async fn main() {
    // One process may have several rustls providers linked in: say which.
    let _ = rustls::crypto::ring::default_provider().install_default();
    run(Plugin::new(
        "spelaiondb-plugin-hive",
        env!("CARGO_PKG_VERSION"),
        vec![
            Arc::new(HiveDriver::hive()),
            Arc::new(HiveDriver::spark()),
            Arc::new(HiveDriver::kyuubi()),
            Arc::new(HiveDriver::impala()),
        ],
    ))
    .await
}
