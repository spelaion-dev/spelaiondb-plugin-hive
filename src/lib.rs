//! HiveServer2's protocol for SpelaionDB (its ADR-041 and ADR-042): one plugin for the engines
//! that speak it: Apache Hive, Spark's Thrift server (`spark`), Apache Kyuubi and Apache Impala.
//! They differ in what SpelaionDB's form offers and in how Impala logs in without a password.
//!
//! The protocol is Thrift's (`TCLIService`, [`tcliservice`] written by Thrift's compiler), here
//! on a thread of the connection's own because Thrift's Rust runtime blocks: the async calls of
//! the contract hand that thread a job and await its answer.
//!
//! A statement runs asynchronously on the server; its status is polled, so a cancel is seen
//! between two polls (or two fetches) and sent as `CancelOperation`. A cancel reaches the
//! statement running when it was asked and every one sent before it, never one sent after.
//!
//! Results come as columns ([`types`]). The explorer's schemas are Hive's databases, from
//! `GetSchemas`; tables and columns from `GetTables` and `GetColumns`.
//!
//! Declared, so SpelaionDB hides it: transactions, editing, importing, binding values, plans.
//! SpelaionDB gives these engines no read-only mode and no agents (no barrier of ours). Kerberos
//! is not spoken: SASL's `PLAIN` (no authentication, LDAP or a custom provider) or no SASL.

mod hs2;
#[allow(
    clippy::all,
    clippy::pedantic,
    dead_code,
    unused,
    non_camel_case_types,
    non_snake_case
)]
mod tcliservice;
pub mod tls;
pub mod types;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc as std_mpsc};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use spelaion_driver_api::{
    CancelHandle, Capabilities, ColumnInfo, ConnectParams, Connection, Driver, DriverError,
    EventStream, ExecEvent, ExecOptions, ObjectInfo, ObjectKind, ReferencingKey, Result,
    SchemaColumn, SchemaInfo, Secret, TableInfo,
};
use tokio::sync::{mpsc, oneshot};

use crate::hs2::{Auth, Session, Target};
use crate::tcliservice::{TGetInfoType, TOperationHandle, TOperationState};

/// One engine name this plugin serves.
#[derive(Debug, Clone)]
pub struct HiveDriver {
    engine: &'static str,
}

impl HiveDriver {
    pub fn hive() -> Self {
        Self { engine: "hive" }
    }

    /// Spark's Thrift server (Spark SQL through HiveServer2's protocol).
    pub fn spark() -> Self {
        Self { engine: "spark" }
    }

    pub fn kyuubi() -> Self {
        Self { engine: "kyuubi" }
    }

    pub fn impala() -> Self {
        Self { engine: "impala" }
    }
}

#[async_trait]
impl Driver for HiveDriver {
    fn engine(&self) -> &str {
        self.engine
    }

    async fn connect(&self, params: &ConnectParams) -> Result<Box<dyn Connection>> {
        if params.client_certificate.is_some() || params.client_key.is_some() {
            return Err(DriverError::Connection(
                "HiveServer2 logs in with a user and a password: client certificates are not supported"
                    .into(),
            ));
        }
        let tls = tls::configure(params.tls, params.ca_certificate.as_deref(), None)?;
        let password = params
            .password
            .as_ref()
            .map(|p| p.expose().to_string())
            .unwrap_or_default();
        // Impala with no authentication speaks no SASL; with LDAP it speaks `PLAIN`. The others
        // always speak `PLAIN`, with any password when they check none.
        let auth = if self.engine == "impala" && password.is_empty() {
            Auth::NoSasl
        } else {
            let user = if params.user.is_empty() {
                "anonymous".to_string()
            } else {
                params.user.clone()
            };
            Auth::Plain {
                user,
                password: if password.is_empty() {
                    "anonymous".into()
                } else {
                    password
                },
            }
        };
        let target = Target {
            host: params.host.clone(),
            port: params.port,
            connect_addr: params.connect_addr,
            tls,
            auth,
            connect_timeout: params.connect_timeout(),
            database: (!params.database.is_empty()).then(|| params.database.clone()),
        };
        let scrub = Scrub(params.password.clone());
        let closed = Arc::new(AtomicBool::new(false));
        let (jobs, opened) = Worker::start(target.clone(), scrub.clone(), closed.clone()).await?;
        Ok(Box::new(HiveConnection {
            capabilities: Capabilities {
                transactions: false,
                cancel: true,
                explain: false,
                editable_results: false,
                schemas: true,
                insert_rows: false,
                bound_params: false,
            },
            jobs,
            cancel: Arc::new(Cancel {
                sent: AtomicU64::new(0),
                upto: AtomicU64::new(0),
                running: Mutex::new(None),
                target,
            }),
            closed,
            scrub,
            server: opened,
        }))
    }
}

/// Takes the password out of what the server or the transport says.
#[derive(Clone)]
struct Scrub(Option<Secret>);

impl Scrub {
    fn text(&self, text: &str) -> String {
        match &self.0 {
            Some(password) if !password.expose().is_empty() => {
                text.replace(password.expose(), "***")
            }
            _ => text.to_string(),
        }
    }
}

/// Cancels: remembers the last statement sent when it was asked, so that statement and every
/// one before it stop, and none sent after it is touched.
struct Cancel {
    sent: AtomicU64,
    upto: AtomicU64,
    /// The statement running now, by number, and its operation on the server.
    running: Mutex<Option<(u64, TOperationHandle)>>,
    /// Where a cancel is sent from while the connection's own socket waits on a fetch.
    target: Target,
}

impl Cancel {
    fn asked(&self, seq: u64) -> bool {
        self.upto.load(Ordering::SeqCst) >= seq
    }

    fn set_running(&self, running: Option<(u64, TOperationHandle)>) {
        *self.running.lock().unwrap_or_else(|e| e.into_inner()) = running;
    }
}

struct HiveCancel(Arc<Cancel>);

#[async_trait]
impl CancelHandle for HiveCancel {
    /// The connection's thread sees a cancel between two status polls or two fetches. But Hive
    /// runs a simple query (no `FROM`, or a plain scan) inside the fetch itself, which may then
    /// take as long as the query: so the running operation is also cancelled from a second,
    /// short connection (HiveServer2 finds an operation by its handle, whatever the session).
    async fn cancel(&self) -> Result<()> {
        let sent = self.0.sent.load(Ordering::SeqCst);
        self.0.upto.fetch_max(sent, Ordering::SeqCst);
        let running = self
            .0
            .running
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let Some((seq, operation)) = running else {
            return Ok(());
        };
        if !self.0.asked(seq) {
            return Ok(());
        }
        let target = self.0.target.clone();
        let timeout = target.connect_timeout + Duration::from_secs(5);
        let sent = tokio::task::spawn_blocking(move || {
            if let Ok(mut session) = Session::open(&target) {
                let _ = session.cancel(&operation);
                session.close();
            }
        });
        // Best effort: the connection's thread cancels it too as soon as it can.
        let _ = tokio::time::timeout(timeout, sent).await;
        Ok(())
    }
}

/// What the connection's thread runs, one at a time, on its session.
type Job = Box<dyn FnOnce(&mut Worker) + Send>;

/// The connection's thread: the session and what it needs to answer.
struct Worker {
    session: Session,
    scrub: Scrub,
    closed: Arc<AtomicBool>,
}

/// What the server said of itself when the session opened.
#[derive(Clone)]
struct ServerInfo {
    version: String,
}

impl Worker {
    /// Opens the session on a thread of its own, which then runs the jobs it is sent until the
    /// connection is dropped, and closes the session.
    async fn start(
        target: Target,
        scrub: Scrub,
        closed: Arc<AtomicBool>,
    ) -> Result<(std_mpsc::Sender<Job>, ServerInfo)> {
        let (jobs, receiver) = std_mpsc::channel::<Job>();
        let (opened, answer) = oneshot::channel::<Result<ServerInfo>>();
        let timeout = target.connect_timeout;
        std::thread::Builder::new()
            .name("hiveserver2".into())
            .spawn(move || {
                let mut session = match Session::open(&target) {
                    Ok(session) => session,
                    Err(e) => {
                        let _ = opened.send(Err(login_error(e, &scrub)));
                        return;
                    }
                };
                let name = session
                    .info(TGetInfoType::CLI_DBMS_NAME)
                    .unwrap_or_default();
                let version = session.info(TGetInfoType::CLI_DBMS_VER).unwrap_or_default();
                let version = format!("{name} {version}").trim().to_string();
                if opened.send(Ok(ServerInfo { version })).is_err() {
                    session.close();
                    return;
                }
                let mut worker = Worker {
                    session,
                    scrub,
                    closed,
                };
                while let Ok(job) = receiver.recv() {
                    job(&mut worker);
                }
                worker.session.close();
            })
            .map_err(|e| DriverError::Connection(format!("the connection's thread: {e}")))?;
        match tokio::time::timeout(timeout + Duration::from_secs(5), answer).await {
            Ok(Ok(Ok(info))) => Ok((jobs, info)),
            Ok(Ok(Err(e))) => Err(e),
            Ok(Err(_)) => Err(DriverError::Connection(
                "the connection's thread ended before it connected".into(),
            )),
            Err(_) => Err(DriverError::Connection(format!(
                "the server did not answer within {} s",
                timeout.as_secs()
            ))),
        }
    }

    /// A call's failure as the contract's error: a lost socket loses the connection.
    fn error(&self, error: hs2::Error) -> DriverError {
        match error {
            hs2::Error::Lost(message) => {
                self.closed.store(true, Ordering::SeqCst);
                DriverError::Connection(self.scrub.text(&message))
            }
            hs2::Error::Server { sql_state, message } => DriverError::Database {
                code: sql_state,
                message: self.scrub.text(&message),
                detail: None,
                hint: None,
                position: None,
            },
        }
    }

    /// Waits until an operation ends, asking it to stop when `stop` says so.
    fn wait(
        &mut self,
        operation: &TOperationHandle,
        stop: &dyn Fn() -> bool,
    ) -> Result<Option<i64>> {
        let mut pause = Duration::from_millis(5);
        loop {
            let status = self.session.status(operation).map_err(|e| self.error(e))?;
            match status.operation_state {
                Some(TOperationState::FINISHED_STATE) => return Ok(status.num_modified_rows),
                Some(TOperationState::ERROR_STATE) => {
                    return Err(DriverError::Database {
                        code: status.sql_state.filter(|s| !s.is_empty()),
                        message: self.scrub.text(
                            status
                                .error_message
                                .as_deref()
                                .filter(|m| !m.is_empty())
                                .unwrap_or("the statement failed"),
                        ),
                        detail: None,
                        hint: None,
                        position: None,
                    });
                }
                Some(TOperationState::CANCELED_STATE) | Some(TOperationState::CLOSED_STATE) => {
                    return Err(DriverError::Cancelled);
                }
                Some(TOperationState::TIMEDOUT_STATE) => {
                    return Err(DriverError::Other(
                        "the statement timed out on the server".into(),
                    ));
                }
                _ => {}
            }
            if stop() {
                let _ = self.session.cancel(operation);
                return Err(DriverError::Cancelled);
            }
            std::thread::sleep(pause);
            pause = (pause * 2).min(Duration::from_millis(200));
        }
    }

    /// Every row of a metadata call (or a short statement), as text.
    fn rows(
        &mut self,
        operation: hs2::Result<TOperationHandle>,
    ) -> Result<Vec<Vec<Option<String>>>> {
        let operation = operation.map_err(|e| self.error(e))?;
        let outcome = (|| {
            self.wait(&operation, &|| false)?;
            let mut out = Vec::new();
            // A statement with no result (`USE`) has nothing to fetch, and the server says so.
            if !operation.has_result_set {
                return Ok(out);
            }
            loop {
                let rows = self
                    .session
                    .fetch(&operation, 10_000)
                    .map_err(|e| self.error(e))?;
                let batch = types::text_rows(&rows);
                if batch.is_empty() {
                    break;
                }
                out.extend(batch);
            }
            Ok(out)
        })();
        let _ = self.session.close_operation(operation);
        outcome
    }

    /// The rows of a metadata call, by column name.
    fn named_rows(
        &mut self,
        operation: hs2::Result<TOperationHandle>,
        names: &[&str],
    ) -> Result<Vec<Vec<Option<String>>>> {
        let operation = operation.map_err(|e| self.error(e))?;
        let schema = (|| {
            self.wait(&operation, &|| false)?;
            self.session.metadata(&operation).map_err(|e| self.error(e))
        })();
        let schema = match schema {
            Ok(schema) => schema,
            Err(e) => {
                let _ = self.session.close_operation(operation);
                return Err(e);
            }
        };
        // A name may have spellings: `TABLE_SCHEM` is JDBC's, and Impala's `GetColumns` says
        // `TABLE_MD`.
        let positions: Vec<Option<usize>> = names
            .iter()
            .map(|names| {
                names.split('|').find_map(|name| {
                    schema
                        .columns
                        .iter()
                        .position(|c| c.column_name.eq_ignore_ascii_case(name))
                })
            })
            .collect();
        let rows = self.rows(Ok(operation))?;
        Ok(rows
            .into_iter()
            .map(|row| {
                positions
                    .iter()
                    .map(|p| p.and_then(|p| row.get(p).cloned().flatten()))
                    .collect()
            })
            .collect())
    }

    /// Runs one statement and streams its result.
    fn execute(
        &mut self,
        sql: &str,
        batch_size: usize,
        seq: u64,
        cancel: &Cancel,
        events: &mpsc::Sender<Result<ExecEvent>>,
    ) -> Result<()> {
        if cancel.asked(seq) {
            return Err(DriverError::Cancelled);
        }
        let operation = self.session.execute(sql).map_err(|e| self.error(e))?;
        cancel.set_running(Some((seq, operation.clone())));
        let outcome = self.stream(&operation, batch_size, seq, cancel, events);
        cancel.set_running(None);
        let _ = self.session.close_operation(operation);
        // What fails after a cancel was asked is the cancel (a fetch the server cut short).
        match outcome {
            Err(DriverError::Database { .. } | DriverError::Other(_)) if cancel.asked(seq) => {
                Err(DriverError::Cancelled)
            }
            outcome => outcome,
        }
    }

    fn stream(
        &mut self,
        operation: &TOperationHandle,
        batch_size: usize,
        seq: u64,
        cancel: &Cancel,
        events: &mpsc::Sender<Result<ExecEvent>>,
    ) -> Result<()> {
        let modified = self.wait(operation, &|| cancel.asked(seq))?;
        if !operation.has_result_set {
            let _ = events.blocking_send(Ok(ExecEvent::Done {
                rows_affected: modified.and_then(|n| u64::try_from(n).ok()),
            }));
            return Ok(());
        }
        let schema = types::result_schema(
            &self
                .session
                .metadata(operation)
                .map_err(|e| self.error(e))?,
        );
        if events
            .blocking_send(Ok(ExecEvent::Columns(schema.clone())))
            .is_err()
        {
            return Ok(());
        }
        let max_rows = i64::try_from(batch_size.clamp(1, 100_000)).unwrap_or(10_000);
        loop {
            if cancel.asked(seq) {
                let _ = self.session.cancel(operation);
                return Err(DriverError::Cancelled);
            }
            let rows = self
                .session
                .fetch(operation, max_rows)
                .map_err(|e| self.error(e))?;
            // `hasMoreRows` is not reliable across servers: the result ends with an empty fetch.
            if types::row_count(&rows) == 0 {
                break;
            }
            let batch = types::convert(&rows, &schema)?;
            if events.blocking_send(Ok(ExecEvent::Batch(batch))).is_err() {
                // Nobody reads any more: the statement stops.
                let _ = self.session.cancel(operation);
                return Ok(());
            }
        }
        let _ = events.blocking_send(Ok(ExecEvent::Done {
            rows_affected: None,
        }));
        Ok(())
    }
}

/// A failed login as the contract's error.
fn login_error(error: hs2::Error, scrub: &Scrub) -> DriverError {
    match error {
        hs2::Error::Lost(message) | hs2::Error::Server { message, .. } => {
            DriverError::Connection(scrub.text(&message))
        }
    }
}

struct HiveConnection {
    capabilities: Capabilities,
    jobs: std_mpsc::Sender<Job>,
    cancel: Arc<Cancel>,
    closed: Arc<AtomicBool>,
    scrub: Scrub,
    server: ServerInfo,
}

impl HiveConnection {
    /// Runs `job` on the connection's thread and waits for what it answers.
    async fn call<T: Send + 'static>(
        &self,
        job: impl FnOnce(&mut Worker) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(DriverError::Connection("the connection is closed".into()));
        }
        let (reply, answer) = oneshot::channel();
        self.jobs
            .send(Box::new(move |worker: &mut Worker| {
                let _ = reply.send(job(worker));
            }))
            .map_err(|_| DriverError::Connection("the connection is closed".into()))?;
        answer
            .await
            .map_err(|_| DriverError::Connection("the connection is closed".into()))?
    }

    async fn tables(&self, schema: &str) -> Result<Vec<(String, ObjectKind, Option<String>)>> {
        let wanted = schema.to_string();
        let rows = self
            .call(move |w| {
                let op = w.session.tables(&wanted, None);
                w.named_rows(
                    op,
                    &[
                        "TABLE_SCHEM|TABLE_MD",
                        "TABLE_NAME",
                        "TABLE_TYPE",
                        "REMARKS",
                    ],
                )
            })
            .await?;
        let mut out: Vec<_> = rows
            .into_iter()
            // The filters are patterns (`_` is any character): only exact names count.
            .filter(|row| {
                row[0]
                    .as_deref()
                    .is_some_and(|s| s.eq_ignore_ascii_case(schema))
            })
            .filter_map(|row| {
                let name = row[1].clone()?;
                Some((
                    name,
                    kind(row[2].as_deref().unwrap_or("")),
                    row[3].clone().filter(|c| !c.is_empty()),
                ))
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    /// Each column of a schema, or of one table: table, name, type, nullable, comment.
    async fn columns(
        &self,
        schema: &str,
        table: Option<&str>,
    ) -> Result<Vec<(String, ColumnInfo)>> {
        let wanted = schema.to_string();
        let wanted_table = table.map(str::to_string);
        let rows = self
            .call(move |w| {
                let op = w.session.columns(&wanted, wanted_table.as_deref());
                w.named_rows(
                    op,
                    &[
                        "TABLE_SCHEM|TABLE_MD",
                        "TABLE_NAME",
                        "COLUMN_NAME",
                        "TYPE_NAME",
                        "NULLABLE",
                        "REMARKS",
                        "ORDINAL_POSITION",
                        "COLUMN_SIZE",
                        "DECIMAL_DIGITS",
                    ],
                )
            })
            .await?;
        let mut out: Vec<(String, i64, ColumnInfo)> = rows
            .into_iter()
            .filter(|row| {
                row[0]
                    .as_deref()
                    .is_some_and(|s| s.eq_ignore_ascii_case(schema))
                    && table.is_none_or(|t| {
                        row[1].as_deref().is_some_and(|n| n.eq_ignore_ascii_case(t))
                    })
            })
            .filter_map(|row| {
                Some((
                    row[1].clone()?,
                    row[6].as_deref().and_then(|p| p.parse().ok()).unwrap_or(0),
                    ColumnInfo {
                        name: row[2].clone()?,
                        native_type: column_type(
                            row[3].as_deref().unwrap_or_default(),
                            row[7].as_deref(),
                            row[8].as_deref(),
                        ),
                        // JDBC's `columnNoNulls` is 0.
                        nullable: row[4].as_deref() != Some("0"),
                        default: None,
                        comment: row[5].clone().filter(|c| !c.is_empty()),
                    },
                ))
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        Ok(out.into_iter().map(|(t, _, c)| (t, c)).collect())
    }
}

/// A column's type as `DESCRIBE` writes it: `GetColumns` gives a decimal's precision and scale,
/// and a `varchar`'s or `char`'s length, apart from its type name.
fn column_type(name: &str, size: Option<&str>, digits: Option<&str>) -> String {
    let name = name.to_ascii_lowercase();
    if name.contains('(') {
        return name;
    }
    match (name.as_str(), size, digits) {
        ("decimal", Some(p), Some(s)) => format!("decimal({p},{s})"),
        ("varchar" | "char", Some(n), _) => format!("{name}({n})"),
        _ => name,
    }
}

fn kind(table_type: &str) -> ObjectKind {
    match table_type.to_ascii_uppercase().replace('_', " ").as_str() {
        "VIEW" | "VIRTUAL VIEW" => ObjectKind::View,
        "MATERIALIZED VIEW" => ObjectKind::MaterializedView,
        _ => ObjectKind::Table,
    }
}

/// A name in backquotes, as HiveQL, Spark SQL and Impala quote one.
fn quote(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

#[async_trait]
impl Connection for HiveConnection {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    async fn execute(&self, sql: &str, opts: &ExecOptions) -> Result<EventStream> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(DriverError::Connection("the connection is closed".into()));
        }
        let seq = self.cancel.sent.fetch_add(1, Ordering::SeqCst) + 1;
        let (events, receiver) = mpsc::channel(4);
        let cancel = self.cancel.clone();
        let sql = sql.to_string();
        let batch_size = opts.batch_size.max(1);
        let job: Job = Box::new(move |worker: &mut Worker| {
            if let Err(e) = worker.execute(&sql, batch_size, seq, &cancel, &events) {
                let _ = events.blocking_send(Err(e));
            }
        });
        self.jobs
            .send(job)
            .map_err(|_| DriverError::Connection("the connection is closed".into()))?;
        let mut receiver = receiver;
        Ok(futures_util::stream::poll_fn(move |cx| receiver.poll_recv(cx)).boxed())
    }

    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::new(HiveCancel(self.cancel.clone()))
    }

    async fn server_version(&self) -> Result<String> {
        Ok(self.server.version.clone())
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    async fn current_database(&self) -> Result<Option<String>> {
        let rows = self
            .call(|w| {
                let op = w.session.execute("SELECT current_database()");
                w.rows(op)
            })
            .await?;
        Ok(rows
            .into_iter()
            .next()
            .and_then(|row| row.into_iter().next().flatten())
            .filter(|name| !name.is_empty()))
    }

    async fn select_database(&self, database: &str) -> Result<()> {
        let statement = format!("USE {}", quote(database));
        self.call(move |w| {
            let op = w.session.execute(&statement);
            w.rows(op)
        })
        .await
        .map(|_| ())
    }

    async fn list_databases(&self) -> Result<Vec<String>> {
        Ok(self
            .list_schemas()
            .await?
            .into_iter()
            .map(|schema| schema.name)
            .collect())
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        let rows = self
            .call(|w| {
                let op = w.session.schemas();
                w.named_rows(op, &["TABLE_SCHEM|TABLE_MD"])
            })
            .await?;
        let mut out: Vec<SchemaInfo> = rows
            .into_iter()
            .filter_map(|row| row.into_iter().next().flatten())
            .map(|name| SchemaInfo {
                system: matches!(
                    name.to_ascii_lowercase().as_str(),
                    "information_schema" | "sys" | "_impala_builtins"
                ),
                name,
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out.dedup_by(|a, b| a.name == b.name);
        Ok(out)
    }

    async fn list_objects(&self, schema: &str) -> Result<Vec<ObjectInfo>> {
        Ok(self
            .tables(schema)
            .await?
            .into_iter()
            .map(|(name, kind, comment)| ObjectInfo {
                schema: schema.to_string(),
                name,
                kind,
                estimated_rows: None,
                comment,
            })
            .collect())
    }

    async fn describe_table(&self, schema: &str, name: &str) -> Result<TableInfo> {
        let (table, kind, comment) = self
            .tables(schema)
            .await?
            .into_iter()
            .find(|(table, _, _)| table.eq_ignore_ascii_case(name))
            .ok_or_else(|| DriverError::Other(format!("there is no table {schema}.{name}")))?;
        let columns = self
            .columns(schema, Some(&table))
            .await?
            .into_iter()
            .map(|(_, column)| column)
            .collect();
        Ok(TableInfo {
            object: ObjectInfo {
                schema: schema.to_string(),
                name: table,
                kind,
                estimated_rows: None,
                comment,
            },
            columns,
            primary_key: Vec::new(),
            indexes: Vec::new(),
            foreign_keys: Vec::new(),
            constraints: Vec::new(),
        })
    }

    async fn list_columns(&self, schema: &str) -> Result<Vec<SchemaColumn>> {
        Ok(self
            .columns(schema, None)
            .await?
            .into_iter()
            .map(|(table, column)| SchemaColumn {
                table,
                name: column.name,
                native_type: column.native_type,
            })
            .collect())
    }

    async fn list_referencing_keys(
        &self,
        _schema: &str,
        _name: &str,
    ) -> Result<Vec<ReferencingKey>> {
        Ok(Vec::new())
    }

    /// A view's statement, as the server writes it (`SHOW CREATE TABLE`, one row per line).
    async fn relation_definition(&self, schema: &str, name: &str) -> Result<String> {
        let statement = format!("SHOW CREATE TABLE {}.{}", quote(schema), quote(name));
        let rows = self
            .call(move |w| {
                let op = w.session.execute(&statement);
                w.rows(op)
            })
            .await?;
        let text = rows
            .into_iter()
            .filter_map(|row| row.into_iter().next().flatten())
            .collect::<Vec<_>>()
            .join("\n");
        Ok(self.scrub.text(&text))
    }
}
