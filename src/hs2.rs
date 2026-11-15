//! HiveServer2's wire: a TCP socket (TLS when asked), SASL's `PLAIN` exchange or none at all,
//! then Thrift's binary protocol over it, framed after SASL as SASL's transport frames it.
//!
//! Everything here is blocking and lives on the connection's own thread ([`crate`]'s actor):
//! Thrift's Rust runtime reads and writes with `std::io`.

use std::cell::RefCell;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, StreamOwned};
use thrift::protocol::{
    TBinaryInputProtocol, TBinaryOutputProtocol, TInputProtocol, TOutputProtocol,
};
use thrift::transport::{
    TBufferedReadTransport, TBufferedWriteTransport, TFramedReadTransport, TFramedWriteTransport,
};

use crate::tcliservice::{
    TCLIServiceSyncClient, TCancelOperationReq, TCloseOperationReq, TCloseSessionReq,
    TExecuteStatementReq, TFetchOrientation, TFetchResultsReq, TGetColumnsReq, TGetInfoReq,
    TGetInfoType, TGetInfoValue, TGetOperationStatusReq, TGetOperationStatusResp,
    TGetResultSetMetadataReq, TGetSchemasReq, TGetTablesReq, TOpenSessionReq, TOperationHandle,
    TProtocolVersion, TRowSet, TSessionHandle, TStatus, TStatusCode, TTCLIServiceSyncClient,
    TTableSchema,
};
use crate::tls::Tls;

/// How the client logs in.
#[derive(Clone)]
pub enum Auth {
    /// SASL's `PLAIN`: what HiveServer2 speaks with no authentication (any user, any password)
    /// and with LDAP or a custom provider (the password checked).
    Plain { user: String, password: String },
    /// No SASL at all (`NOSASL`): Impala without authentication.
    NoSasl,
}

/// Where and how to connect.
#[derive(Clone)]
pub struct Target {
    pub host: String,
    pub port: u16,
    pub connect_addr: Option<SocketAddr>,
    pub tls: Tls,
    pub auth: Auth,
    pub connect_timeout: Duration,
    /// The database the session starts in, if any.
    pub database: Option<String>,
}

/// What went wrong in a call.
#[derive(Debug)]
pub enum Error {
    /// The socket or the protocol failed: the connection is gone.
    Lost(String),
    /// The server refused the call; the connection is still good.
    Server {
        sql_state: Option<String>,
        message: String,
    },
}

impl Error {
    fn server(message: impl Into<String>) -> Self {
        Self::Server {
            sql_state: None,
            message: message.into(),
        }
    }
}

impl From<thrift::Error> for Error {
    fn from(error: thrift::Error) -> Self {
        match error {
            thrift::Error::Transport(e) => Self::Lost(e.message),
            thrift::Error::Protocol(e) => Self::Lost(e.message),
            thrift::Error::Application(e) => Self::server(e.message),
            thrift::Error::User(e) => Self::server(e.to_string()),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// The socket, plain or in TLS, shared by Thrift's reading and writing halves on one thread.
trait Stream: Read + Write {}
impl<T: Read + Write> Stream for T {}

#[derive(Clone)]
struct Shared(Rc<RefCell<Box<dyn Stream>>>);

impl Read for Shared {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.borrow_mut().read(buf)
    }
}

impl Write for Shared {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.borrow_mut().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.borrow_mut().flush()
    }
}

type Client = TCLIServiceSyncClient<Box<dyn TInputProtocol>, Box<dyn TOutputProtocol>>;

/// How long one call may take before the connection is given up on. Statements run
/// asynchronously on the server (their status is polled), so no call should come near it.
const CALL_TIMEOUT: Duration = Duration::from_secs(300);

/// The protocol asked for: the newest this file knows. The server answers with the newest both
/// know, and a server that knows less still serves every call made here.
const PROTOCOL: TProtocolVersion = TProtocolVersion::HIVE_CLI_SERVICE_PROTOCOL_V10;

/// One HiveServer2 session.
pub struct Session {
    client: Client,
    handle: TSessionHandle,
}

impl Session {
    pub fn open(target: &Target) -> Result<Self> {
        let addrs: Vec<SocketAddr> = match target.connect_addr {
            Some(addr) => vec![addr],
            None => (target.host.as_str(), target.port)
                .to_socket_addrs()
                .map_err(|e| Error::Lost(format!("{}: {e}", target.host)))?
                .collect(),
        };
        let stream = match &target.tls {
            Tls::Off => plain(&addrs, target.connect_timeout)?,
            Tls::Required(config) => tls(&addrs, target, config.clone())?,
            Tls::Preferred(config) => match tls(&addrs, target, config.clone()) {
                Ok(stream) => stream,
                // No TLS on the other side: plain, as libpq falls back.
                Err(_) => plain(&addrs, target.connect_timeout)?,
            },
        };
        let shared = Shared(Rc::new(RefCell::new(stream)));
        let (input, output): (Box<dyn TInputProtocol>, Box<dyn TOutputProtocol>) =
            match &target.auth {
                Auth::Plain { user, password } => {
                    sasl_plain(&mut shared.clone(), user, password)?;
                    (
                        Box::new(TBinaryInputProtocol::new(
                            TFramedReadTransport::new(shared.clone()),
                            true,
                        )),
                        Box::new(TBinaryOutputProtocol::new(
                            TFramedWriteTransport::new(shared),
                            true,
                        )),
                    )
                }
                Auth::NoSasl => (
                    Box::new(TBinaryInputProtocol::new(
                        TBufferedReadTransport::new(shared.clone()),
                        true,
                    )),
                    Box::new(TBinaryOutputProtocol::new(
                        TBufferedWriteTransport::new(shared),
                        true,
                    )),
                ),
            };
        let mut client = TCLIServiceSyncClient::new(input, output);
        let (username, password) = match &target.auth {
            Auth::Plain { user, password } => (Some(user.clone()), Some(password.clone())),
            Auth::NoSasl => (None, None),
        };
        let configuration = target.database.as_ref().map(|database| {
            [("use:database".to_string(), database.clone())]
                .into_iter()
                .collect()
        });
        let open = client.open_session(TOpenSessionReq {
            client_protocol: PROTOCOL,
            username,
            password,
            configuration,
        })?;
        check(&open.status)?;
        let handle = open
            .session_handle
            .ok_or_else(|| Error::Lost("the server opened no session".into()))?;
        Ok(Self { client, handle })
    }

    /// Runs `statement` on the server without waiting for it.
    pub fn execute(&mut self, statement: &str) -> Result<TOperationHandle> {
        let answer = self.client.execute_statement(TExecuteStatementReq {
            session_handle: self.handle.clone(),
            statement: statement.to_string(),
            conf_overlay: None,
            run_async: Some(true),
            query_timeout: None,
        })?;
        check(&answer.status)?;
        answer
            .operation_handle
            .ok_or_else(|| Error::server("the server started no operation"))
    }

    pub fn status(&mut self, operation: &TOperationHandle) -> Result<TGetOperationStatusResp> {
        let answer = self.client.get_operation_status(TGetOperationStatusReq {
            operation_handle: operation.clone(),
            get_progress_update: None,
        })?;
        check(&answer.status)?;
        Ok(answer)
    }

    pub fn metadata(&mut self, operation: &TOperationHandle) -> Result<TTableSchema> {
        let answer = self
            .client
            .get_result_set_metadata(TGetResultSetMetadataReq {
                operation_handle: operation.clone(),
            })?;
        check(&answer.status)?;
        answer
            .schema
            .ok_or_else(|| Error::server("the server described no result"))
    }

    /// The next rows of a result: none once it is read whole.
    pub fn fetch(&mut self, operation: &TOperationHandle, max_rows: i64) -> Result<TRowSet> {
        let answer = self.client.fetch_results(TFetchResultsReq {
            operation_handle: operation.clone(),
            orientation: TFetchOrientation::FETCH_NEXT,
            max_rows,
            fetch_type: None,
        })?;
        check(&answer.status)?;
        answer
            .results
            .ok_or_else(|| Error::server("the server sent no rows"))
    }

    pub fn cancel(&mut self, operation: &TOperationHandle) -> Result<()> {
        let answer = self.client.cancel_operation(TCancelOperationReq {
            operation_handle: operation.clone(),
        })?;
        check(&answer.status)
    }

    pub fn close_operation(&mut self, operation: TOperationHandle) -> Result<()> {
        let answer = self.client.close_operation(TCloseOperationReq {
            operation_handle: operation,
        })?;
        check(&answer.status)
    }

    pub fn schemas(&mut self) -> Result<TOperationHandle> {
        let answer = self.client.get_schemas(TGetSchemasReq {
            session_handle: self.handle.clone(),
            catalog_name: None,
            schema_name: None,
        })?;
        check(&answer.status)?;
        answer
            .operation_handle
            .ok_or_else(|| Error::server("the server started no operation"))
    }

    pub fn tables(&mut self, schema: &str, table: Option<&str>) -> Result<TOperationHandle> {
        let answer = self.client.get_tables(TGetTablesReq {
            session_handle: self.handle.clone(),
            catalog_name: None,
            schema_name: Some(schema.to_string()),
            table_name: Some(table.unwrap_or("%").to_string()),
            table_types: None,
        })?;
        check(&answer.status)?;
        answer
            .operation_handle
            .ok_or_else(|| Error::server("the server started no operation"))
    }

    pub fn columns(&mut self, schema: &str, table: Option<&str>) -> Result<TOperationHandle> {
        let answer = self.client.get_columns(TGetColumnsReq {
            session_handle: self.handle.clone(),
            catalog_name: None,
            schema_name: Some(schema.to_string()),
            table_name: Some(table.unwrap_or("%").to_string()),
            column_name: Some("%".to_string()),
        })?;
        check(&answer.status)?;
        answer
            .operation_handle
            .ok_or_else(|| Error::server("the server started no operation"))
    }

    /// One fact the server tells of itself, as text.
    pub fn info(&mut self, kind: TGetInfoType) -> Result<String> {
        let answer = self.client.get_info(TGetInfoReq {
            session_handle: self.handle.clone(),
            info_type: kind,
        })?;
        check(&answer.status)?;
        Ok(match answer.info_value {
            TGetInfoValue::StringValue(text) => text,
            TGetInfoValue::SmallIntValue(n) => n.to_string(),
            TGetInfoValue::IntegerBitmask(n) | TGetInfoValue::IntegerFlag(n) => n.to_string(),
            TGetInfoValue::BinaryValue(n) => n.to_string(),
            TGetInfoValue::LenValue(n) => n.to_string(),
        })
    }

    pub fn close(mut self) {
        let _ = self.client.close_session(TCloseSessionReq {
            session_handle: self.handle.clone(),
        });
    }
}

/// A call's status: anything but success is the server refusing it.
fn check(status: &TStatus) -> Result<()> {
    match status.status_code {
        TStatusCode::SUCCESS_STATUS
        | TStatusCode::SUCCESS_WITH_INFO_STATUS
        | TStatusCode::STILL_EXECUTING_STATUS => Ok(()),
        _ => Err(Error::Server {
            sql_state: status.sql_state.clone().filter(|s| !s.is_empty()),
            message: status
                .error_message
                .clone()
                .filter(|m| !m.is_empty())
                .unwrap_or_else(|| {
                    format!("the server refused the call ({:?})", status.status_code)
                }),
        }),
    }
}

fn connect(addrs: &[SocketAddr], timeout: Duration) -> Result<TcpStream> {
    let mut last = None;
    for addr in addrs {
        match TcpStream::connect_timeout(addr, timeout) {
            Ok(stream) => {
                let _ = stream.set_nodelay(true);
                stream
                    .set_read_timeout(Some(timeout))
                    .and_then(|()| stream.set_write_timeout(Some(timeout)))
                    .map_err(|e| Error::Lost(e.to_string()))?;
                return Ok(stream);
            }
            Err(e) => last = Some(e),
        }
    }
    Err(Error::Lost(match last {
        Some(e) => e.to_string(),
        None => "the host has no address".into(),
    }))
}

fn plain(addrs: &[SocketAddr], timeout: Duration) -> Result<Box<dyn Stream>> {
    let stream = connect(addrs, timeout)?;
    after_login(&stream);
    Ok(Box::new(stream))
}

fn tls(
    addrs: &[SocketAddr],
    target: &Target,
    config: Arc<ClientConfig>,
) -> Result<Box<dyn Stream>> {
    let socket = connect(addrs, target.connect_timeout)?;
    // The name checked is the server's own, even through a tunnel (the socket is the tunnel's).
    let name = ServerName::try_from(target.host.clone())
        .map_err(|e| Error::Lost(format!("TLS: {}: {e}", target.host)))?;
    let connection =
        ClientConnection::new(config, name).map_err(|e| Error::Lost(format!("TLS: {e}")))?;
    let mut stream = StreamOwned::new(connection, socket);
    while stream.conn.is_handshaking() {
        stream
            .conn
            .complete_io(&mut stream.sock)
            .map_err(|e| Error::Lost(format!("TLS: {e}")))?;
    }
    after_login(&stream.sock);
    Ok(Box::new(stream))
}

/// Past the login, a call may take longer than connecting may.
fn after_login(stream: &TcpStream) {
    let _ = stream.set_read_timeout(Some(CALL_TIMEOUT));
    let _ = stream.set_write_timeout(Some(CALL_TIMEOUT));
}

/// SASL's `PLAIN` exchange, as Thrift's SASL transport frames it: a status byte, a length and a
/// payload. The server answers `COMPLETE` when it accepts the login.
fn sasl_plain(stream: &mut Shared, user: &str, password: &str) -> Result<()> {
    const START: u8 = 1;
    const OK: u8 = 2;
    const COMPLETE: u8 = 5;
    let send = |stream: &mut Shared, status: u8, payload: &[u8]| -> Result<()> {
        let mut frame = Vec::with_capacity(payload.len() + 5);
        frame.push(status);
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(payload);
        stream
            .write_all(&frame)
            .and_then(|()| stream.flush())
            .map_err(|e| Error::Lost(e.to_string()))
    };
    send(stream, START, b"PLAIN")?;
    let mut credentials = Vec::with_capacity(user.len() + password.len() + 2);
    credentials.push(0);
    credentials.extend_from_slice(user.as_bytes());
    credentials.push(0);
    credentials.extend_from_slice(password.as_bytes());
    send(stream, OK, &credentials)?;
    let mut head = [0u8; 5];
    stream.read_exact(&mut head).map_err(|e| {
        Error::Lost(format!(
            "the server did not answer the login (is it HiveServer2, with SASL?): {e}"
        ))
    })?;
    let length = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
    // A refusal's message is short; anything longer is not this protocol.
    if length > 64 * 1024 {
        return Err(Error::Lost(
            "the server's answer to the login is not SASL".into(),
        ));
    }
    let mut body = vec![0u8; length];
    stream
        .read_exact(&mut body)
        .map_err(|e| Error::Lost(e.to_string()))?;
    if head[0] == COMPLETE {
        return Ok(());
    }
    let message = String::from_utf8_lossy(&body).trim().to_string();
    Err(Error::Server {
        sql_state: None,
        message: if message.is_empty() {
            "the server refused the login".into()
        } else {
            format!("the server refused the login: {message}")
        },
    })
}
