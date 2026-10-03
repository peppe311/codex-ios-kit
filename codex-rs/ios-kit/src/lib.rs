//! The Codex App Server inside an iOS app.
//!
//! iOS lets an app start no other program, so the App Server and the
//! code-mode host, which runs the current models' tool calls as JavaScript,
//! run as tasks of the app itself. The App Server listens on a Unix socket in
//! the app's own container, which no other app can open, and the app speaks
//! the same JSON-RPC over it (WebSocket frames, as `codex app-server
//! --listen unix://PATH` does) as Supervisor's desktop and Android apps speak
//! with the separate program. The code-mode host listens on a loopback port
//! that only the App Server is told about.
// The App Server's request handling is one deep async fn; its layout needs the
// recursion limit codex-app-server's own executable sets.
#![recursion_limit = "256"]

pub mod connection;

use std::ffi::CStr;
use std::ffi::CString;
use std::ffi::c_char;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::net::TcpListener as StdTcpListener;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::mpsc;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use codex_app_server::AppServerRuntimeOptions;
use codex_app_server::AppServerTransport;
use codex_app_server::CodeModeHostTransport;
use codex_app_server::PluginStartupTasks;
use codex_app_server::RemoteControlStartupMode;
use codex_app_server::run_main_with_transport_options;
use codex_arg0::Arg0DispatchPaths;
use codex_async_utils::THREAD_STACK_SIZE_BYTES;
use codex_config::LoaderOverrides;
use codex_protocol::protocol::SessionSource;
use codex_utils_absolute_path::AbsolutePathBuf;
use codex_utils_cli::CliConfigOverrides;
use codex_websocket_auth::WebsocketAuthSettings;
use serde::Deserialize;
use serde_json::json;

/// How long the App Server may take to listen on its socket.
const START_TIMEOUT: Duration = Duration::from_secs(30);

/// What the host app tells the kit when it starts Codex.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StartOptions {
    /// Codex's home: sign-in, threads and the SQLite state.
    pub codex_home: PathBuf,
    /// The Unix socket the App Server listens on. Unix socket paths are
    /// short (104 bytes on iOS), so it belongs in a short directory.
    pub socket_path: PathBuf,
    /// `key=value` overrides, as `codex app-server -c` takes them.
    #[serde(default)]
    pub config_overrides: Vec<String>,
}

/// Where a started App Server listens.
#[derive(Debug, Clone)]
pub struct Started {
    pub socket_path: PathBuf,
    pub code_mode_host: String,
}

/// The process runs one App Server: its environment, tracing and runtime are
/// process-wide.
static STARTED: Mutex<Option<Started>> = Mutex::new(None);

/// The runtime Codex runs on and its socket, for the app's connections.
static RUNTIME: OnceLock<(tokio::runtime::Handle, PathBuf)> = OnceLock::new();

pub(crate) fn runtime() -> Option<(tokio::runtime::Handle, PathBuf)> {
    RUNTIME.get().cloned()
}

/// Starts the code-mode host and the App Server on a thread of their own and
/// returns once the App Server listens on its socket.
pub fn start(options: StartOptions) -> Result<Started> {
    let mut started = STARTED
        .lock()
        .map_err(|_| anyhow!("the Codex kit's state is poisoned"))?;
    if let Some(started) = started.as_ref() {
        return Ok(started.clone());
    }
    std::fs::create_dir_all(&options.codex_home)
        .with_context(|| format!("creating {}", options.codex_home.display()))?;
    if let Some(parent) = options.socket_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // A socket left by an earlier run of the app would refuse the bind.
    match std::fs::remove_file(&options.socket_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("removing the previous App Server socket"),
    }
    let socket_path = AbsolutePathBuf::from_absolute_path(&options.socket_path)
        .context("the App Server socket path must be absolute")?;
    // SAFETY: the kit sets CODEX_HOME once, before any of its threads start;
    // the host app does not read or change the environment meanwhile.
    unsafe { std::env::set_var("CODEX_HOME", &options.codex_home) };

    let host_port = free_loopback_port()?;
    let host_url = format!("grpc://127.0.0.1:{host_port}");
    let host_transport = CodeModeHostTransport::Grpc(url::Url::parse(&host_url)?);
    let overrides = options.config_overrides.clone();
    let connections_socket = options.socket_path.clone();
    let (failed_tx, failed_rx) = mpsc::channel::<String>();
    // Codex's own threads have 16 MiB stacks; its deepest futures overflow the
    // default 2 MiB of a Tokio worker.
    std::thread::Builder::new()
        .name("codex-app-server".into())
        .stack_size(THREAD_STACK_SIZE_BYTES)
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name("codex")
                .thread_stack_size(THREAD_STACK_SIZE_BYTES)
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = failed_tx.send(format!("Codex could not start its runtime: {error}"));
                    return;
                }
            };
            let _ = RUNTIME.set((runtime.handle().clone(), connections_socket));
            let outcome = runtime.block_on(async move {
                let host_listen = host_url.clone();
                tokio::spawn(async move {
                    if let Err(error) = codex_code_mode_host::run_main(&host_listen).await {
                        eprintln!("codex code-mode host stopped: {error:#}");
                    }
                });
                wait_for_port(host_port).await?;
                let runtime_options = AppServerRuntimeOptions {
                    code_mode_host_transport: host_transport,
                    plugin_startup_tasks: PluginStartupTasks::Start,
                    remote_control_startup_mode: RemoteControlStartupMode::DisabledEphemeral,
                    install_shutdown_signal_handler: false,
                    managed_daemon: false,
                };
                // Codex re-runs its own executable for some commands; inside an app
                // that is the app, and on iOS such starts are refused like any other.
                let arg0_paths = Arg0DispatchPaths {
                    codex_self_exe: std::env::current_exe().ok(),
                    ..Arg0DispatchPaths::default()
                };
                run_main_with_transport_options(
                    arg0_paths,
                    CliConfigOverrides {
                        raw_overrides: overrides,
                    },
                    LoaderOverrides::default(),
                    /*strict_config*/ false,
                    /*default_analytics_enabled*/ false,
                    AppServerTransport::UnixSocket { socket_path },
                    SessionSource::default(),
                    WebsocketAuthSettings::default(),
                    runtime_options,
                )
                .await
                .map(|_| ())
                .map_err(anyhow::Error::from)
            });
            let message = match outcome {
                Ok(()) => "The Codex App Server stopped.".to_owned(),
                Err(error) => format!("The Codex App Server stopped: {error:#}"),
            };
            let _ = failed_tx.send(message);
        })
        .context("starting the Codex thread")?;

    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        if std::fs::metadata(&options.socket_path).is_ok() {
            let result = Started {
                socket_path: options.socket_path.clone(),
                code_mode_host: format!("grpc://127.0.0.1:{host_port}"),
            };
            *started = Some(result.clone());
            return Ok(result);
        }
        match failed_rx.try_recv() {
            Ok(message) => return Err(anyhow!(message)),
            Err(mpsc::TryRecvError::Disconnected) => {
                return Err(anyhow!("The Codex App Server stopped while starting."));
            }
            Err(mpsc::TryRecvError::Empty) => {}
        }
        if Instant::now() > deadline {
            return Err(anyhow!(
                "The Codex App Server did not start within 30 seconds."
            ));
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// A loopback port nothing listens on yet, for the code-mode host.
fn free_loopback_port() -> Result<u16> {
    let listener = StdTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .context("reserving a loopback port for the code-mode host")?;
    Ok(listener.local_addr()?.port())
}

async fn wait_for_port(port: u16) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if tokio::net::TcpStream::connect((Ipv4Addr::LOCALHOST, port))
            .await
            .is_ok()
        {
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err(anyhow!(
                "the code-mode host did not listen within 10 seconds"
            ));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Starts Codex from a JSON [`StartOptions`] and answers with JSON:
/// `{"ok":true,"socketPath":…}` or `{"ok":false,"error":…}`. The caller frees
/// the answer with [`codex_ios_kit_free`].
///
/// # Safety
/// `options` must be a valid NUL-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn codex_ios_kit_start(options: *const c_char) -> *mut c_char {
    let answer = (|| -> Result<serde_json::Value> {
        if options.is_null() {
            return Err(anyhow!("no start options"));
        }
        // SAFETY: the caller passes a valid NUL-terminated string.
        let text = unsafe { CStr::from_ptr(options) }.to_str()?;
        let options: StartOptions = serde_json::from_str(text)?;
        let started = start(options)?;
        Ok(json!({"ok": true, "socketPath": started.socket_path, "codeModeHost": started.code_mode_host}))
    })()
    .unwrap_or_else(|error| json!({"ok": false, "error": format!("{error:#}")}));
    CString::new(answer.to_string())
        .map(CString::into_raw)
        .unwrap_or(std::ptr::null_mut())
}

/// Frees an answer of [`codex_ios_kit_start`].
///
/// # Safety
/// `text` must come from [`codex_ios_kit_start`] and be freed once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn codex_ios_kit_free(text: *mut c_char) {
    if !text.is_null() {
        // SAFETY: the pointer came from CString::into_raw in this crate.
        drop(unsafe { CString::from_raw(text) });
    }
}
