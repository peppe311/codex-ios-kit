//! Exercises the kit on Linux as iOS would run it: with `--no-exec` the
//! process may start no other program (seccomp refuses execve), Codex runs as
//! threads of this process, and a JSON-RPC client talks to the App Server over
//! its Unix socket.
//!
//! codex-ios-kit-probe --home DIR [--no-exec] [--login] [--prompt TEXT]
//!     [--model ID] [--effort LEVEL] [--cwd DIR]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use codex_ios_kit::StartOptions;
use futures::SinkExt;
use futures::StreamExt;
use serde_json::Value;
use serde_json::json;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::Message;

#[derive(Default)]
struct Args {
    home: Option<PathBuf>,
    no_exec: bool,
    login: bool,
    prompt: Option<String>,
    model: Option<String>,
    effort: Option<String>,
    cwd: Option<PathBuf>,
}

fn parse_args() -> Result<Args> {
    let mut args = Args::default();
    let mut values = std::env::args().skip(1);
    while let Some(flag) = values.next() {
        let mut value = || values.next().ok_or_else(|| anyhow!("{flag} needs a value"));
        match flag.as_str() {
            "--home" => args.home = Some(PathBuf::from(value()?)),
            "--no-exec" => args.no_exec = true,
            "--login" => args.login = true,
            "--prompt" => args.prompt = Some(value()?),
            "--model" => args.model = Some(value()?),
            "--effort" => args.effort = Some(value()?),
            "--cwd" => args.cwd = Some(PathBuf::from(value()?)),
            other => bail!("unknown argument {other}"),
        }
    }
    Ok(args)
}

fn main() -> Result<()> {
    let args = parse_args()?;
    if args.no_exec {
        forbid_exec()?;
        // The refusal is the point: prove it before Codex starts.
        match std::process::Command::new("/bin/true").status() {
            Err(error) => println!("probe: starting programs is refused ({error})"),
            Ok(_) => bail!("seccomp did not refuse execve"),
        }
    }
    let home = args.home.clone().context("--home is required")?;
    let home = std::fs::canonicalize(&home).or_else(|_| {
        std::fs::create_dir_all(&home)?;
        std::fs::canonicalize(&home)
    })?;
    let started = codex_ios_kit::start(StartOptions {
        codex_home: home.join("codex"),
        socket_path: home.join("app-server.sock"),
        config_overrides: vec![
            format!("sqlite_home={}", toml_string(&home.join("codex"))),
            "cli_auth_credentials_store=\"file\"".into(),
            "features.code_mode_host=true".into(),
            "features.apps=false".into(),
            "features.plugins=false".into(),
        ],
    })?;
    println!(
        "probe: App Server on {} with the code-mode host on {}",
        started.socket_path.display(),
        started.code_mode_host
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(session(started.socket_path, args, home))
}

fn toml_string(path: &std::path::Path) -> String {
    serde_json::to_string(&path.display().to_string()).unwrap_or_default()
}

/// A JSON-RPC client of the App Server over its socket, as the iOS app's.
struct Client {
    outgoing: mpsc::UnboundedSender<Message>,
    pending: Arc<Mutex<HashMap<i64, oneshot::Sender<Value>>>>,
    next_id: AtomicI64,
}

impl Client {
    async fn request(&self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, tx);
        self.outgoing
            .send(Message::Text(
                json!({"id": id, "method": method, "params": params})
                    .to_string()
                    .into(),
            ))
            .map_err(|_| anyhow!("the connection is closed"))?;
        let answer = tokio::time::timeout(Duration::from_secs(120), rx)
            .await
            .map_err(|_| anyhow!("{method} got no answer"))?
            .map_err(|_| anyhow!("{method}: the connection closed"))?;
        if let Some(error) = answer.get("error") {
            bail!("{method} failed: {error}");
        }
        Ok(answer.get("result").cloned().unwrap_or(Value::Null))
    }

    fn notify(&self, method: &str, params: Option<Value>) -> Result<()> {
        let mut message = json!({"method": method});
        if let Some(params) = params {
            message["params"] = params;
        }
        self.outgoing
            .send(Message::Text(message.to_string().into()))
            .map_err(|_| anyhow!("the connection is closed"))
    }
}

async fn session(socket: PathBuf, args: Args, home: PathBuf) -> Result<()> {
    let stream = tokio::net::UnixStream::connect(&socket).await?;
    let (websocket, _) = tokio_tungstenite::client_async("ws://localhost/rpc", stream).await?;
    let (mut sink, mut source) = websocket.split();
    let (outgoing, mut outgoing_rx) = mpsc::unbounded_channel::<Message>();
    let pending: Arc<Mutex<HashMap<i64, oneshot::Sender<Value>>>> = Arc::default();
    let (events_tx, mut events) = mpsc::unbounded_channel::<Value>();
    tokio::spawn(async move {
        while let Some(message) = outgoing_rx.recv().await {
            if sink.send(message).await.is_err() {
                break;
            }
        }
    });
    let reader_pending = Arc::clone(&pending);
    let replies = outgoing.clone();
    tokio::spawn(async move {
        while let Some(Ok(message)) = source.next().await {
            let Message::Text(text) = message else {
                continue;
            };
            let Ok(value) = serde_json::from_str::<Value>(&text) else {
                continue;
            };
            let has_method = value.get("method").is_some();
            match (value.get("id").and_then(Value::as_i64), has_method) {
                (Some(id), false) => {
                    if let Some(tx) = reader_pending.lock().await.remove(&id) {
                        let _ = tx.send(value);
                    }
                }
                (_, true) if value.get("id").is_some() => {
                    // A server request: approvals are accepted, anything else declined.
                    let method = value["method"].as_str().unwrap_or_default().to_owned();
                    println!("probe: server request {method}: {}", value["params"]);
                    let result = if method.ends_with("requestApproval") {
                        json!({"decision": "accept"})
                    } else {
                        json!({})
                    };
                    let _ = replies.send(Message::Text(
                        json!({"id": value["id"], "result": result})
                            .to_string()
                            .into(),
                    ));
                }
                _ => {
                    let _ = events_tx.send(value);
                }
            }
        }
        println!("probe: the App Server closed the connection");
    });
    let client = Client {
        outgoing,
        pending,
        next_id: AtomicI64::new(1),
    };

    let initialized = client
        .request(
            "initialize",
            json!({"clientInfo": {"name": "supervisor-ios-probe", "title": "Supervisor iOS probe", "version": "0.1.0"},
                   "capabilities": {"experimentalApi": true}}),
        )
        .await?;
    println!("probe: initialize → {initialized}");
    client.notify("initialized", None)?;

    let account = client
        .request("account/read", json!({"refreshToken": false}))
        .await?;
    println!("probe: account/read → {account}");
    if args.login {
        let started = client
            .request("account/login/start", json!({"type": "chatgptDeviceCode"}))
            .await?;
        println!("probe: SIGN-IN {started}");
        let completed = wait_for(
            &mut events,
            "account/login/completed",
            Duration::from_secs(900),
        )
        .await?;
        println!("probe: account/login/completed → {}", completed["params"]);
    }

    let models = client.request("model/list", json!({})).await?;
    for model in models["data"].as_array().into_iter().flatten() {
        println!(
            "probe: model {} ({}) efforts {:?}",
            model["id"].as_str().unwrap_or_default(),
            model["displayName"].as_str().unwrap_or_default(),
            model["supportedReasoningEfforts"]
                .as_array()
                .map(|efforts| efforts
                    .iter()
                    .filter_map(|e| e["reasoningEffort"].as_str())
                    .collect::<Vec<_>>())
        );
    }

    let Some(prompt) = args.prompt else {
        return Ok(());
    };
    let cwd = args.cwd.unwrap_or_else(|| home.join("project"));
    std::fs::create_dir_all(&cwd)?;
    let mut thread_params =
        json!({"cwd": cwd, "approvalPolicy": "never", "sandbox": "danger-full-access"});
    if let Some(model) = &args.model {
        thread_params["model"] = json!(model);
    }
    let thread = client.request("thread/start", thread_params).await?;
    let thread_id = thread["thread"]["id"]
        .as_str()
        .context("thread/start gave no thread id")?
        .to_owned();
    println!("probe: thread {thread_id} in {}", cwd.display());
    let mut turn_params =
        json!({"threadId": thread_id, "input": [{"type": "text", "text": prompt}]});
    if let Some(effort) = &args.effort {
        turn_params["effort"] = json!(effort);
    }
    let turn = client.request("turn/start", turn_params).await?;
    println!("probe: turn/start → {}", turn["turn"]["id"]);
    loop {
        let event = tokio::time::timeout(Duration::from_secs(600), events.recv())
            .await
            .map_err(|_| anyhow!("no event for 10 minutes"))?
            .context("the connection closed during the turn")?;
        let method = event["method"].as_str().unwrap_or_default();
        match method {
            "item/agentMessage/delta" => {
                print!("{}", event["params"]["delta"].as_str().unwrap_or_default());
                use std::io::Write;
                let _ = std::io::stdout().flush();
            }
            "item/started" | "item/completed" => {
                let item = &event["params"]["item"];
                let kind = item["type"].as_str().unwrap_or_default();
                if kind != "agentMessage" && kind != "userMessage" && kind != "reasoning" {
                    println!("\nprobe: {method} {kind}: {}", short(item));
                }
            }
            "error" => println!("\nprobe: error {}", event["params"]),
            "turn/completed" => {
                println!(
                    "\nprobe: turn/completed {}",
                    short(&event["params"]["turn"])
                );
                break;
            }
            _ => {}
        }
    }
    for entry in std::fs::read_dir(&cwd)? {
        let entry = entry?;
        println!(
            "probe: file {} ({} bytes)",
            entry.path().display(),
            entry.metadata()?.len()
        );
    }
    Ok(())
}

fn short(value: &Value) -> String {
    let text = value.to_string();
    if text.chars().count() > 600 {
        format!("{}…", text.chars().take(600).collect::<String>())
    } else {
        text
    }
}

async fn wait_for(
    events: &mut mpsc::UnboundedReceiver<Value>,
    method: &str,
    limit: Duration,
) -> Result<Value> {
    tokio::time::timeout(limit, async {
        while let Some(event) = events.recv().await {
            if event["method"] == method {
                return Ok(event);
            }
        }
        Err(anyhow!("the connection closed before {method}"))
    })
    .await
    .map_err(|_| anyhow!("{method} did not arrive in time"))?
}

/// Refuses execve and execveat to every thread started from now on, as iOS
/// refuses an app's attempts to start programs.
#[cfg(target_os = "linux")]
fn forbid_exec() -> Result<()> {
    const LOAD_SYSCALL_NUMBER: u16 = 0x20; // BPF_LD | BPF_W | BPF_ABS
    const JUMP_IF_EQUAL: u16 = 0x15; // BPF_JMP | BPF_JEQ | BPF_K
    const RETURN: u16 = 0x06; // BPF_RET | BPF_K
    const ALLOW: u32 = 0x7fff_0000;
    let refuse = 0x0005_0000 | libc::EPERM as u32;
    let filter = [
        libc::sock_filter {
            code: LOAD_SYSCALL_NUMBER,
            jt: 0,
            jf: 0,
            k: 0,
        },
        libc::sock_filter {
            code: JUMP_IF_EQUAL,
            jt: 0,
            jf: 1,
            k: libc::SYS_execve as u32,
        },
        libc::sock_filter {
            code: RETURN,
            jt: 0,
            jf: 0,
            k: refuse,
        },
        libc::sock_filter {
            code: JUMP_IF_EQUAL,
            jt: 0,
            jf: 1,
            k: libc::SYS_execveat as u32,
        },
        libc::sock_filter {
            code: RETURN,
            jt: 0,
            jf: 0,
            k: refuse,
        },
        libc::sock_filter {
            code: RETURN,
            jt: 0,
            jf: 0,
            k: ALLOW,
        },
    ];
    let program = libc::sock_fprog {
        len: filter.len() as u16,
        filter: filter.as_ptr() as *mut libc::sock_filter,
    };
    // SAFETY: plain prctl calls with a filter that outlives them.
    unsafe {
        if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
            bail!(
                "PR_SET_NO_NEW_PRIVS failed: {}",
                std::io::Error::last_os_error()
            );
        }
        if libc::prctl(
            libc::PR_SET_SECCOMP,
            libc::SECCOMP_MODE_FILTER,
            &program as *const libc::sock_fprog,
        ) != 0
        {
            bail!("seccomp failed: {}", std::io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn forbid_exec() -> Result<()> {
    bail!("--no-exec works on Linux only")
}
