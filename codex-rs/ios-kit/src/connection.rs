//! Connections of the host app to the App Server over its socket, so the app
//! exchanges JSON-RPC messages through plain C calls instead of speaking
//! WebSocket over a Unix socket itself.

use std::collections::HashMap;
use std::ffi::CStr;
use std::ffi::CString;
use std::ffi::c_char;
use std::ffi::c_void;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use futures::SinkExt;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

/// What the kit tells the app about a connection: `MESSAGE` with one JSON-RPC
/// message from the App Server, then `CLOSED` exactly once with the reason.
pub type MessageCallback =
    unsafe extern "C" fn(context: *mut c_void, kind: i32, text: *const c_char);
pub const MESSAGE: i32 = 0;
pub const CLOSED: i32 = 1;

struct Connection {
    outgoing: mpsc::UnboundedSender<String>,
    task: tokio::task::AbortHandle,
}

static CONNECTIONS: Mutex<Option<HashMap<u64, Connection>>> = Mutex::new(None);
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// The app's callback and its context, which the app keeps valid until the
/// connection's `CLOSED`; the kit calls it from its own threads.
#[derive(Clone, Copy)]
struct Target {
    callback: MessageCallback,
    context: usize,
}

// SAFETY: the app promises the callback may be called from any thread and its
// context stays valid until CLOSED; the kit only passes the context back.
unsafe impl Send for Target {}

impl Target {
    fn deliver(&self, kind: i32, text: &str) {
        // A message with a NUL byte cannot cross the C boundary; JSON never has one.
        let Ok(text) = CString::new(text) else { return };
        // SAFETY: see Target.
        unsafe { (self.callback)(self.context as *mut c_void, kind, text.as_ptr()) };
    }
}

/// Opens a connection to the started App Server and returns its id, or 0 when
/// Codex has not started. Messages sent before it is open wait for it.
pub fn connect(callback: MessageCallback, context: *mut c_void) -> u64 {
    let Some((runtime, socket)) = crate::runtime() else {
        return 0;
    };
    let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
    let target = Target {
        callback,
        context: context as usize,
    };
    let (outgoing, incoming) = mpsc::unbounded_channel::<String>();
    let task = runtime.spawn(async move {
        let reason = match run(socket, incoming, target).await {
            Ok(()) => "Codex closed the connection.".to_owned(),
            Err(error) => format!("The connection to Codex ended: {error:#}"),
        };
        forget(id);
        target.deliver(CLOSED, &reason);
    });
    let mut connections = CONNECTIONS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    connections.get_or_insert_with(HashMap::new).insert(
        id,
        Connection {
            outgoing,
            task: task.abort_handle(),
        },
    );
    id
}

/// Queues one JSON-RPC message for the App Server; false once the connection
/// has closed.
pub fn send(id: u64, message: String) -> bool {
    let connections = CONNECTIONS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    connections
        .as_ref()
        .and_then(|map| map.get(&id))
        .is_some_and(|connection| connection.outgoing.send(message).is_ok())
}

/// Closes a connection; its `CLOSED` follows unless it had closed already.
pub fn disconnect(id: u64) {
    let connection = forget(id);
    if let Some(connection) = connection {
        // Dropping the sender ends the writer, which ends the connection.
        drop(connection.outgoing);
        // A connection still opening has no writer to end yet.
        let task = connection.task;
        if let Some((runtime, _)) = crate::runtime() {
            runtime.spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                task.abort();
            });
        }
    }
}

fn forget(id: u64) -> Option<Connection> {
    let mut connections = CONNECTIONS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    connections.as_mut().and_then(|map| map.remove(&id))
}

async fn run(
    socket: PathBuf,
    mut incoming: mpsc::UnboundedReceiver<String>,
    target: Target,
) -> anyhow::Result<()> {
    let stream = tokio::net::UnixStream::connect(&socket).await?;
    let (websocket, _) = tokio_tungstenite::client_async("ws://localhost/rpc", stream).await?;
    let (mut sink, mut source) = websocket.split();
    loop {
        tokio::select! {
            message = incoming.recv() => match message {
                Some(text) => sink.send(Message::Text(text.into())).await?,
                None => {
                    let _ = sink.send(Message::Close(None)).await;
                    return Ok(());
                }
            },
            frame = source.next() => match frame {
                Some(Ok(Message::Text(text))) => target.deliver(MESSAGE, &text),
                Some(Ok(Message::Close(_))) | None => return Ok(()),
                Some(Ok(_)) => {}
                Some(Err(error)) => return Err(error.into()),
            },
        }
    }
}

/// See [`connect`].
///
/// # Safety
/// `callback` must accept calls from any thread until it receives `CLOSED`,
/// and `context` must stay valid until then.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn codex_ios_kit_connect(
    callback: MessageCallback,
    context: *mut c_void,
) -> u64 {
    connect(callback, context)
}

/// See [`send`].
///
/// # Safety
/// `message` must be a valid NUL-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn codex_ios_kit_send(connection: u64, message: *const c_char) -> bool {
    if message.is_null() {
        return false;
    }
    // SAFETY: the caller passes a valid NUL-terminated string.
    match unsafe { CStr::from_ptr(message) }.to_str() {
        Ok(text) => send(connection, text.to_owned()),
        Err(_) => false,
    }
}

/// See [`disconnect`].
#[unsafe(no_mangle)]
pub extern "C" fn codex_ios_kit_disconnect(connection: u64) {
    disconnect(connection);
}
