// Runs the Codex App Server and its code-mode host inside an iOS app
// (codex-rs/ios-kit). The App Server listens on the Unix socket the options
// name; the app exchanges Codex's JSON-RPC messages with it through
// codex_ios_kit_connect and codex_ios_kit_send.
#ifndef CODEX_IOS_KIT_H
#define CODEX_IOS_KIT_H

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/// Starts Codex once per process from JSON options
/// `{"codexHome": PATH, "socketPath": PATH, "configOverrides": ["key=value", ...]}`
/// and answers with JSON `{"ok": true, "socketPath": ...}` or
/// `{"ok": false, "error": ...}`, freed with codex_ios_kit_free.
char *codex_ios_kit_start(const char *options);

/// Frees an answer of codex_ios_kit_start.
void codex_ios_kit_free(char *text);

/// A connection's messages: CODEX_IOS_KIT_MESSAGE with one JSON-RPC message
/// from the App Server, then CODEX_IOS_KIT_CLOSED exactly once with the
/// reason. Called on the kit's threads; `text` lives only during the call.
#define CODEX_IOS_KIT_MESSAGE 0
#define CODEX_IOS_KIT_CLOSED 1
typedef void (*codex_ios_kit_callback)(void *context, int32_t kind, const char *text);

/// Opens a connection to the started App Server and returns its id, or 0
/// when Codex has not started. `context` must stay valid until CLOSED.
uint64_t codex_ios_kit_connect(codex_ios_kit_callback callback, void *context);

/// Queues one JSON-RPC message; false once the connection has closed.
bool codex_ios_kit_send(uint64_t connection, const char *message);

/// Closes a connection; its CLOSED follows unless it had closed already.
void codex_ios_kit_disconnect(uint64_t connection);

#ifdef __cplusplus
}
#endif

#endif
