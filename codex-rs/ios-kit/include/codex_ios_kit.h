// Runs the Codex App Server and its code-mode host inside an iOS app
// (codex-rs/ios-kit). The App Server listens on the Unix socket the options
// name and speaks Codex's JSON-RPC in WebSocket frames over it.
#ifndef CODEX_IOS_KIT_H
#define CODEX_IOS_KIT_H

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

#ifdef __cplusplus
}
#endif

#endif
