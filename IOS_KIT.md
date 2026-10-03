# Codex inside an iOS app

This repository is [OpenAI Codex](https://github.com/openai/codex) at tag
`rust-v0.160.0` (Apache-2.0) with the changes an iOS app needs to run the
Codex App Server inside itself. Supervisor's iOS app uses it; the app itself
is not part of this repository.

## Why

iOS lets an app start no other program, so the `codex app-server` executable
and the `codex-code-mode-host` it starts cannot run on an iPhone.
`codex-rs/ios-kit` runs both as tasks of the app instead:

- the code-mode host, which runs the current models' tool calls as JavaScript
  in V8, on a loopback gRPC port that only the App Server is told about;
- the App Server on a Unix socket in the app's container, speaking the same
  JSON-RPC in WebSocket frames as `codex app-server --listen unix://PATH`.

Commands still cannot run: every attempt to start a program fails on iOS as
it does for any app.

## Changes from upstream

1. V8 152.2.0 without its sandbox. rusty_v8 publishes V8 for
   `aarch64-apple-ios` and its simulator from 152.x, built without pointer
   compression or the V8 sandbox; upstream pins 150.4.0 with the sandbox,
   which only its own Bazel mirror builds. V8 152 carries ICU 78, so the ICU
   data moves to `deno_core_icudata` 0.78.0.
2. `codex-rs/ios-kit`: the C API in `include/codex_ios_kit.h`
   (`codex_ios_kit_start`, `codex_ios_kit_free`) and
   `codex-ios-kit-probe`, which runs the kit on Linux with `execve` refused by
   seccomp, as iOS refuses it, and talks to the App Server over its socket.
3. Upstream's `.github` (CI, release and dependency automation) is removed;
   `.github/workflows/ios-kit.yml` builds `CodexKit.xcframework` for iPhone
   and the arm64 simulator on a macOS runner.

## Build

```sh
cd codex-rs
cargo build --release -p codex-ios-kit --lib --target aarch64-apple-ios
cargo run -p codex-ios-kit --bin codex-ios-kit-probe -- --home /tmp/kit --no-exec
```

## License

Apache-2.0, as upstream: see `LICENSE` and `NOTICE`.
