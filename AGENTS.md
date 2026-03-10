# AGENTS.md – Best Practices for Robust and Modular Rust

This document captures the conventions and guidelines that all contributors and AI coding agents should follow when working on **shardlake**.

---

## 1. Code Organisation

- **One responsibility per module.** Keep each `mod` focused on a single concern (e.g. `storage`, `query`, `network`). Avoid catch-all `utils` modules.
- **Prefer libraries over binaries for reusable logic.** Place core logic in `lib.rs` (or a `lib/` workspace crate) so it can be unit-tested without a running binary.
- **Flat module hierarchy first.** Only introduce sub-modules when a module exceeds ~300 lines or contains clearly distinct responsibilities.
- **Re-export judiciously.** Use `pub use` at crate root to expose a clean public API, hiding internal module paths.

## 2. Error Handling

- **Never use `unwrap` or `expect` in production code paths.** Use `?` for propagation and return `Result<T, E>`.
- **Define a crate-level error type.** Use [`thiserror`](https://docs.rs/thiserror) to derive `std::error::Error` with descriptive variants.
- **Avoid stringly-typed errors.** `anyhow` is acceptable in binaries/integration tests; prefer typed errors in library code.
- **Document error variants** in doc-comments so callers understand when each variant is returned.

## 3. Ownership and Borrowing

- **Prefer borrowing over cloning** unless ownership transfer is semantically correct.
- **Minimise `Arc`/`Mutex` usage.** Reach for them only when shared, mutable, cross-thread state is genuinely required.
- **Use `Cow<'_, str>` / `Cow<'_, [u8]>`** for data that is usually borrowed but occasionally needs to be owned.
- **Avoid interior mutability (`Cell`, `RefCell`) in concurrent code.** Use proper synchronisation primitives.

## 4. Traits and Generics

- **Favour trait objects (`dyn Trait`) for runtime polymorphism** and generics (`<T: Trait>`) for zero-cost compile-time dispatch.
- **Keep trait definitions narrow.** A trait with one or two methods is easier to implement and mock.
- **Use associated types instead of generic parameters** when there is only one sensible concrete type per implementor.
- **Seal traits** (via a private super-trait) when they are internal implementation details not intended for downstream implementation.

## 5. Concurrency

- **Prefer message-passing over shared state.** Use channels (`std::sync::mpsc`, `crossbeam`, or async channels) to communicate between threads/tasks.
- **Mark all public async functions with `#[must_use]`** where the returned future must not be silently dropped.
- **Choose an async runtime once and document it.** Do not mix `tokio` and `async-std` in the same crate graph.
- **Avoid blocking in async contexts.** Offload CPU-intensive work with `tokio::task::spawn_blocking` or a dedicated thread pool.

## 6. Testing

- **Unit-test every public function.** Place tests in a `#[cfg(test)] mod tests` block in the same file as the code under test.
- **Integration tests go in `tests/`.** Each file in `tests/` is compiled as a separate crate with access only to the public API.
- **Use `#[should_panic(expected = "...")]` sparingly.** Prefer returning `Result` and asserting on the error variant.
- **Employ property-based testing** (`proptest` or `quickcheck`) for functions with non-trivial input spaces.
- **Mock external I/O via trait injection**, not by patching global state.

## 7. Documentation

- **Every public item must have a doc-comment (`///`).** Include at least one sentence describing the purpose, plus an `# Examples` section for non-trivial APIs.
- **Run `cargo doc --no-deps --open` locally** before opening a PR to verify rendered documentation.
- **Keep `README.md` in sync** with the public API surface and usage examples.
- **Keep `docs/` in sync with every user-facing change.** The `docs/` folder contains user-facing documentation organised by topic:
  - `docs/getting-started.md` — installation, quickstart, and end-to-end walkthrough
  - `docs/cli-reference.md` — every CLI subcommand, flag, and its defaults
  - `docs/api-reference.md` — HTTP endpoints, request/response schemas, and error codes
  - `docs/data-formats.md` — input JSONL schema, artifact storage layout, manifest schema, `.sidx` binary format
  - `docs/configuration.md` — `SystemConfig` fields, `nprobe`/`num_shards` tuning guidance, logging
  
  **When adding or modifying a CLI flag, HTTP endpoint, data format, configuration field, or any other user-visible behaviour, update the relevant file(s) in `docs/` in the same PR.** Reviewers should treat a docs-only change as incomplete if the corresponding `docs/` page is not updated.

## 8. Performance

- **Profile before optimising.** Use `cargo flamegraph`, `criterion` benchmarks, or `perf` to locate hot paths.
- **Prefer stack allocation.** Avoid heap allocation in tight loops; use arrays or `SmallVec` where bounded sizes are known.
- **Use `#[inline]` judiciously** on small, frequently-called functions; do not blanket-annotate.

## 9. Safety and `unsafe`

- **Avoid `unsafe` unless absolutely necessary.** If required, isolate it in a dedicated module with a `# Safety` doc-comment on every `unsafe fn` and `unsafe` block.
- **Every `unsafe` block must have a comment** explaining why it is sound.
- **Run `cargo miri test`** on any crate containing `unsafe` code to detect undefined behaviour.

## 10. Dependency Management

- **Pin major versions** in `Cargo.toml` (`dependency = "1"` not `"*"`).
- **Audit new dependencies** with `cargo audit` before merging.
- **Prefer `no_std`-compatible crates** when targeting embedded or WASM environments.
- **Remove unused dependencies** with `cargo machete` or `cargo udeps`.

## 11. Formatting and Linting

- **Always run `cargo fmt`** before committing. The project uses the default `rustfmt` configuration.
- **Zero Clippy warnings policy.** Run `cargo clippy -- -D warnings` in CI.
- **Enable useful Clippy lints** at crate level:

  ```rust
  #![warn(clippy::pedantic, clippy::nursery)]
  ```

## 12. CI / CD Expectations

- All PRs must pass: `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test`, `cargo doc --no-deps`.
- Security scanning via `cargo audit` runs on every push to `main`.
- Benchmarks (`cargo bench`) run on release branches to catch regressions.

---

_These guidelines apply to human contributors and AI coding agents alike. When in doubt, favour clarity and correctness over cleverness._
