# Rust API Design Guidelines

Well-designed APIs are critical for long-term maintainability and usability. Rust APIs should prioritize **clarity, safety, and zero-cost abstraction**.

## API Design Principles

Rust APIs should be:

* **Predictable** — behavior is obvious from types and names.
* **Explicit** — avoid hidden allocations or side effects.
* **Composable** — small pieces that integrate easily.
* **Zero-cost** — abstractions compile away.

APIs should encode invariants using **types rather than runtime checks**.

Example:

Instead of:

```rust
fn create_user(email: String) -> Result<User>
```

Prefer:

```rust
fn create_user(email: Email) -> Result<User>
```

Where `Email` guarantees validation.

---

# Type-Driven Design

Rust encourages encoding domain constraints in the type system.

Benefits:

* invalid states become impossible
* errors move from runtime to compile-time
* APIs become self-documenting

Example:

Instead of:

```rust
struct Order {
    status: String
}
```

Prefer:

```rust
enum OrderStatus {
    Pending,
    Paid,
    Cancelled
}
```

This prevents invalid states.

---

# Builder Pattern

Complex object construction should use the **builder pattern**.

Advantages:

* avoids large constructors
* supports optional parameters
* improves readability

Example:

```rust
struct ConfigBuilder {
    timeout: Option<u64>,
    retries: Option<u32>,
}
```

Usage:

```rust
let config = Config::builder()
    .timeout(5000)
    .retries(3)
    .build();
```

---

# Error Type Architecture

Error handling should follow structured design.

Guidelines:

* use domain-specific error types
* avoid generic error strings
* use `thiserror` for error definitions
* use `anyhow` only at application boundaries

Example:

```rust
#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("file not found")]
    NotFound,
    #[error("permission denied")]
    PermissionDenied,
}
```

---

# Public API Surface

Minimize public exposure.

Rules:

* only expose necessary types
* hide implementation details
* use module boundaries carefully

Example:

```rust
pub mod api;
mod internal;
```

The internal module remains private.

---

# Trait Design

Traits enable polymorphism and composability.

Guidelines:

* keep traits **small and focused**
* avoid large multi-purpose traits
* prefer composition of small traits

Example:

```rust
trait Readable {
    fn read(&self);
}

trait Writable {
    fn write(&self);
}
```

---

# Generics vs Trait Objects

Rust supports both compile-time and runtime polymorphism.

### Prefer generics when:

* performance is critical
* type known at compile time

Example:

```rust
fn process<T: Processor>(p: T)
```

### Use trait objects when:

* heterogeneous collections
* dynamic dispatch needed

Example:

```rust
Box<dyn Processor>
```

---

# Fluent APIs

Fluent interfaces improve readability when chaining operations.

Example:

```rust
query
    .filter("status = active")
    .limit(10)
    .execute();
```

However, avoid overuse if it harms clarity.

---

# Module Organization

Modules should reflect system architecture.

Recommended structure:

```
src/
  api/
  domain/
  services/
  infrastructure/
  utils/
```

Principles:

* domain logic separated from infrastructure
* avoid circular dependencies
* small modules preferred

---

# Encapsulation Patterns

Rust enables strong encapsulation through visibility modifiers.

Visibility levels:

```
pub
pub(crate)
pub(super)
```

Guidelines:

* default to private
* expose minimal interfaces
* internal structures hidden

---

# Configuration Management

Configuration should be structured and type-safe.

Use:

* `serde`
* environment configuration
* typed config structs

Example:

```rust
#[derive(Deserialize)]
struct Config {
    db_url: String,
    max_connections: u32,
}
```

---

# Feature Flags

Rust supports conditional compilation via features.

Used for:

* optional dependencies
* experimental features
* platform-specific code

Example:

```
#[cfg(feature = "metrics")]
```

Guidelines:

* keep features orthogonal
* avoid complex dependency graphs

---

# Dependency Hygiene

Dependencies must remain controlled.

Rules:

* minimize dependency count
* prefer mature crates
* audit regularly

Tools:

```
cargo audit
cargo deny
```

---

# Versioning and Stability

Public libraries should follow **semantic versioning**.

Rules:

* breaking changes increment major version
* additive changes increment minor version
* bug fixes increment patch version

---

# Documentation Standards

Public APIs require documentation comments.

Example:

```rust
/// Reads a configuration file and returns parsed settings.
pub fn load_config(path: &str) -> Result<Config>
```

Documentation must include:

* purpose
* arguments
* return values
* examples when helpful

Generate docs with:

```
cargo doc --open
```

---

# Example-Driven Documentation

Include examples in documentation comments.

Example:

````rust
/// Calculates the average of a slice.
///
/// # Example
/// ```
/// let avg = calculate_average(&[1,2,3]);
/// ```
````

Examples act as executable tests.

---

# Migration and Refactoring Strategy

Refactoring should maintain API stability.

Strategies:

* introduce new APIs alongside old ones
* deprecate gradually
* maintain backward compatibility when possible

Example:

```rust
#[deprecated(note = "use new_api instead")]
```

---

# Summary

High-quality Rust APIs emphasize:

* strong typing
* explicit ownership
* minimal public surface
* composability
* predictable performance

When these principles are followed, Rust codebases remain:

* maintainable
* safe
* scalable
* developer-friendly.

---

