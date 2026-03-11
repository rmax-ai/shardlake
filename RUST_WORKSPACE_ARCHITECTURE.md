# Rust Workspace Architecture

## Purpose

This document defines architectural practices for building **large Rust systems** using Cargo workspaces and multiple crates. It focuses on scalability, compile-time isolation, maintainability, and performance.

These practices are particularly relevant for:

* distributed systems
* infrastructure platforms
* AI/ML infrastructure
* agent orchestration frameworks
* data pipelines
* backend services

The goal is to ensure the system remains maintainable beyond **100k+ lines of code**.

---

# Core Architectural Principles

Large Rust systems should follow these principles:

1. **Separation of concerns**
2. **Layered dependencies**
3. **Minimal crate coupling**
4. **Clear ownership boundaries**
5. **Fast compilation**
6. **Explicit system architecture**

Crate boundaries act as **architectural enforcement mechanisms**.

---

# Workspace Fundamentals

A **Cargo workspace** allows multiple crates to share dependencies, build configuration, and CI pipelines.

Example structure:

```
workspace/
├─ Cargo.toml
├─ crates/
│  ├─ api/
│  ├─ domain/
│  ├─ services/
│  ├─ storage/
│  ├─ runtime/
│  └─ utils/
└─ apps/
   ├─ server/
   └─ cli/
```

The workspace root `Cargo.toml` defines the workspace members.

Example:

```toml
[workspace]
members = [
    "crates/api",
    "crates/domain",
    "crates/services",
    "crates/storage",
    "crates/runtime",
    "apps/server",
    "apps/cli"
]
```

Benefits:

* shared dependency resolution
* consistent versions
* faster builds

---

# Crate Types

Rust workspaces typically include different crate types.

### Library crates

Reusable modules.

Example:

```
crates/domain
crates/storage
crates/services
```

---

### Application crates

Executable binaries.

Example:

```
apps/server
apps/cli
```

---

### Utility crates

Shared helpers.

Example:

```
crates/utils
```

Utility crates should remain **small and dependency-light**.

---

# Layered Architecture

Large Rust systems benefit from **strict layering**.

Recommended layers:

```
Application Layer
Service Layer
Domain Layer
Infrastructure Layer
Utility Layer
```

Dependency direction must be **top-down only**.

Example:

```
apps
  ↓
services
  ↓
domain
  ↓
infrastructure
  ↓
utils
```

Rules:

* domain must not depend on infrastructure
* services orchestrate domain logic
* infrastructure implements external systems

---

# Domain Layer

The domain layer defines core business logic.

Responsibilities:

* domain models
* domain rules
* core invariants
* pure logic

Characteristics:

* minimal dependencies
* no networking
* no database code

Example structure:

```
crates/domain/
├─ lib.rs
├─ models.rs
├─ errors.rs
└─ logic.rs
```

Domain code should be **pure Rust logic**.

---

# Service Layer

Services coordinate domain operations.

Responsibilities:

* orchestration
* workflows
* transaction boundaries

Example:

```
crates/services/
├─ lib.rs
├─ user_service.rs
└─ billing_service.rs
```

Services interact with:

* domain
* infrastructure

---

# Infrastructure Layer

Infrastructure connects the system to external components.

Examples:

* databases
* message queues
* HTTP clients
* file systems

Example structure:

```
crates/storage/
├─ lib.rs
├─ postgres.rs
└─ redis.rs
```

Infrastructure crates must implement **interfaces defined in domain/services**.

---

# API Layer

API crates expose external interfaces.

Examples:

* REST APIs
* gRPC
* CLI commands

Example:

```
crates/api/
├─ lib.rs
├─ http/
└─ grpc/
```

---

# Application Crates

Application crates wire the system together.

Example:

```
apps/server
```

Responsibilities:

* configuration
* dependency injection
* runtime initialization
* service wiring

Example structure:

```
apps/server/
├─ main.rs
├─ config.rs
└─ bootstrap.rs
```

---

# Dependency Direction Enforcement

Dependencies must follow strict direction.

Allowed:

```
services → domain
apps → services
services → infrastructure
```

Not allowed:

```
domain → services
domain → infrastructure
infrastructure → services
```

This ensures **core logic remains independent**.

---

# Interface Boundaries

Use **traits** to define system interfaces.

Example:

```
trait UserRepository {
    fn find_user(&self, id: UserId) -> Result<User>;
}
```

Infrastructure implementations provide concrete behavior.

Example:

```
struct PostgresUserRepository
```

Benefits:

* dependency inversion
* easier testing
* modular architecture

---

# Workspace Dependency Management

Dependencies should be centralized.

Workspace root `Cargo.toml`:

```toml
[workspace.dependencies]
serde = "1"
tokio = "1"
tracing = "0.1"
```

Crates reference them with:

```toml
serde = { workspace = true }
```

Benefits:

* consistent dependency versions
* simplified upgrades

---

# Feature Flags Across Crates

Feature flags allow conditional compilation.

Example:

```
features = ["metrics", "tracing"]
```

Use cases:

* optional telemetry
* experimental features
* platform-specific code

---

# Compilation Performance

Large Rust projects must optimize compile times.

Strategies:

### Small crates

Large modules should be split into smaller crates.

Benefits:

* incremental compilation
* faster rebuilds

---

### Stable dependency boundaries

Avoid frequent changes in foundational crates.

Changes in core crates trigger rebuilds.

---

### Dev dependencies isolation

Place test-only dependencies under:

```
[dev-dependencies]
```

---

# Monorepo Strategy

Rust workspaces function well in monorepos.

Advantages:

* shared infrastructure
* unified CI
* consistent tooling

Example structure:

```
repo/
├─ workspace/
│  ├─ crates/
│  └─ apps/
├─ docs/
└─ scripts/
```

---

# Testing Across Crates

Testing strategies include:

### Unit tests

Inside each crate.

```
#[cfg(test)]
mod tests
```

---

### Integration tests

Located in:

```
tests/
```

These test cross-crate behavior.

---

### System tests

Executed against full applications.

Example:

```
apps/server/tests/
```

---

# Workspace Tooling

Common developer workflow:

```
cargo check
cargo build
cargo test
cargo clippy
cargo fmt
```

Useful workspace commands:

```
cargo check --workspace
cargo test --workspace
```

---

# CI/CD Pipeline for Workspaces

CI pipelines should enforce:

1. formatting
2. linting
3. tests
4. security audit

Example pipeline steps:

```
cargo fmt --check
cargo clippy --all-targets --all-features
cargo test --workspace
cargo audit
```

---

# Versioning Strategy

Internal workspace crates usually share versioning.

Public libraries must follow **semantic versioning**.

Example:

```
0.1.0
0.2.0
1.0.0
```

---

# Observability Architecture

Telemetry should be centralized.

Recommended crates:

```
tracing
metrics
```

Applications initialize telemetry.

Example:

```
tracing_subscriber::init();
```

Libraries should emit logs but not configure them.

---

# Security Practices

Large systems must enforce:

* dependency auditing
* minimal unsafe usage
* input validation

Tools:

```
cargo audit
cargo deny
```

---

# Example Workspace Layout

Example production architecture:

```
workspace/
├─ Cargo.toml
├─ crates/
│  ├─ domain
│  ├─ services
│  ├─ storage
│  ├─ api
│  └─ utils
├─ apps/
│  ├─ server
│  └─ cli
└─ tests/
```

This structure scales effectively for large systems.

---

# Scaling Beyond 100k LOC

To maintain long-term scalability:

* enforce crate boundaries
* maintain strict dependency direction
* isolate domain logic
* centralize configuration
* keep crates small and focused

---

# Summary

Large Rust systems remain maintainable when they adopt:

* Cargo workspace architecture
* layered crate dependencies
* trait-based boundaries
* centralized dependency management
* modular crate design

This architecture enables Rust systems to scale to **large codebases without losing clarity or compile-time guarantees**.

---

