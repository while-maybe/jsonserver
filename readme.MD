# Go Dynamic JSON API Server

[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8.svg?style=flat-square)](https://go.dev/)
[![Architecture](https://img.shields.io/badge/Architecture-Hexagonal-8B008B.svg?style=flat-square)]()


A high-fidelity mock API server written in idiomatic Go. It dynamically serves a complete RESTful API from a directory of JSON files, featuring live hot-reloading, configurable persistence strategies, and a professional-grade concurrent architecture.

This project is engineered for frontend developers who need a fast, feature-rich backend for prototyping, and for backend developers studying robust software architecture in Go. It moves beyond simple mock servers by providing a concurrent-safe, transactional, and highly configurable persistence layer that intelligently responds to live changes in its data source.

---

## ✨ Key Features & Design Philosophy

This server is built upon several core principles to ensure it is both powerful and reliable.

*   **Dual Operational Modes:** Runs as a persistent, file-watching server by default, or as a read-only server that instantly serves data piped to it from the command line.
*   **Live Hot-Reloading:** In its default server mode, it actively watches the data directory. **Create, modify, or delete a JSON file, and the API reflects the changes instantly** without a restart, providing an incredibly fast development feedback loop.
*   **Configurable Persistence:** Control the durability vs. performance trade-off with configurable write strategies. Choose between immediate synchronous writes for maximum safety, asynchronous writes for speed, or batched writes for high-throughput environments.
*   **Zero-Configuration Startup:** On its first run, it creates a `data` directory populated with demonstration files. Simply edit these files or add your own to define your API. No schemas are required.
*   **Full CRUD API:** Complete support for `GET`, `POST`, `PUT`, and `DELETE` operations across all discovered resources.
*   **Concurrent-Safe by Design:** All read and write operations are fully protected by `sync.RWMutex`, making the server safe for heavy concurrent use.
*   **Transactional File Persistence:** Write operations are atomic. If saving a file to disk fails in synchronous mode, in-memory changes are automatically rolled back, guaranteeing data consistency.
*   **Clean Architectural Design:** Built using **Hexagonal Architecture (Ports & Adapters)** and **Dependency Injection** to create a decoupled, maintainable, and highly testable system.

---

## 🚀 Getting Started

### Prerequisites

*   Go `1.25` or later.
*   Git

### 1. Installation

Clone the repository to your local machine:
```bash
git clone https://github.com/your-username/your-repo.git
cd your-repo
```

### 2. Configuration

The server is configured using a clear precedence chain: **Environment Variables > `.env` file > Default Values**.

To configure for local development, copy the example file:
```bash
cp .env.example .env
```
Now you can edit the `.env` file to customise the server's behaviour.

| Variable             | Default           | Description                                                                                             |
|----------------------|-------------------|---------------------------------------------------------------------------------------------------------|
| `SERVER_ADDR`        | `:8080`           | The address the server will listen on.                                                                  |
| `DATA_DIR`           | `data`            | The directory where the JSON data files are stored and watched.                                         |
| `PERSISTENCE_MODE`   | `immediate_async` | The write strategy. Options: `immediate_sync`, `immediate_async`, `batched`.                              |
| `PERSISTENCE_TIMER`  | `5s`              | The interval for the `batched` persistence mode (e.g., `5s`, `1m`).                                     |

### 3. Running the Server

There are two ways to run the application.

#### **Mode 1: Default Server Mode** (with Persistence and Hot-Reloading)

Simply run the application. On the first run, it will automatically create the `data` directory with sample files if it doesn't exist.
```bash
go mod tidy
go run ./cmd/server/main.go
```
The server will start and begin watching the `data` directory.

**Try the hot-reloading!** While the server is running, create a new `tags.json` file in the `data` directory. You will see a log message, and the `/tags` endpoint will become available instantly.

#### **Mode 2: Pipe Mode** (Read-Only, from `stdin`)

The server can instantly serve any JSON data piped to it. This is incredibly useful for quick tests or for serving data from other command-line tools. In this mode, the API is **read-only**.

**Linux / macOS Example:**
```bash
# Serve the contents of a single db.json file
cat db.json | go run ./cmd/server/main.go
```

**Windows (PowerShell) Example:**
```powershell
# Serve the contents of a single db.json file
Get-Content db.json | go run ./cmd/server/main.go
```

---

## 💻 API Usage & Conventions

Interact with your auto-generated API using any HTTP client. All responses are wrapped in a consistent envelope for predictability.

#### **Smart Behaviour & API Conventions**

*   **Collections: Automatic Duplicate ID Prevention:** A `POST` request with an existing `id` will be rejected with `409 Conflict` to ensure data integrity.
*   **Keyed-Objects: The Key is the Identifier:**
    *   On `PUT`, any `"id"` field in the request body is removed to enforce the URL key as the single source of truth.
    *   On `GET`, the key is automatically injected as an `"id"` field in the response for client-side consistency.

---

## 🏛️ Architectural Highlights & Design Philosophy

This project is a practical implementation of **Hexagonal Architecture (Ports & Adapters)**. The design choices were driven by the need for testability, maintainability, and resilience.

*   **Dependency Injection for Testability:** The repository's core logic is decoupled from the filesystem (`Persister` interface) and the file watcher (`Watcher` interface). This allows for 100% unit test coverage of critical failure paths—such as I/O errors during a write—by injecting mock objects in the tests.

*   **Transactional Writes with Rollbacks:** In synchronous mode, write operations are fully transactional. An in-memory modification is only considered successful after it is persisted. If the disk write fails, the original in-memory state is restored, guaranteeing consistency.

*   **Defensive Deep Copies:** To prevent subtle concurrency bugs with reference types (maps/slices), the repository performs a deep copy of all incoming data and of its internal state before modification. This ensures complete data isolation and makes the rollback mechanism bulletproof.

*   **Functional Options Pattern:** The repository's constructor uses the Functional Options pattern. This provides a clean, scalable, and self-documenting API for assembling the component with its dependencies.

---

## ✅ Running the Tests

A comprehensive unit test suite provides an ironclad guarantee of the repository's correctness, including its transactional and concurrent behaviour.

To run all tests:
```bash
go test ./...
```

---

## 🗺️ Roadmap

The core functionality and architecture are stable and feature-complete. Future development will focus on enhancing the API's querying capabilities.

### Core
-   [x] `GET`, `POST`, `PUT`, `DELETE` - Full support for all resource types.
-   [x] **Discovery:** Root endpoint (`/`) to list all available resources.
-   [x] **Live Hot-Reloading:** The repository watches the data directory for live changes.
-   [x] **Pipe Mode:** Ability to serve read-only data from `stdin`.
-   [x] **Configurable Persistence:** Support for `immediate_sync`, `immediate_async`, and `batched` write modes.
-   [x] **Architecture:** Full implementation of Hexagonal Architecture and Dependency Injection.
-   [x] **Configuration:** Graceful shutdown and external configuration via `.env` file.
-   [x] **Testing:** Comprehensive unit test suite for the repository layer.

### Future Enhancements
-   💡 **API Querying:** Add support for pagination (`?_limit=`, `?_offset=`), filtering (`?field=value`), and sorting (`?_sort=field`) for collections.
-   💡 **Authentication:** Implement an optional middleware layer for API security using JWT or API keys.
-   💡 **Durability:** Implement a Write-Ahead Log (WAL) to make asynchronous and batched modes crash-safe.
-   💡 **DevOps:** Full Docker and Docker Compose support for easy containerisation and deployment.