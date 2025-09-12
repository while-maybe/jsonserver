# Go Dynamic JSON API Server

[![Go Version](https://img.shields.io/badge/Go-1.25-00ADD8.svg?style=flat-square)](https://go.dev/)
[![Architect](https://img.shields.io/badge/Architecture-Hexagonal-8B008B.svg?style=flat-square)]()
<!-- [![Go Report Card](https://goreportcard.com/badge/github.com/while-maybe/json-server)](https://goreportcard.com/report/github.com/while-maybe/json-server)
[![Build Status](https://github.com/while-maybe/json-server/actions/workflows/go.yml/badge.svg)](https://github.com/while-maybe/json-server/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) -->

A high-fidelity mock API server written in idiomatic Go. It dynamically serves a complete RESTful API from a directory of JSON files, featuring live hot-reloading, transactional persistence, and a professional-grade concurrent architecture.

This project is engineered for frontend developers who need a fast, feature-rich backend for prototyping, and for backend developers studying robust software architecture in Go. It moves beyond simple mock servers by providing a concurrent-safe, transactional, file-based persistence layer that intelligently responds to live changes in its data source.

---

## ✨ Key Features & Design Philosophy

This server is built upon several core principles to ensure it is both powerful and reliable.

*   **Dual Operational Modes:** Runs as a persistent, file-watching server by default, or as a read-only server that instantly serves data piped to it from the command line.
*   **Live Hot-Reloading:** In its default server mode, it actively watches the data directory. **Create, modify, or delete a JSON file, and the API reflects the changes instantly** without a restart. This provides an incredibly fast development feedback loop.
*   **Zero-Configuration, Opinionated Behaviour:** On its first run, it creates a `data` directory populated with demonstration files. Simply edit these files or add your own to define your API. No schemas are required.
*   **Full CRUD API:** Complete support for `GET`, `POST`, `PUT`, and `DELETE` operations across all discovered resources.
*   **Dual Resource Types:** Natively understands the difference between **Collections** (JSON arrays of objects with `"id"` fields) and **Keyed-Objects** (JSON objects acting as key-value stores).
*   **Concurrent-Safe by Design:** All read and write operations are fully protected by `sync.RWMutex`, making the server safe for heavy concurrent use.
*   **Transactional File Persistence:** Write operations are atomic. If saving a file to disk fails, in-memory changes are automatically rolled back, guaranteeing data consistency.
*   **Clean Architectural Design:** Built using **Hexagonal Architecture (Ports & Adapters)** and **Dependency Injection** to create a decoupled, maintainable, and highly testable system.

---

## 🚀 Getting Started

### Prerequisites

*   Go `1.25` or later.
*   Git

### 1. Installation

Clone the repository to your local machine:
```bash
git clone https://github.com/while-maybe/jsonserver
cd json-server
```

### 2. Configuration

The server is configured using a clear precedence chain: **Environment Variables > `.env` file > Default Values**.

To configure for local development, copy the example file:
```bash
cp .env.example .env
```
Now you can edit the `.env` file to change the server address or data directory.

| Variable     | Default Value | Description                                         |
|--------------|---------------|-----------------------------------------------------|
| `SERVER_ADDR`| `:8080`       | The address the server will listen on.              |
| `DATA_DIR`   | `data`        | The directory where the JSON data files are stored. |

### 3. Running the Server

There are two ways to run the application.

#### **Mode 1: Default Server Mode** (with Persistence and Hot-Reloading)

Simply run the application. On the first run, it will automatically create the `data` directory with sample files if it doesn't exist.
```bash
go mod tidy
go run ./cmd/server/main.go
```
The server will start on `http://localhost:8080` and begin watching the `data` directory.

**Try the hot-reloading!** While the server is running, create a new `tags.json` file in the `data` directory. You will see a log message in the terminal, and the `/tags` endpoint will become available instantly.

#### **Mode 2: Pipe Mode** (Read-Only, from `stdin`)

The server can instantly serve any JSON data piped to it. This is incredibly useful for quick tests or for serving data from other command-line tools. In this mode, the API is **read-only**; `POST`, `PUT`, and `DELETE` requests will not persist.

**Linux / macOS Example:**
```bash
# Serve the contents of a single db.json file
cat db.json | GOEXPERIMENT=jsonv2 go run ./cmd/server/main.go
```

**Windows (PowerShell) Example:**
```powershell
# Serve the contents of a single db.json file
Get-Content db.json |  GOEXPERIMENT=jsonv2 go run ./cmd/server/main.go
```

---

## 💻 API Usage

Interact with your auto-generated API using any HTTP client. All responses are wrapped in a consistent envelope for predictability and ease of use.

#### **Root Endpoint (Discover all resources)**
```bash
curl localhost:8080/
```
**Response:** `200 OK`
```json
{
  "data": {
    "count": 2,
    "items": [
      "config",
      "posts"
    ]
  }
}
```

#### **Collection Operations (e.g., `/posts`)**
*   `GET /posts` - Get all records in the collection.
*   `GET /posts/{id}` - Get a single record by its ID.
*   `POST /posts` - Create a new record in the collection.
*   `PUT /posts/{id}` - Update an existing record by its ID.
*   `DELETE /posts/{id}` - Delete a record by its ID.

#### **Keyed-Object Operations (e.g., `/config`)**
*   `GET /config` - Get all key-value pairs from the object.
*   `GET /config/{key}` - Get a single value by its key.
*   `PUT /config/{key}` - Create or update a key-value pair.
*   `DELETE /config/{key}` - Delete a key-value pair.

---

## Smart Behaviour & API Conventions

To provide a seamless and predictable experience, the server has several smart, opinionated behaviours regarding data integrity and structure.

#### **Collections: Automatic Duplicate ID Prevention**

When you `POST` a new record to a collection (e.g., `/posts`), the server checks if a record with the same `id` already exists. If it does, the request is rejected with a `409 Conflict` status code to ensure the integrity of the collection.

#### **Keyed-Objects: The Key is the Identifier**

For keyed-object resources (e.g., `/config`), the key in the URL is treated as the record's unique identifier. This leads to two important behaviours:

1.  **ID Removal on `PUT`:** When you `PUT` data to a keyed-object (e.g., `/config/theme`), any `"id"` field within the JSON body is automatically **removed before persistence**. This enforces the principle that the URL key is the single source of truth, preventing ambiguity.

2.  **ID Injection on `GET`:** Conversely, for consistency, when you `GET` a single record from a keyed-object (e.g., `/config/theme`), the server automatically **injects the key into the response as an `"id"` field**. This powerful feature allows client-side applications to handle records from both collections and keyed-objects in a uniform way.

**Example `GET /config/theme` Response:**
```json
{
  "data": {
    "id": "theme",  // <-- Injected automatically
    "value": "dark" // The actual value stored in the file
  }
}
```

---

## 🏛️ Architectural Highlights & Design Philosophy

This project is a practical implementation of **Hexagonal Architecture (Ports & Adapters)**. The design choices were driven by the need for testability, maintainability, and resilience.

*   **Dependency Injection for Testability:** The repository's core logic is decoupled from the filesystem (`Persister` interface) and the file watcher (`Watcher` interface). This allows for 100% unit test coverage of critical failure paths—such as I/O errors during a write—by injecting mock objects in the tests.

*   **Transactional Writes with Rollbacks:** To prevent data corruption, all write operations are transactional. An in-memory modification is only committed to disk at the end of the operation. If the disk write fails, the original in-memory state is restored, guaranteeing consistency.

*   **Defensive Deep Copies:** To prevent subtle concurrency bugs with reference types (maps/slices), the repository performs a deep copy of all incoming data and of its internal state before modification. This ensures complete data isolation and makes the rollback mechanism bulletproof.

*   **Functional Options Pattern:** The repository's constructor uses the Functional Options pattern. This provides a clean, scalable, and self-documenting API for assembling the component with its dependencies, making it simple to use in production (`NewJsonRepository(dir)`) while allowing full control for testing (`NewJsonRepository(dir, WithPersister(mock))`).

### Architectural Flow
```mermaid
graph TD
    subgraph Filesystem
        I[(data/*.json)]
    end
    subgraph Application
        A[HTTP Request] --> B{Driving Adapter (httpadapter)};
        B --> C{Core Port (Service Interface)};
        C --> D[Core Logic (Service Impl)];
        D --> E{Core Port (Repository Interface)};
        E --> F{Driven Adapter (jsonrepo)};
        F --> G[Persister & Watcher Interfaces];
        G --> H[Concrete Implementations];
    end
    H -- reads/writes/watches --> I;
```

---

## ✅ Running the Tests

A comprehensive unit test suite provides an ironclad guarantee of the repository's correctness, including its transactional and concurrent behaviour.

To run all tests:
```bash
GOEXPERIMENT=jsonv2 go test ./...
```
The tests are located in the `_test` package for each component and use table-driven tests, `testify` for assertions, and mock objects to simulate failure conditions.

---

## 🗺️ Roadmap

The core functionality and architecture are stable and feature-complete. Future development will focus on enhancing the API's querying capabilities and developer experience.

### Core
-   [x] `GET`, `POST`, `PUT`, `DELETE` - Full support for all resource types.
-   [x] **Discovery:** Root endpoint (`/`) to list all available resources.
-   [x] **Live Hot-Reloading:** The repository watches the data directory for live changes.
-   [x] **Pipe Mode:** Ability to serve read-only data from `stdin`.
-   [x] **Architecture:** Full implementation of Hexagonal Architecture and Dependency Injection.
-   [x] **Configuration:** Graceful shutdown and external configuration via a `.env` file.
-   [x] **Testing:** Comprehensive unit test suite for the repository layer.

### Future Enhancements
-   💡 **API Querying:** Add support for pagination (`?_limit=`, `?_offset=`), filtering (`?field=value`), and sorting (`?_sort=field`) for collections.
-   💡 **Authentication:** Implement an optional middleware layer for API security using JWT or API keys.
--   💡 **Schema Validation:** Introduce optional JSON Schema validation for incoming `POST` and `PUT` requests.
-   💡 **DevOps:** Full Docker and Docker Compose support for easy containerisation and deployment.