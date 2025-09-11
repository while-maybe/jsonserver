# Go Dynamic JSON API Server

A high-fidelity mock API server written in idiomatic Go. It dynamically serves a complete RESTful API from a directory of JSON files, featuring live hot-reloading, transactional persistence, and a professional-grade concurrent architecture.

This project is engineered for frontend developers who need a fast, feature-rich backend for prototyping, and for backend developers studying robust software architecture in Go. It moves beyond simple mock servers by providing a concurrent-safe, transactional, file-based persistence layer that intelligently responds to live changes in the data directory.

---

## ✨ Key Features & Design Philosophy

This server is built upon several core principles to ensure it is both powerful and reliable.

*   **Zero-Configuration, Opinionated Behaviour:** Drop your `.json` files into the `data` directory. The server discovers them at startup and automatically generates a complete RESTful API. No schemas or configuration files are required.
*   **Live Hot-Reloading:** The server actively watches the data directory for changes. **Create, modify, or delete a JSON file, and the API reflects the changes instantly** without a server restart. This provides an incredibly fast development feedback loop.
*   **Full CRUD API:** Complete support for `GET`, `POST`, `PUT`, and `DELETE` operations across all discovered resources.
*   **Dual Resource Types:** Natively understands the difference between **Collections** (JSON arrays of objects with `"id"` fields) and **Keyed-Objects** (JSON objects acting as key-value stores).
*   **Concurrent-Safe by Design:** All read and write operations are fully protected by `sync.RWMutex`, making the server safe for heavy concurrent use.
*   **Transactional File Persistence:** Write operations are atomic. If saving a file to disk fails, in-memory changes are automatically rolled back, guaranteeing data consistency.
*   **Clean Architectural Design:** Built using **Hexagonal Architecture (Ports & Adapters)** and **Dependency Injection** to create a decoupled, maintainable, and highly testable system.

---

## 🚀 Getting Started

### Prerequisites

*   Go `1.22` or later.
*   Git

### 1. Installation

Clone the repository to your local machine:
```bash
git clone https://github.com/your-username/your-repo.git
cd your-repo
```

### 2. Configuration & Data

The server is configured via environment variables (a `.env` file is supported for local development). By default, it looks for a `data/` directory in the project root.

**Create the data directory:**
```bash
mkdir data
```

Now, create your resource files inside the `data/` directory. The name of each file becomes a top-level API endpoint.

**Example `data/posts.json` (a Collection):**
```json
[
  { "id": "post-1", "title": "Hello World" },
  { "id": "post-2", "title": "Go is Fun" }
]
```

**Example `data/config.json` (a Keyed-Object):**
```json
{
  "theme": "dark",
  "notifications_enabled": true
}
```

### 3. Running the Server

Tidy the dependencies and run the application.
```bash
go mod tidy
go run ./cmd/server/main.go
```
The server will start on `http://localhost:8080`.

**Try the hot-reloading!** While the server is running, create a new `tags.json` file in the `data` directory. You will see a log message in the terminal, and the `/tags` endpoint will become available instantly.

---

## 💻 API Usage

Interact with your auto-generated API using any HTTP client. All responses are wrapped in a consistent envelope for predictability.

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

## 🏛️ Architectural Highlights & Key Decisions

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
go test ./...
```

The tests are located in the `_test` package for each component and use table-driven tests, `testify` for assertions, and mock objects to simulate failure conditions.

---

## 🗺️ Roadmap

The core functionality and architecture are stable and feature-complete. The next phase of development will focus on enhancing the API's querying capabilities and developer experience.

### Core
-   [x] `GET`, `POST`, `PUT`, `DELETE` - Full support for all resource types.
-   [x] **Discovery:** Root endpoint (`/`) to list all available resources.
-   [x] **Hot-Reloading:** The repository watches the data directory for live changes.
-   [x] **Architecture:** Full implementation of Hexagonal Architecture and Dependency Injection.
-   [x] **Configuration:** Graceful shutdown and external configuration via a `.env` file.
-   [x] **Testing:** Comprehensive unit test suite for the repository layer.

### Future Enhancements
-   💡 **API Querying:** Add support for pagination (`?_limit=`, `?_offset=`), filtering (`?field=value`), and sorting (`?_sort=field`) for collections.
-   💡 **Authentication:** Implement an optional middleware layer for API security using JWT or API keys.
-   💡 **Schema Validation:** Introduce optional JSON Schema validation for incoming `POST` and `PUT` requests.
-   💡 **DevOps:** Full Docker and Docker Compose support for easy containerisation and deployment.