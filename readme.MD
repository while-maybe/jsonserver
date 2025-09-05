# Go Generic JSON Server

A dynamic, concurrent-safe, RESTful mock API server written in idiomatic Go. Powered by Hexagonal Architecture, Dependency Injection, and the new `json/v2` standard library module.

This project is a high-fidelity mock server designed for frontend developers who need a quick, feature-rich backend, and for backend developers studying professional software architecture in Go. It goes beyond simple mock servers by providing a transactional, file-based persistence layer, full CRUD capabilities for both collections and keyed-objects, and a clean, decoupled design.

---

## ✨ Key Features

*   **Full CRUD API:** Complete support for `GET`, `POST`, `PUT`, and `DELETE` operations.
*   **Dynamic Endpoint Generation:** Automatically creates RESTful API endpoints from any `db.json` file on startup.
*   **Dual Resource Types:** Natively supports both **Collections** (arrays of objects with IDs) and **Keyed-Objects** (JSON objects acting as key-value stores).
*   **Concurrent-Safe:** All read and write operations are fully protected by mutexes, making the server safe for concurrent use.
*   **Transactional File Persistence:** Write operations are atomic. If saving to disk fails, in-memory changes are automatically rolled back, guaranteeing data consistency.
*   **Clean Architectural Design:** Built using **Hexagonal Architecture (Ports & Adapters)** and **Dependency Injection** to create a decoupled, maintainable, and highly testable system.
*   **Modern Go & High-Performance Routing:** Uses the experimental **`json/v2`** module and the popular `chi` router for efficiency and expressiveness.

---

## 🚀 Getting Started

### Prerequisites

*   Go `1.2x` or later (with support for the experimental `json/v2` module).
*   Git

### 1. Installation

Clone the repository to your local machine:
```bash
git clone https://github.com/while-maybe/jsonserver.git
cd jsonserver
```

### 2. Configuration (`db.json`)

Create a `db.json` file in the root of the project. This file is your database. The server supports any combination of top-level arrays (collections) and objects (keyed-objects).

**Example `db.json`:**
```json
{
  "posts": [
    {
      "id": "post-1",
      "title": "Hello Hexagonal Architecture!",
      "author": "Alice"
    }
  ],
  "users": [
    {
      "id": "user-a",
      "username": "alice"
    }
  ],
  "config": {
    "theme": "dark",
    "notifications": {
      "enabled": true,
      "level": "verbose"
    }
  }
}
```

### 3. Running the Server

Tidy the dependencies and run the application. The server will start on `http://localhost:8080`.
```bash
go mod tidy
GOEXPERIMENT=jsonv2 go run ./cmd/app/main.go
```
*(Note: The `GOEXPERIMENT=jsonv2` flag is required for Go versions `1.22`+ and is expected to become standard in `1.26`+.)*

---

## 💻 API Usage

Interact with your auto-generated API using any HTTP client. All responses containing data are wrapped in a consistent `{"data": ...}` envelope, and list responses include a `count`.

#### **Root Endpoint (List all resources)**
```bash
curl localhost:8080/
```
**Response:** `200 OK`
```json
{
  "count": 3,
  "data": [
    "config",
    "posts",
    "users"
  ]
}
```

#### **Collection Operations (`/posts`)**
*   `GET /posts` - Get all records in the collection.
*   `GET /posts/{id}` - Get a single record by its ID.
*   `POST /posts` - Create a new record in the collection.
*   `PUT /posts/{id}` - Update a record by its ID.
*   `DELETE /posts/{id}` - Delete a record by its ID.

#### **Keyed-Object Operations (`/config`)**
*   `GET /config` - Get all key-value pairs from the object.
*   `GET /config/{key}` - Get a single value (or nested object) by its key.
*   `PUT /config/{key}` - Create or update a key-value pair.
*   `DELETE /config/{key}` - Delete a key-value pair.

---

## 🏛️ Architecture & Design

This project is a practical implementation of **Hexagonal Architecture (Ports & Adapters)**, enforcing a strict separation of concerns. The core application logic is completely decoupled from the HTTP API and the file-based persistence mechanism, making the system modular and easy to test.

*   **The Core (`internal/core`):** Contains the pure business logic (`service`) and entities (`domain`). It is technology-agnostic.
*   **Ports:** Go interfaces define the boundaries between the core and the outside world (`resource.Service`, `resource.Repository`).
*   **Adapters (`internal/adapters`):**
    *   **Driving Adapters (`httpadapter`):** Translate HTTP requests into calls to the core service.
    *   **Driven Adapters (`jsonrepo`):** Implement the repository port, providing the file-based persistence logic.

This design enforces the **Dependency Rule**: all dependencies point inward, towards the core.

### Architectural Flow

```mermaid
graph TD
    A[HTTP Request] --> B{Driving Adapter (httpadapter)};
    B --> C{Core Port (Service Interface)};
    C --> D[Core Logic (Service Implementation)];
    D --> E{Core Port (Repository Interface)};
    E --> F{Driven Adapter (jsonrepo)};
    F --> G[Persister Interface];
    G --> H{File Persister};
    H --> I[(db.json)];
```

---

## 🛠️ Technology Stack

*   **Go:** The core language for the project.
*   **`json/v2` (experimental):** Chosen to explore the performance and correctness improvements in Go's upcoming standard library for JSON handling.
*   **`chi` Router:** A lightweight, fast, and idiomatic router chosen for its excellent URL parameter handling and composable middleware.
*   **Hexagonal Architecture:** The guiding architectural pattern for building a decoupled and highly testable system.
*   **Dependency Injection:** Used to decouple the repository from its persistence mechanism, enabling robust testing of failure modes (e.g., disk write errors).

---

## 🗺️ Roadmap & Future Plans

This project is under active development. The core CRUD functionality is complete, and comprehensive unit tests are in progress.

### Core
-   [x] `GET` - Full support for collections, keyed-objects, and single records.
-   [x] `POST` - Create records in collections.
-   [x] `PUT` - Update records in collections and upsert in keyed-objects.
-   [x] `DELETE` - Full support for collections and keyed-objects.
-   [x] **Discovery:** Root endpoint (`/`) to list all available resources.
-   [x] **Architecture:** Full implementation of Hexagonal Architecture and Dependency Injection.
-   [ ] **Testing:** Comprehensive unit and integration test suite. *(In Progress)*

### Future Ideas & Enhancements
-   **Concurrency:** Explore more advanced concurrency patterns, such as using channels for write operations to serialize access even more effectively.
-   **Hot-Reloading:** Implement a file-watcher to automatically reload the database or configuration when `db.json` changes on disk.
-   **Authentication:** Add a middleware layer for API security using JWT or API keys.
-   **Pagination:** Add support for `?_limit=` and `?_offset=` query parameters for collection endpoints.
-   **Filtering & Sorting:** Implement basic filtering (`?field=value`) and sorting (`?_sort=field`) for collections.
-   **DevOps:** Full Docker and Docker Compose support for easy containerization and deployment.
