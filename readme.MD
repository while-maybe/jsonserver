# Go Dynamic JSON API Server

A high-fidelity mock API server written in idiomatic Go. It dynamically serves a complete RESTful API from a directory of JSON files, featuring hot-reloading, transactional persistence, and a professional-grade concurrent architecture.

This project is engineered for frontend developers who need a fast, feature-rich backend for prototyping, and for backend developers studying robust software architecture in Go. It moves beyond simple mock servers by providing a concurrent-safe, transactional, file-based persistence layer that intelligently responds to live changes in the data directory.

---

## ✨ Key Features & Design Philosophy

This server is built upon several core principles to ensure it is both powerful and reliable.

*   **Zero-Configuration, Opinionated Behaviour:** Drop your `.json` files into the `data` directory. The server discovers them at startup and automatically generates a complete RESTful API. No schemas or configuration files are required.
*   **Live Hot-Reloading:** The server actively watches the data directory for changes. **Create, modify, or delete a JSON file, and the API reflects the changes instantly** without a server restart. This provides an incredibly fast development feedback loop.
*   **Full CRUD API:** Complete support for `GET`, `POST`, `PUT`, and `DELETE` operations across all discovered resources.
*   **Dual Resource Types:** Natively understands the difference between **Collections** (JSON arrays of objects with `"id"` fields) and **Keyed-Objects** (JSON objects acting as key-value stores).
*   **Concurrent-Safe by Design:** All read and write operations are fully protected by mutexes, making the server safe for heavy concurrent use.
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
git clone https://github.com/while-maybe/jsonserver.git
cd jsonserver
```

### 2. Configuration & Data

The server is configured via environment variables (or a `.env` file). By default, it looks for a `data/` directory in the project root.

**Create the data directory:**
```bash
mkdir data
```

Now, create your resource files inside the `data/` directory. The name of each file becomes a top-level API endpoint.

**Example `data/posts.json` (a Collection):**
```json
[
  {
    "id": "post-1",
    "title": "Hello Hexagonal Architecture!",
    "author": "Alice"
  },
  {
    "id": "post-2",
    "title": "The Power of Hot-Reloading",
    "author": "Bob"
  }
]
```

**Example `data/config.json` (a Keyed-Object):**
```json
{
  "theme": "dark",
  "notifications": {
    "enabled": true,
    "level": "verbose"
  }
}
```

### 3. Running the Server

Tidy the dependencies and run the application. The server will start on `http://localhost:8080`.
```bash
go mod tidy
go run ./cmd/server/main.go
```
Now, try it out! **While the server is running, create a new `tags.json` file in the `data` directory.** You will see a log message in the terminal, and the `/tags` endpoint will become available instantly.

---

## 💻 API Usage

Interact with your auto-generated API using any HTTP client. All responses containing data are wrapped in a consistent `{"data": ...}` envelope, and list responses include a `count`.

#### **Root Endpoint (Discover all resources)**
```bash
curl localhost:8080/
```
**Response:** `200 OK`
```json
{
  "count": 2,
  "data": [
    "config",
    "posts"
  ]
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
*   `GET /config/{key}` - Get a single value (or nested object) by its key.
*   `PUT /config/{key}` - Create or update a key-value pair.
*   `DELETE /config/{key}` - Delete a key-value pair.

---

## 🏛️ Architecture & Design

This project is a practical implementation of **Hexagonal Architecture (Ports & Adapters)**, enforcing a strict separation of concerns. The core application logic is completely decoupled from the HTTP API and the file-based persistence mechanism.

*   **The Core (`internal/core`):** Contains the pure business logic (`service`) and entities (`domain`). It is technology-agnostic.
*   **Ports:** Go interfaces (`resource.Service`, `resource.Repository`) define the contracts between the core and the outside world.
*   **Adapters (`internal/adapters`):**
    *   **Driving Adapters (`httpadapter`):** Translate HTTP requests into calls to the core service.
    *   **Driven Adapters (`jsonrepo`):** Implement the repository port, providing the multi-file, hot-reloading persistence logic. It contains a `Persister` component responsible for all filesystem interactions.

This design enforces the **Dependency Rule**: all dependencies point inward, towards the core.

### Architectural Flow

```mermaid
graph TD
    subgraph Filesystem
        I[(data/*.json)]
    end
    subgraph Application
        A[HTTP Request] --> B{Driving Adapter (httpadapter)};
        B --> C{Core Port (Service Interface)};
        C --> D[Core Logic (Service Implementation)];
        D --> E{Core Port (Repository Interface)};
        E --> F{Driven Adapter (jsonrepo)};
        F --> G[Persister Interface];
        G --> H{File Persister};
    end
    H --> I;
    F -- watches --> I;
```

---

## 🛠️ Technology Stack

*   **Go:** The core language for the project.
*   **`chi` Router:** A lightweight, fast, and idiomatic router chosen for its excellent URL parameter handling and composable middleware.
*   **`fsnotify`:** A powerful library used to implement the cross-platform, live hot-reloading of data files.
*   **Hexagonal Architecture:** The guiding architectural pattern for building a decoupled and highly testable system.
*   **Dependency Injection:** Used to decouple the repository from its persistence mechanism, enabling robust testing of failure modes.

---

## 🗺️ Roadmap & Future Plans

The core functionality and architecture are stable and feature-complete. Comprehensive unit tests provide a high degree of confidence in the repository's correctness.

### Core
-   [x] `GET`, `POST`, `PUT`, `DELETE` - Full support for all resource types.
-   [x] **Discovery:** Root endpoint (`/`) to list all available resources.
-   [x] **Hot-Reloading:** The repository now watches the data directory for live changes.
-   [x] **Architecture:** Full implementation of Hexagonal Architecture and Dependency Injection.
-   [x] **Testing:** Comprehensive unit test suite for the repository layer.
-   [ ] **Configuration:** Graceful shutdown and external configuration via a `.env` file. *(In Progress)*

### Future Ideas & Enhancements
-   **API Querying:** Add support for pagination (`?_limit=`, `?_offset=`), filtering (`?field=value`), and sorting (`?_sort=field`) for collections.
-   **Authentication:** Implement an optional middleware layer for API security using JWT or API keys.
-   **Schema Validation:** Introduce optional JSON Schema validation for incoming `POST` and `PUT` requests.
-   **DevOps:** Full Docker and Docker Compose support for easy containerisation and deployment.
-   **Web UI:** A simple web interface to view and interact with the available API resources.