
# Go Generic JSON Server

A dynamic, RESTful mock API server written in idiomatic Go, powered by Hexagonal Architecture and the new `json/v2` standard library module.

This project serves as a practical exploration of building robust, decoupled, and testable applications in Go. It's a lightweight, zero-configuration tool for frontend developers who need a quick mock backend, or for backend developers studying software architecture.

---

## ✨ Key Features (So Far)

*   **Dynamic Endpoint Generation:** Automatically creates RESTful API endpoints from a `db.json` file on startup.
*   **RESTful Collections:** Exposes `GET /{resourceName}` endpoints for any top-level array in your JSON data.
*   **Clean Architectural Design:** Built using **Hexagonal Architecture (Ports & Adapters)** to decouple business logic from external technologies.
*   **Modern Go:** Uses the experimental **`json/v2`** module to explore the future of Go's standard library.
*   **High-Performance Routing:** Leverages the popular `chi` router for efficient and expressive routing.

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

Create a `db.json` file in the root of the project. The server will use this file as its data source. Each top-level key that is an array will become a resource endpoint.

**Example `db.json`:**
```json
{
  "posts": [
    {
      "id": "post-1",
      "title": "Hello Hexagonal Architecture!",
      "author": "Alice"
    },
    {
      "id": "post-2",
      "title": "Vertical Slicing is Awesome",
      "author": "Bob"
    }
  ],
  "users": [
    {
      "id": "user-a",
      "username": "alice",
      "email": "alice@example.com"
    }
  ]
}
```

### 3. Running the Server

Tidy the dependencies and run the application:
```bash
go mod tidy
go run ./cmd/app/main.go
```
The server will start and be available at `http://localhost:8080`.

Note: As of Aug-2025 (Go v1.25.0), jsonv2 needs the app to be run with:
```bash
GOEXPERIMENT=jsonv2 go run ./...
```
This is expected to not be needed in versions 1.26+


---

## 💻 API Usage

You can now use tools like `curl` or Postman to interact with the auto-generated API.

#### Get all records from a collection:
```bash
curl localhost:8080/posts
```
**Response:** `200 OK`
```json
[
  {
    "author": "Alice",
    "id": "post-1",
    "title": "Hello Hexagonal Architecture!"
  },
  {
    "author": "Bob",
    "id": "post-2",
    "title": "Vertical Slicing is Awesome"
  }
]
```

#### Get a resource that doesn't exist:
If you request a resource not present in `db.json`, the server will correctly return a successful response with an empty array, which simplifies client-side logic.
```bash
curl localhost:8080/comments
```
**Response:** `200 OK`
```json
[]
```

---

## 🏛️ Architecture & Design

This project is built using **Hexagonal Architecture (Ports & Adapters)**. The primary goal is to isolate the core application logic from outside concerns like databases, APIs, and frameworks.

*   **The Core:** `internal/core` contains the pure business logic (`service`) and entities (`domain`). It knows nothing about HTTP, JSON files, or any specific technology.
*   **Ports:** The core defines its boundaries through Go interfaces (`port.go` and `repository.go`). These are the "ports."
*   **Adapters:** `internal/adapters` contain the technology-specific code.
    *   **Driving Adapters** (e.g., `httpadapter`) drive the application by calling the core's service ports.
    *   **Driven Adapters** (e.g., `jsonrepo`) are driven by the application and implement the core's repository ports.

This enforces the **Dependency Rule**: all dependencies point inward, toward the core.

### Folder Structure

```plaintext
.
└── internal/
    ├── adapters/
    │   ├── driven/jsonrepo/      # Adapter for file-based persistence
    │   └── driving/httpadapter/  # Adapter for the HTTP API
    └── core/
        ├── domain/               # Core business entities (e.g., Record)
        └── service/resource/     # Core business logic and port definitions
```

### Architectural Flow

```mermaid
graph TD
    A[HTTP Request] --> B{Driving Adapter (httpadapter)};
    B --> C{Core Port (Service Interface)};
    C --> D[Core Logic (Service Implementation)];
    D --> E{Core Port (Repository Interface)};
    E --> F{Driven Adapter (jsonrepo)};
    F --> G[(db.json)];
```

---

## 🛠️ Technology Stack

*   **Go:** The core language for the project.
*   **`json/v2` (experimental):** Chosen to explore the performance and correctness improvements in Go's upcoming standard library for JSON handling.
*   **`chi` Router:** A lightweight, fast, and idiomatic router that composes beautifully with the standard `net/http` library. It was chosen over the standard library for its superior URL parameter and middleware handling, and over larger frameworks like Gin to maintain a clear separation of concerns.
*   **Hexagonal Architecture:** The guiding architectural pattern, chosen to build a decoupled, maintainable, and highly testable system.

---

## 🗺️ Roadmap & Future Plans

This project is being built iteratively using a "vertical slice" approach.

-   [x] **Core:** `GET /{resourceName}` endpoint for reading collections.
-   [ ] **Feature:** `GET /{resourceName}/{id}` for reading a single record.
-   [ ] **Feature:** `POST /{resourceName}` to create a new record (with auto-ID generation).
-   [ ] **Feature:** `PUT`, `PATCH`, and `DELETE` support for full CRUD functionality.
-   [ ] **Enhancement:** Support for "Keyed Object" resources (where top-level JSON values are objects, not arrays).
-   [ ] **Enhancement:** Hot-reloading the server when `db.json` or a config file changes.
-   [ ] **Quality:** Comprehensive unit and integration tests for all layers.
-   [ ] **DevOps:** Docker support for easy containerization and deployment.