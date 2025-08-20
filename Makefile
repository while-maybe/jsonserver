export PATH := $(shell go env GOPATH)/bin:$(PATH)

.DEFAULT_GOAL := build

.PHONY: fmt vet build
fmt:
	@go fmt ./...

vet: fmt
	@go vet ./...

build: vet
	@go build ./...

clean:
	@go mod tidy
	@go clean

test:
	@go test ./... -vet=off

# --- Database Migrations ---
DB_URL := sqlite3://louder.db
MIGRATIONS_PATH := migrations

.PHONY: migrate-new migrate-up migrate-down migrate-status
migrate-new: ## Create a new migration file. Usage: make migrate-new name=create_users
	@echo ">> Creating migration: $(name)"
	@migrate create -ext .sql -dir migrations $(name)

migrate-up: ## Apply all 'up' migrations
	@echo ">> Applying all up migrations..."
	@migrate -database "$(DB_URL)" -path "$(MIGRATIONS_PATH)" up

migrate-down: ## Revert the last 'down' migration
	@echo ">> Reverting the last migration..."
	@migrate -database "$(DB_URL)" -path "$(MIGRATIONS_PATH)" down 1

migrate-status: ## Show the current migration status
	@echo ">> Checking migration status..."
	@migrate -database "$(DB_URL)" -path "$(MIGRATIONS_PATH)" version

migrate-force: ## Force a migration to a different version
	@echo ">> Forcing migration to $(version)..."
	@migrate -database "$(DB_URL)" -path "$(MIGRATIONS_PATH)" force $(version)
