#!/bin/bash

# Installation script for Go SDK development tools
set -e

echo "Installing Go development tools..."

# Function to check if a command exists
is_installed() {
    command -v "$1" &> /dev/null
}

# 1. Install sqlc
if is_installed sqlc; then
    echo "sqlc is already installed: $(sqlc version)"
else
    echo "Installing sqlc..."
    go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest
fi

# 2. Install swag
if is_installed swag; then
    echo "swag is already installed: $(swag --version)"
else
    echo "Installing swag..."
    go install github.com/swaggo/swag/cmd/swag@latest
fi

# 3. Install golangci-lint
GOLANGCI_VERSION="v2.1.5"
if is_installed golangci-lint; then
    echo "golangci-lint is already installed: $(golangci-lint --version)"
else
    echo "Installing golangci-lint ${GOLANGCI_VERSION}..."
    curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b $(go env GOPATH)/bin ${GOLANGCI_VERSION}
fi

# 4. Install golang-migrate
if is_installed migrate; then
    echo "golang-migrate is already installed: $(migrate -version)"
else
    echo "Fetching latest golang-migrate version..."
    MIGRATE_VERSION=$(curl -s https://api.github.com/repos/golang-migrate/migrate/releases/latest | grep '"tag_name"' | cut -d'"' -f4)
    echo "Installing golang-migrate ${MIGRATE_VERSION}..."
    
    # Download and extract to a temporary directory
    TMP_DIR=$(mktemp -d)
    curl -L "https://github.com/golang-migrate/migrate/releases/download/${MIGRATE_VERSION}/migrate.linux-amd64.tar.gz" | tar -xz -C "$TMP_DIR"
    
    mv "$TMP_DIR/migrate" "$(go env GOPATH)/bin/migrate"
    chmod +x "$(go env GOPATH)/bin/migrate"
    rm -rf "$TMP_DIR"
fi

echo ""
echo "=============================================="
echo "Verification complete!"
echo "=============================================="