.DEFAULT_GOAL := help

VERSION  ?= dev
REVISION ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
IMAGE    ?= ghcr.io/aenix-org/loadbalancer-controller:$(VERSION)

.PHONY: help
help: ## Show this help.
	@awk 'BEGIN {FS = ":.*## "}; /^[a-zA-Z_-]+:.*## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: tidy
tidy: ## Run `go mod tidy`.
	go mod tidy

.PHONY: build
build: ## Build the binary for the host platform.
	CGO_ENABLED=0 go build \
		-trimpath \
		-ldflags "-s -w -X main.version=$(VERSION) -X main.revision=$(REVISION)" \
		-o bin/loadbalancer-controller \
		./cmd

.PHONY: test
test: ## Run unit tests with the race detector.
	go test -race ./...

.PHONY: vet
vet: ## Run `go vet`.
	go vet ./...

.PHONY: lint
lint: ## Run golangci-lint (requires the binary in PATH).
	golangci-lint run

.PHONY: check
check: vet test ## Run vet + tests.

.PHONY: image
image: ## Build the OCI image with the host's docker / podman.
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg REVISION=$(REVISION) \
		--file Containerfile \
		--tag $(IMAGE) \
		.

.PHONY: push
push: image ## Build + push the OCI image. Requires `docker login ghcr.io`.
	docker push $(IMAGE)
