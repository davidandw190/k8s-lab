APP_NAME = image-processing-webhook
DOCKER_REPO = docker.io/davidandw190
DOCKER_TAG ?= latest
DOCKER_IMAGE = $(DOCKER_REPO)/$(APP_NAME):$(DOCKER_TAG)
NAMESPACE = image-processing
NODEPORT = 30070

GO = go
GOFLAGS = -v
LDFLAGS = -ldflags "-s -w"

.PHONY: all build clean test docker-build docker-push deploy help run lint

all: build

build:
	@echo "Building $(APP_NAME)..."
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o bin/$(APP_NAME)

# Run the application locally
run:
	@echo "Running $(APP_NAME)..."
	$(GO) run main.go

# Clean build artifacts
clean:
	@echo "Cleaning up..."
	rm -rf bin/
	rm -f $(APP_NAME)

# Run tests
test:
	@echo "Running tests..."
	$(GO) test $(GOFLAGS) ./...

# Run linter
lint:
	@echo "Running linter..."
	golangci-lint run ./...

# Build Docker image
docker-build:
	@echo "Building Docker image $(DOCKER_IMAGE)..."
	docker build -t $(DOCKER_IMAGE) .

# Push Docker image to registry
docker-push: docker-build
	@echo "Pushing Docker image $(DOCKER_IMAGE)..."
	docker push $(DOCKER_IMAGE)

tag:
	$(eval GIT_HASH = $(shell git rev-parse --short HEAD))
	$(eval DATE = $(shell date +%Y%m%d))
	$(eval NEW_TAG = $(DATE)-$(GIT_HASH))
	@echo "Creating new tag: $(NEW_TAG)"
	$(eval DOCKER_TAG = $(NEW_TAG))
	@echo "Building with tag: $(DOCKER_TAG)"

# Build and push with a versioned tag
release: tag docker-build docker-push
	@echo "Released $(DOCKER_REPO)/$(APP_NAME):$(DOCKER_TAG)"

# Deploy to Kubernetes
deploy:
	@echo "Deploying $(APP_NAME) to Kubernetes..."
	kubectl apply -f ../k8s/webhook/001-deployment.yaml
	kubectl apply -f ../k8s/webhook/002-service.yaml
	kubectl apply -f ../k8s/webhook/003-sinkbinding.yaml


# Display help information
help:
	@echo "Available targets:"
	@echo "  build          - Build the application locally"
	@echo "  run            - Run the application locally"
	@echo "  clean          - Remove build artifacts"
	@echo "  test           - Run tests"
	@echo "  lint           - Run linter"
	@echo "  docker-build   - Build Docker image"
	@echo "  docker-push    - Push Docker image to registry"
	@echo "  tag            - Generate a new version tag"
	@echo "  release        - Build and push with a versioned tag"
	@echo "  deploy         - Deploy to Kubernetes"
	@echo "  help           - Display this help information"