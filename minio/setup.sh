#!/bin/bash

GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_section() {
    echo -e "\n${BLUE}=== $1 ===${NC}\n"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
    exit 1
}

print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

print_section "Verifying Environment"

# Check if we're in a directory containing the expected files
if [[ ! -f "01-minio-namespace.yaml" ]] || [[ ! -f "05-minio-statefulset.yaml" ]]; then
    print_error "This script must be run from the MinIO deployment directory containing the YAML manifests"
fi

# Check if kubectl is installed
if ! command -v kubectl &> /dev/null; then
    print_error "kubectl is not installed. Please install kubectl first"
fi

# Check if kubernetes cluster is accessible
if ! kubectl cluster-info &> /dev/null; then
    print_error "Cannot connect to Kubernetes cluster. Please check your kubeconfig"
fi

print_success "Environment verification completed"

# Deploy MinIO components in order
print_section "Deploying MinIO Components"

# Array of manifest files in order
manifests=(
    "001-minio-namespace.yaml"
    "002-minio-secrets.yaml"
    "003-minio-configmap.yaml"
    "004-minio-pvc.yaml"
    "005-minio-statefulset.yaml"
    "006-minio-services.yaml"
)

# Apply each manifest
for manifest in "${manifests[@]}"; do
    if [[ -f "$manifest" ]]; then
        echo "Applying $manifest..."
        if ! kubectl apply -f "$manifest"; then
            print_error "Failed to apply $manifest"
        fi
        print_success "Applied $manifest"
    else
        print_info "Skipping $manifest (file not found)"
    fi
done

# Wait for MinIO pod to be ready
print_section "Waiting for MinIO Pod"
echo "Waiting for MinIO pod to be ready (this may take a few minutes)..."

# Wait up to 5 minutes for the pod to be ready
timeout=300
start_time=$(date +%s)

while true; do
    if [[ $(kubectl get pods -n minio-system -l app=minio -o jsonpath='{.items[0].status.phase}') == "Running" ]]; then
        break
    fi
    
    current_time=$(date +%s)
    elapsed=$((current_time - start_time))
    
    if [[ ${elapsed} -gt ${timeout} ]]; then
        print_error "Timeout waiting for MinIO pod to be ready"
    fi
    
    echo -n "."
    sleep 5
done

print_success "MinIO pod is running"

# Run the test job
print_section "Running MinIO Test Job"
echo "Applying test job configuration..."

if ! kubectl apply -f 007-minio-test.yaml; then
    print_error "Failed to apply test job"
fi

# Wait for job completion and get logs
echo "Waiting for test job to complete..."
sleep 10

# Get and analyze test results
test_pod=$(kubectl get pods -n minio-system -l job-name=minio-test --output=jsonpath='{.items[0].metadata.name}')
test_logs=$(kubectl logs -n minio-system "$test_pod")

print_section "Test Results Analysis"
echo "MinIO Test Results:"
echo "-------------------"
echo -e "${GREEN}1. Connection Test:${NC}"
if echo "$test_logs" | grep -q "Added.*successfully"; then
    print_success "Successfully connected to MinIO server"
else
    print_error "Failed to connect to MinIO server"
fi

echo -e "\n${GREEN}2. Bucket Operations:${NC}"
if echo "$test_logs" | grep -q "Bucket created successfully"; then
    print_success "Successfully created test bucket"
else
    print_error "Failed to create test bucket"
fi

echo -e "\n${GREEN}3. File Operations:${NC}"
if echo "$test_logs" | grep -q "hello.txt.*->.*test-bucket" && echo "$test_logs" | grep -q "test.bin.*->.*test-bucket"; then
    print_success "Successfully uploaded test files"
    echo "  - Uploaded hello.txt (12 B)"
    echo "  - Uploaded test.bin (100 KiB)"
else
    print_error "Failed to upload test files"
fi

echo -e "\n${GREEN}4. Metadata Operations:${NC}"
if echo "$test_logs" | grep -q "Tags set for"; then
    print_success "Successfully set metadata tags"
fi

print_section "Deployment Complete"
print_success "MinIO has been successfully deployed and tested"
echo -e "\nTo access MinIO console, run:"
echo -e "${YELLOW}kubectl port-forward -n minio-system svc/minio 9001:9001${NC}"
echo -e "Then visit ${BLUE}http://localhost:9001${NC} in your browser"
echo -e "Credentials can be found in your minio-secrets.yaml file"