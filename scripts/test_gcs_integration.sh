#!/bin/bash
set -e

# GCS Integration Test Script
# This script tests the GCS artifact client with a real GCS bucket

# Usage: ./scripts/test_gcs_integration.sh <bucket-name>

if [ -z "$1" ]; then
    echo "Usage: $0 <gcs-bucket-name>"
    echo ""
    echo "Example: $0 my-test-bucket"
    echo ""
    echo "Prerequisites:"
    echo "  1. GCP authentication configured (run: gcloud auth application-default login)"
    echo "  2. A GCS bucket created for testing"
    echo "  3. Permissions to read/write/delete objects in the bucket"
    exit 1
fi

BUCKET_NAME="$1"

echo "=========================================="
echo "GCS Integration Tests"
echo "=========================================="
echo "Bucket: $BUCKET_NAME"
echo ""

# Check if gcloud auth is configured
if ! gcloud auth application-default print-access-token &>/dev/null; then
    echo "❌ GCP authentication not configured"
    echo "Please run: gcloud auth application-default login"
    exit 1
fi

echo "✅ GCP authentication configured"
echo ""

# Check if bucket exists
if ! gsutil ls "gs://$BUCKET_NAME" &>/dev/null; then
    echo "❌ Bucket gs://$BUCKET_NAME does not exist or is not accessible"
    exit 1
fi

echo "✅ Bucket gs://$BUCKET_NAME is accessible"
echo ""

echo "Running integration tests..."
echo ""

# Run the integration tests
TEST_GCS_BUCKET="$BUCKET_NAME" cargo test --package sp1-cluster-artifact --test gcs_integration_test -- --ignored --test-threads=1

echo ""
echo "=========================================="
echo "All tests passed! ✅"
echo "=========================================="
