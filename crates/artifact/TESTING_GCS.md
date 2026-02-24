# GCS Artifact Client Testing Guide

This document describes how to test the GCS artifact client implementation.

## Unit Tests

Unit tests verify the core logic without requiring GCS infrastructure.

```bash
# Run all GCS unit tests
cargo test --package sp1-cluster-artifact gcs::tests

# Run a specific unit test
cargo test --package sp1-cluster-artifact gcs::tests::test_chunk_calculation_large_file
```

**Coverage:** 19 unit tests covering:
- Key generation and prefixing
- Chunking logic and boundary calculations
- Thread distribution
- Configuration validation

## Integration Tests

Integration tests verify actual upload/download with a real GCS bucket.

### Prerequisites

1. **GCP Authentication:**
   ```bash
   gcloud auth application-default login
   ```

2. **Create a test bucket:**
   ```bash
   # Create bucket (replace with your project and desired name)
   gsutil mb -p YOUR_PROJECT_ID -l us-east4 gs://your-test-bucket

   # Set lifecycle policy to auto-delete test artifacts (optional)
   echo '[{"action": {"type": "Delete"}, "condition": {"age": 1}}]' > lifecycle.json
   gsutil lifecycle set lifecycle.json gs://your-test-bucket
   ```

3. **Verify permissions:**
   ```bash
   # Test write access
   echo "test" | gsutil cp - gs://your-test-bucket/test.txt

   # Test read access
   gsutil cat gs://your-test-bucket/test.txt

   # Test delete access
   gsutil rm gs://your-test-bucket/test.txt
   ```

### Running Integration Tests

**Option 1: Using the test script (recommended)**
```bash
./scripts/test_gcs_integration.sh your-test-bucket
```

**Option 2: Manual test execution**
```bash
# Run all integration tests (17 tests)
TEST_GCS_BUCKET=your-test-bucket cargo test --package sp1-cluster-artifact --test gcs_integration_test -- --ignored --test-threads=1

# Run a specific integration test
TEST_GCS_BUCKET=your-test-bucket cargo test --package sp1-cluster-artifact --test gcs_integration_test test_upload_and_download_large_file -- --ignored

# Run only par_upload tests (3 tests)
TEST_GCS_BUCKET=your-test-bucket cargo test --package sp1-cluster-artifact --test gcs_integration_test test_par_upload -- --ignored

# Run only par_download tests (7 tests)
TEST_GCS_BUCKET=your-test-bucket cargo test --package sp1-cluster-artifact --test gcs_integration_test test_par_download -- --ignored

# Run round-trip and stress tests
TEST_GCS_BUCKET=your-test-bucket cargo test --package sp1-cluster-artifact --test gcs_integration_test round_trip -- --ignored
TEST_GCS_BUCKET=your-test-bucket cargo test --package sp1-cluster-artifact --test gcs_integration_test stress -- --ignored
```

**Note:** Use `--test-threads=1` to avoid concurrent access issues during testing.

### Integration Test Coverage

**Basic Integration Tests:**
1. `test_upload_and_download_small_file` - Tests files < 32MB (single chunk)
2. `test_upload_and_download_large_file` - Tests 40MB file (parallel chunked download)
3. `test_compressed_upload` - Tests pre-compressed artifact upload
4. `test_exists_and_delete` - Tests existence checks and deletion
5. `test_delete_batch` - Tests batch deletion of multiple artifacts
6. `test_artifact_type_prefixes` - Tests different artifact type prefixes (program/, stdin/, proof/, etc.)
7. `test_parallel_download_correctness` - Tests 100MB file with pattern verification (stress tests parallel chunk reassembly)

**par_upload Tests (Upload Path Coverage):**
1. `test_par_upload_small_file_single_chunk` - 1MB file (single upload path)
2. `test_par_upload_exactly_chunk_size` - Exactly 32MB (edge case)
3. `test_par_upload_large_file` - 50MB file (large file upload path)

**par_download Tests (Download Path Coverage):**
1. `test_par_download_small_file_single_chunk` - 5MB file (single download path)
2. `test_par_download_exactly_chunk_size` - ~32MB compressed (edge case)
3. `test_par_download_multiple_chunks` - 80MB file (parallel chunks)
4. `test_par_download_uneven_chunks` - 75MB file (tests uneven last chunk)
5. `test_par_download_with_different_concurrency` - Tests concurrency levels 1, 4, 16, 32
6. `test_par_upload_par_download_round_trip_various_sizes` - Tests 100KB to 100MB (8 sizes)
7. `test_par_download_stress_test` - 150MB file, 5 download attempts with timing

**Total: 17 integration tests**

### What Each Test Validates

**Small File Test:**
- Upload with compression
- Single-chunk download
- Data integrity

**Large File Test:**
- Upload of 40MB file
- Parallel chunked download (triggers at >32MB)
- Chunk reassembly correctness

**Parallel Download Correctness:**
- 100MB file with known pattern
- Multiple download attempts
- Verifies chunks are reassembled in correct order
- Stress tests concurrency logic

### Detailed par_upload and par_download Test Coverage

**Upload Path Testing:**
- **Small files (≤32MB):** Tests single upload without chunking
  - Validates `if data.len() <= CHUNK_SIZE` branch
  - Uses simple upload with retry
- **Exactly 32MB:** Edge case at threshold boundary
- **Large files (>32MB):** Tests large file upload path
  - Validates data cloning with Arc
  - Tests upload with content_length hint

**Download Path Testing:**
- **Small files (≤32MB):** Tests single download without parallel chunks
  - Validates `if size <= CHUNK_SIZE as i64` branch
  - Uses single download with retry
- **Large files (>32MB):** Tests parallel chunked download
  - Validates chunk enumeration: `(0..size).step_by(CHUNK_SIZE).enumerate()`
  - Tests thread distribution: `min(concurrency, starts.len())`
  - Tests chunk size calculation: `div_ceil(chunks, threads)`
  - Validates Range API with i64 to u64 conversion
  - Tests chunk reassembly via mpsc channel
  - Verifies no chunk is lost or duplicated

**Edge Cases Tested:**
- Files exactly at 32MB boundary (both compressed and uncompressed)
- Uneven last chunks (e.g., 75MB → 2 full chunks + 11MB partial)
- Very large files (150MB → ~5 chunks after compression)
- Different concurrency settings (1, 4, 16, 32)
- Multiple sequential downloads of same artifact

**Round-Trip Testing:**
Tests 8 different file sizes covering all code paths:
- 100KB, 1MB, 10MB (single chunk upload/download)
- 31MB (just under threshold)
- 32MB (exactly at threshold)
- 33MB (just over threshold, triggers large file path)
- 50MB, 100MB (well into parallel chunk territory)

## Manual Testing

For ad-hoc testing or debugging:

```rust
// In bin/node/src/main.rs or a test binary
use sp1_cluster_artifact::gcs::GcsArtifactClient;
use sp1_prover_types::{ArtifactClient, ArtifactType};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = GcsArtifactClient::new(
        "your-test-bucket".to_string(),
        32, // concurrency
    ).await?;

    // Test artifact
    struct TestId;
    impl sp1_prover_types::ArtifactId for TestId {
        fn id(&self) -> &str { "manual-test-123" }
    }
    let artifact = TestId;

    // Upload
    let data = b"Test data".to_vec();
    client.upload_raw(&artifact, ArtifactType::Program, data.clone()).await?;
    println!("✅ Uploaded");

    // Download
    let downloaded = client.download_raw(&artifact, ArtifactType::Program).await?;
    assert_eq!(data, downloaded);
    println!("✅ Downloaded and verified");

    // Delete
    client.delete(&artifact, ArtifactType::Program).await?;
    println!("✅ Deleted");

    Ok(())
}
```

## Monitoring Test Artifacts

During testing, you can monitor the bucket:

```bash
# List all objects
gsutil ls -r gs://your-test-bucket

# Watch for test artifacts in real-time
watch -n 2 'gsutil ls -r gs://your-test-bucket | tail -20'

# Check specific prefix
gsutil ls gs://your-test-bucket/program/

# Clean up all test artifacts
gsutil -m rm -r gs://your-test-bucket/test-*
```

## Code Path Coverage Matrix

| Test | Upload Path | Download Path | File Size | Chunks | Notes |
|------|-------------|---------------|-----------|--------|-------|
| `test_par_upload_small_file_single_chunk` | Single | Single | 1MB | 1 | Below threshold |
| `test_par_upload_exactly_chunk_size` | Single | Single | 32MB | 1 | At threshold |
| `test_par_upload_large_file` | Large | Parallel | 50MB | ~2 | Above threshold |
| `test_par_download_small_file_single_chunk` | Single | Single | 5MB | 1 | Below threshold |
| `test_par_download_exactly_chunk_size` | Single | Single | ~32MB | 1 | At threshold |
| `test_par_download_multiple_chunks` | Large | Parallel | 80MB | ~3 | Multiple chunks |
| `test_par_download_uneven_chunks` | Large | Parallel | 75MB | ~3 | Uneven last chunk |
| `test_par_download_with_different_concurrency` | Large | Parallel | 60MB | ~2 | Tests concurrency 1,4,16,32 |
| `test_par_upload_par_download_round_trip_various_sizes` | Both | Both | 100KB-100MB | 1-4 | 8 different sizes |
| `test_par_download_stress_test` | Large | Parallel | 150MB | ~5 | 5 sequential downloads |

**Upload Paths:**
- **Single:** `data.len() <= CHUNK_SIZE` → Simple upload
- **Large:** `data.len() > CHUNK_SIZE` → Upload with content_length

**Download Paths:**
- **Single:** `size <= CHUNK_SIZE` → Single download with retry
- **Parallel:** `size > CHUNK_SIZE` → Chunked parallel download with semaphore

## Performance Testing

To test parallel download performance:

```bash
# Enable debug logging
RUST_LOG=sp1_cluster_artifact=debug \
TEST_GCS_BUCKET=your-test-bucket \
cargo test --package sp1-cluster-artifact --test gcs_integration_test test_parallel_download_correctness -- --ignored --nocapture
```

This will show download timing and chunk information in the logs.

## Cleanup

After testing:

```bash
# Remove all test artifacts
gsutil -m rm -r gs://your-test-bucket/test-*

# Or delete the entire test bucket
gsutil rb gs://your-test-bucket
```

## Troubleshooting

**Authentication errors:**
```bash
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
```

**Permission errors:**
- Ensure your account has `storage.objects.create`, `storage.objects.get`, `storage.objects.delete` permissions
- Check bucket IAM permissions: `gsutil iam get gs://your-test-bucket`

**Timeout errors:**
- Increase backoff timeout in tests
- Check network connectivity to GCS
- Verify bucket region (prefer same region as your location)

**Chunk reassembly errors:**
- Check `test_parallel_download_correctness` test output
- Verify concurrent request limits aren't being hit
- Check GCS quotas: https://console.cloud.google.com/iam-admin/quotas
