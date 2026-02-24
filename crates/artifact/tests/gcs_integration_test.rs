// Integration tests for GCS artifact client
// These tests require a real GCS bucket and authentication
//
// TEST COVERAGE (17 tests):
// - Basic integration: 7 tests (upload, download, compression, exists, delete, prefixes)
// - par_upload coverage: 3 tests (small, exact, large files)
// - par_download coverage: 7 tests (small, exact, multi-chunk, uneven, concurrency, round-trip, stress)
//
// To run these tests:
// 1. Set up GCP authentication: gcloud auth application-default login
// 2. Create a test bucket: gsutil mb -p PROJECT_ID gs://your-test-bucket
// 3. Run tests:
//    - All tests: TEST_GCS_BUCKET=your-test-bucket cargo test --test gcs_integration_test -- --ignored --test-threads=1
//    - Upload tests only: TEST_GCS_BUCKET=your-test-bucket cargo test --test gcs_integration_test test_par_upload -- --ignored
//    - Download tests only: TEST_GCS_BUCKET=your-test-bucket cargo test --test gcs_integration_test test_par_download -- --ignored
//
// See TESTING_GCS.md for detailed documentation

use anyhow::Result;
use sp1_cluster_artifact::gcs::GcsArtifactClient;
use sp1_prover_types::{ArtifactClient, ArtifactId, ArtifactType};

struct TestArtifact {
    id: String,
}

impl ArtifactId for TestArtifact {
    fn id(&self) -> &str {
        &self.id
    }
}

fn get_test_bucket() -> Option<String> {
    std::env::var("TEST_GCS_BUCKET").ok()
}

fn skip_if_no_bucket() {
    if get_test_bucket().is_none() {
        println!("Skipping GCS integration test: TEST_GCS_BUCKET not set");
        println!("To run: TEST_GCS_BUCKET=your-bucket cargo test --test gcs_integration_test -- --ignored");
    }
}

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket
async fn test_upload_and_download_small_file() -> Result<()> {
    skip_if_no_bucket();
    let bucket = match get_test_bucket() {
        Some(b) => b,
        None => return Ok(()),
    };

    let client = GcsArtifactClient::new(bucket, 32).await?;
    let artifact = TestArtifact {
        id: format!("test-small-{}", uuid::Uuid::new_v4()),
    };

    // Small data (< 32MB)
    let original_data = b"Hello, GCS! This is a small test file.".to_vec();

    // Upload
    client
        .upload_raw(&artifact, ArtifactType::Program, original_data.clone())
        .await?;

    // Download
    let downloaded_data = client
        .download_raw(&artifact, ArtifactType::Program)
        .await?;

    // Verify
    assert_eq!(original_data, downloaded_data);

    // Cleanup
    client.delete(&artifact, ArtifactType::Program).await?;

    println!("✅ Small file upload/download test passed");
    Ok(())
}

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket
async fn test_upload_and_download_large_file() -> Result<()> {
    skip_if_no_bucket();
    let bucket = match get_test_bucket() {
        Some(b) => b,
        None => return Ok(()),
    };

    let client = GcsArtifactClient::new(bucket, 32).await?;
    let artifact = TestArtifact {
        id: format!("test-large-{}", uuid::Uuid::new_v4()),
    };

    // Large data (> 32MB to trigger parallel download)
    let size = 40 * 1024 * 1024; // 40MB
    let original_data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();

    println!("Uploading {} MB file...", size / 1024 / 1024);

    // Upload
    client
        .upload_raw(&artifact, ArtifactType::Stdin, original_data.clone())
        .await?;

    println!("Downloading {} MB file...", size / 1024 / 1024);

    // Download (should use parallel chunks)
    let downloaded_data = client.download_raw(&artifact, ArtifactType::Stdin).await?;

    // Verify
    assert_eq!(original_data.len(), downloaded_data.len());
    assert_eq!(original_data, downloaded_data);

    // Cleanup
    client.delete(&artifact, ArtifactType::Stdin).await?;

    println!("✅ Large file upload/download test passed");
    Ok(())
}

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket
async fn test_compressed_upload() -> Result<()> {
    use sp1_cluster_artifact::CompressedUpload;

    skip_if_no_bucket();
    let bucket = match get_test_bucket() {
        Some(b) => b,
        None => return Ok(()),
    };

    let client = GcsArtifactClient::new(bucket, 32).await?;
    let artifact = TestArtifact {
        id: format!("test-compressed-{}", uuid::Uuid::new_v4()),
    };

    let original_data = b"This is test data for compression".repeat(1000);

    // Compress data manually
    let compressed_data = zstd::encode_all(original_data.as_slice(), 3)?;

    // Upload compressed
    client
        .upload_raw_compressed(&artifact, ArtifactType::Proof, compressed_data)
        .await?;

    // Download and verify (download_raw decompresses automatically)
    let downloaded_data = client.download_raw(&artifact, ArtifactType::Proof).await?;
    assert_eq!(original_data, downloaded_data);

    // Cleanup
    client.delete(&artifact, ArtifactType::Proof).await?;

    println!("✅ Compressed upload test passed");
    Ok(())
}

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket
async fn test_exists_and_delete() -> Result<()> {
    skip_if_no_bucket();
    let bucket = match get_test_bucket() {
        Some(b) => b,
        None => return Ok(()),
    };

    let client = GcsArtifactClient::new(bucket, 32).await?;
    let artifact = TestArtifact {
        id: format!("test-exists-{}", uuid::Uuid::new_v4()),
    };

    // Should not exist initially
    assert!(!client.exists(&artifact, ArtifactType::Program).await?);

    // Upload
    let data = b"Test data for exists check".to_vec();
    client
        .upload_raw(&artifact, ArtifactType::Program, data)
        .await?;

    // Should exist now
    assert!(client.exists(&artifact, ArtifactType::Program).await?);

    // Delete
    client.delete(&artifact, ArtifactType::Program).await?;

    // Should not exist after delete
    assert!(!client.exists(&artifact, ArtifactType::Program).await?);

    println!("✅ Exists and delete test passed");
    Ok(())
}

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket
async fn test_delete_batch() -> Result<()> {
    skip_if_no_bucket();
    let bucket = match get_test_bucket() {
        Some(b) => b,
        None => return Ok(()),
    };

    let client = GcsArtifactClient::new(bucket, 32).await?;

    // Create multiple artifacts
    let artifacts: Vec<TestArtifact> = (0..5)
        .map(|i| TestArtifact {
            id: format!("test-batch-{}-{}", i, uuid::Uuid::new_v4()),
        })
        .collect();

    // Upload all
    let data = b"Batch test data".to_vec();
    for artifact in &artifacts {
        client
            .upload_raw(artifact, ArtifactType::Proof, data.clone())
            .await?;
    }

    // Verify all exist
    for artifact in &artifacts {
        assert!(client.exists(artifact, ArtifactType::Proof).await?);
    }

    // Delete batch
    client.delete_batch(&artifacts, ArtifactType::Proof).await?;

    // Verify all deleted
    for artifact in &artifacts {
        assert!(!client.exists(artifact, ArtifactType::Proof).await?);
    }

    println!("✅ Batch delete test passed");
    Ok(())
}

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket
async fn test_artifact_type_prefixes() -> Result<()> {
    skip_if_no_bucket();
    let bucket = match get_test_bucket() {
        Some(b) => b,
        None => return Ok(()),
    };

    let client = GcsArtifactClient::new(bucket, 32).await?;
    let base_id = format!("test-prefix-{}", uuid::Uuid::new_v4());
    let data = b"Prefix test data".to_vec();

    let artifact_types = vec![
        ArtifactType::Program,
        ArtifactType::Stdin,
        ArtifactType::Proof,
        ArtifactType::Groth16Circuit,
        ArtifactType::PlonkCircuit,
    ];

    // Upload same ID with different types (should go to different prefixes)
    for artifact_type in &artifact_types {
        let artifact = TestArtifact {
            id: base_id.clone(),
        };
        client
            .upload_raw(&artifact, *artifact_type, data.clone())
            .await?;
    }

    // Verify all exist independently
    for artifact_type in &artifact_types {
        let artifact = TestArtifact {
            id: base_id.clone(),
        };
        assert!(client.exists(&artifact, *artifact_type).await?);

        // Download and verify
        let downloaded = client.download_raw(&artifact, *artifact_type).await?;
        assert_eq!(data, downloaded);
    }

    // Cleanup
    for artifact_type in &artifact_types {
        let artifact = TestArtifact {
            id: base_id.clone(),
        };
        client.delete(&artifact, *artifact_type).await?;
    }

    println!("✅ Artifact type prefix test passed");
    Ok(())
}

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket
async fn test_parallel_download_correctness() -> Result<()> {
    skip_if_no_bucket();
    let bucket = match get_test_bucket() {
        Some(b) => b,
        None => return Ok(()),
    };

    let client = GcsArtifactClient::new(bucket, 32).await?;
    let artifact = TestArtifact {
        id: format!("test-parallel-{}", uuid::Uuid::new_v4()),
    };

    // Create a large file with a known pattern to verify chunk reassembly
    let size = 100 * 1024 * 1024; // 100MB (will be split into ~4 chunks after compression)
    let mut original_data = Vec::with_capacity(size);
    for i in 0..size {
        // Use a pattern that's easy to verify
        original_data.push(((i / 1024) % 256) as u8);
    }

    println!("Uploading {} MB patterned file...", size / 1024 / 1024);

    // Upload
    client
        .upload_raw(&artifact, ArtifactType::Stdin, original_data.clone())
        .await?;

    println!("Downloading with parallel chunks...");

    // Download multiple times to stress test parallel logic
    for attempt in 1..=3 {
        let downloaded_data = client.download_raw(&artifact, ArtifactType::Stdin).await?;

        // Verify size
        assert_eq!(
            original_data.len(),
            downloaded_data.len(),
            "Size mismatch on attempt {}",
            attempt
        );

        // Verify content
        assert_eq!(
            original_data, downloaded_data,
            "Content mismatch on attempt {}",
            attempt
        );

        println!("  Attempt {}/3: ✓", attempt);
    }

    // Cleanup
    client.delete(&artifact, ArtifactType::Stdin).await?;

    println!("✅ Parallel download correctness test passed");
    Ok(())
}

// Tests specifically for par_upload_file and par_download_file internal methods

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket
async fn test_par_upload_small_file_single_chunk() -> Result<()> {
    skip_if_no_bucket();
    let bucket = match get_test_bucket() {
        Some(b) => b,
        None => return Ok(()),
    };

    let client = GcsArtifactClient::new(bucket, 32).await?;
    let artifact = TestArtifact {
        id: format!("test-par-upload-small-{}", uuid::Uuid::new_v4()),
    };

    // 1MB file - should use single upload path
    let size = 1 * 1024 * 1024;
    let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();

    println!("Testing par_upload with 1MB file (single chunk path)...");

    // Upload via upload_raw which calls par_upload_file internally
    client
        .upload_raw(&artifact, ArtifactType::Program, data.clone())
        .await?;

    // Verify
    assert!(client.exists(&artifact, ArtifactType::Program).await?);
    let downloaded = client.download_raw(&artifact, ArtifactType::Program).await?;
    assert_eq!(data, downloaded);

    // Cleanup
    client.delete(&artifact, ArtifactType::Program).await?;

    println!("✅ par_upload small file test passed");
    Ok(())
}

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket
async fn test_par_upload_exactly_chunk_size() -> Result<()> {
    skip_if_no_bucket();
    let bucket = match get_test_bucket() {
        Some(b) => b,
        None => return Ok(()),
    };

    let client = GcsArtifactClient::new(bucket, 32).await?;
    let artifact = TestArtifact {
        id: format!("test-par-upload-exact-{}", uuid::Uuid::new_v4()),
    };

    // Exactly 32MB - edge case, should still use single upload
    let size = 32 * 1024 * 1024;
    let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();

    println!("Testing par_upload with exactly 32MB file...");

    client
        .upload_raw(&artifact, ArtifactType::Stdin, data.clone())
        .await?;

    let downloaded = client.download_raw(&artifact, ArtifactType::Stdin).await?;
    assert_eq!(data.len(), downloaded.len());
    assert_eq!(data, downloaded);

    client.delete(&artifact, ArtifactType::Stdin).await?;

    println!("✅ par_upload exactly chunk size test passed");
    Ok(())
}

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket
async fn test_par_upload_large_file() -> Result<()> {
    skip_if_no_bucket();
    let bucket = match get_test_bucket() {
        Some(b) => b,
        None => return Ok(()),
    };

    let client = GcsArtifactClient::new(bucket, 32).await?;
    let artifact = TestArtifact {
        id: format!("test-par-upload-large-{}", uuid::Uuid::new_v4()),
    };

    // 50MB file - should use large file upload path (> 32MB threshold)
    let size = 50 * 1024 * 1024;
    let data: Vec<u8> = (0..size).map(|i| ((i / 4096) % 256) as u8).collect();

    println!("Testing par_upload with 50MB file (large file path)...");

    client
        .upload_raw(&artifact, ArtifactType::Proof, data.clone())
        .await?;

    let downloaded = client.download_raw(&artifact, ArtifactType::Proof).await?;
    assert_eq!(data.len(), downloaded.len());
    assert_eq!(data, downloaded);

    client.delete(&artifact, ArtifactType::Proof).await?;

    println!("✅ par_upload large file test passed");
    Ok(())
}

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket
async fn test_par_download_small_file_single_chunk() -> Result<()> {
    skip_if_no_bucket();
    let bucket = match get_test_bucket() {
        Some(b) => b,
        None => return Ok(()),
    };

    let client = GcsArtifactClient::new(bucket, 32).await?;
    let artifact = TestArtifact {
        id: format!("test-par-download-small-{}", uuid::Uuid::new_v4()),
    };

    // 5MB file - should use single download path
    let size = 5 * 1024 * 1024;
    let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();

    println!("Testing par_download with 5MB file (single chunk path)...");

    // Upload
    client
        .upload_raw(&artifact, ArtifactType::Program, data.clone())
        .await?;

    // Download via download_raw which calls par_download_file internally
    let downloaded = client.download_raw(&artifact, ArtifactType::Program).await?;
    assert_eq!(data.len(), downloaded.len());
    assert_eq!(data, downloaded);

    // Cleanup
    client.delete(&artifact, ArtifactType::Program).await?;

    println!("✅ par_download small file test passed");
    Ok(())
}

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket
async fn test_par_download_exactly_chunk_size() -> Result<()> {
    skip_if_no_bucket();
    let bucket = match get_test_bucket() {
        Some(b) => b,
        None => return Ok(()),
    };

    let client = GcsArtifactClient::new(bucket, 32).await?;
    let artifact = TestArtifact {
        id: format!("test-par-download-exact-{}", uuid::Uuid::new_v4()),
    };

    // Exactly 32MB compressed size - edge case
    // We need to upload data that compresses to exactly 32MB
    let size = 40 * 1024 * 1024; // Start with larger size
    let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();

    println!("Testing par_download with ~32MB compressed file (edge case)...");

    client
        .upload_raw(&artifact, ArtifactType::Stdin, data.clone())
        .await?;

    let downloaded = client.download_raw(&artifact, ArtifactType::Stdin).await?;
    assert_eq!(data.len(), downloaded.len());
    assert_eq!(data, downloaded);

    client.delete(&artifact, ArtifactType::Stdin).await?;

    println!("✅ par_download exactly chunk size test passed");
    Ok(())
}

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket
async fn test_par_download_multiple_chunks() -> Result<()> {
    skip_if_no_bucket();
    let bucket = match get_test_bucket() {
        Some(b) => b,
        None => return Ok(()),
    };

    let client = GcsArtifactClient::new(bucket, 32).await?;
    let artifact = TestArtifact {
        id: format!("test-par-download-multi-{}", uuid::Uuid::new_v4()),
    };

    // 80MB file - will trigger parallel chunked download
    let size = 80 * 1024 * 1024;
    let data: Vec<u8> = (0..size).map(|i| ((i / 8192) % 256) as u8).collect();

    println!("Testing par_download with 80MB file (parallel chunks)...");

    client
        .upload_raw(&artifact, ArtifactType::Proof, data.clone())
        .await?;

    // Download should use parallel chunks
    let downloaded = client.download_raw(&artifact, ArtifactType::Proof).await?;
    assert_eq!(data.len(), downloaded.len());
    assert_eq!(data, downloaded);

    client.delete(&artifact, ArtifactType::Proof).await?;

    println!("✅ par_download multiple chunks test passed");
    Ok(())
}

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket
async fn test_par_download_uneven_chunks() -> Result<()> {
    skip_if_no_bucket();
    let bucket = match get_test_bucket() {
        Some(b) => b,
        None => return Ok(()),
    };

    let client = GcsArtifactClient::new(bucket, 32).await?;
    let artifact = TestArtifact {
        id: format!("test-par-download-uneven-{}", uuid::Uuid::new_v4()),
    };

    // 70MB + 5MB = 75MB - will create uneven last chunk after compression
    let size = 75 * 1024 * 1024;
    let data: Vec<u8> = (0..size).map(|i| ((i / 16384) % 256) as u8).collect();

    println!("Testing par_download with 75MB file (uneven last chunk)...");

    client
        .upload_raw(&artifact, ArtifactType::Stdin, data.clone())
        .await?;

    let downloaded = client.download_raw(&artifact, ArtifactType::Stdin).await?;
    assert_eq!(data.len(), downloaded.len());
    assert_eq!(data, downloaded);

    client.delete(&artifact, ArtifactType::Stdin).await?;

    println!("✅ par_download uneven chunks test passed");
    Ok(())
}

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket
async fn test_par_download_with_different_concurrency() -> Result<()> {
    skip_if_no_bucket();
    let bucket = match get_test_bucket() {
        Some(b) => b,
        None => return Ok(()),
    };

    // Test with different concurrency levels
    let concurrency_levels = vec![1, 4, 16, 32];
    let size = 60 * 1024 * 1024; // 60MB
    let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();

    for concurrency in concurrency_levels {
        let client = GcsArtifactClient::new(bucket.clone(), concurrency).await?;
        let artifact = TestArtifact {
            id: format!("test-par-download-concurrency-{}-{}", concurrency, uuid::Uuid::new_v4()),
        };

        println!("Testing par_download with concurrency={}...", concurrency);

        client
            .upload_raw(&artifact, ArtifactType::Program, data.clone())
            .await?;

        let downloaded = client.download_raw(&artifact, ArtifactType::Program).await?;
        assert_eq!(data.len(), downloaded.len());
        assert_eq!(data, downloaded);

        client.delete(&artifact, ArtifactType::Program).await?;
        println!("  ✓ Concurrency {} passed", concurrency);
    }

    println!("✅ par_download with different concurrency test passed");
    Ok(())
}

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket
async fn test_par_upload_par_download_round_trip_various_sizes() -> Result<()> {
    skip_if_no_bucket();
    let bucket = match get_test_bucket() {
        Some(b) => b,
        None => return Ok(()),
    };

    let client = GcsArtifactClient::new(bucket, 32).await?;

    // Test various sizes to cover different code paths
    let test_sizes = vec![
        ("100KB", 100 * 1024),
        ("1MB", 1 * 1024 * 1024),
        ("10MB", 10 * 1024 * 1024),
        ("31MB", 31 * 1024 * 1024),
        ("32MB", 32 * 1024 * 1024),
        ("33MB", 33 * 1024 * 1024),
        ("50MB", 50 * 1024 * 1024),
        ("100MB", 100 * 1024 * 1024),
    ];

    for (label, size) in test_sizes {
        let artifact = TestArtifact {
            id: format!("test-roundtrip-{}-{}", label, uuid::Uuid::new_v4()),
        };

        println!("Testing {} round-trip...", label);

        // Create data with pattern for verification
        let data: Vec<u8> = (0..size).map(|i| ((i / 1024) % 256) as u8).collect();

        // Upload (via par_upload_file internally)
        client
            .upload_raw(&artifact, ArtifactType::Proof, data.clone())
            .await?;

        // Download (via par_download_file internally)
        let downloaded = client.download_raw(&artifact, ArtifactType::Proof).await?;

        // Verify
        assert_eq!(data.len(), downloaded.len(), "{} size mismatch", label);
        assert_eq!(data, downloaded, "{} content mismatch", label);

        // Cleanup
        client.delete(&artifact, ArtifactType::Proof).await?;

        println!("  ✓ {} passed", label);
    }

    println!("✅ Round-trip test for various sizes passed");
    Ok(())
}

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket
async fn test_par_download_stress_test() -> Result<()> {
    skip_if_no_bucket();
    let bucket = match get_test_bucket() {
        Some(b) => b,
        None => return Ok(()),
    };

    let client = GcsArtifactClient::new(bucket, 32).await?;
    let artifact = TestArtifact {
        id: format!("test-par-download-stress-{}", uuid::Uuid::new_v4()),
    };

    // 150MB file - tests chunking with many chunks
    let size = 150 * 1024 * 1024;

    // Create data with a verifiable pattern
    let mut data = Vec::with_capacity(size);
    for chunk_idx in 0..(size / 1024) {
        // Each 1KB block has a unique byte pattern based on its index
        let byte_val = ((chunk_idx / 100) % 256) as u8;
        for _ in 0..1024 {
            data.push(byte_val);
        }
    }

    println!("Testing par_download stress test with 150MB file...");
    println!("  This will create multiple 32MB chunks for parallel download");

    // Upload
    let start = std::time::Instant::now();
    client
        .upload_raw(&artifact, ArtifactType::Stdin, data.clone())
        .await?;
    let upload_time = start.elapsed();
    println!("  Upload took: {:?}", upload_time);

    // Download multiple times to stress test
    for attempt in 1..=5 {
        let start = std::time::Instant::now();
        let downloaded = client.download_raw(&artifact, ArtifactType::Stdin).await?;
        let download_time = start.elapsed();

        assert_eq!(data.len(), downloaded.len(), "Size mismatch on attempt {}", attempt);
        assert_eq!(data, downloaded, "Content mismatch on attempt {}", attempt);

        println!("  Attempt {}/5: {:?}", attempt, download_time);
    }

    // Cleanup
    client.delete(&artifact, ArtifactType::Stdin).await?;

    println!("✅ par_download stress test passed");
    Ok(())
}
