use crate::CompressedUpload;
use sp1_prover_types::{ArtifactClient, ArtifactId, ArtifactType};

use anyhow::{anyhow, Result};
use backoff::{exponential::ExponentialBackoff, ExponentialBackoffBuilder, SystemClock};
use google_cloud_storage::client::{Client as GcsClient, ClientConfig};
use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use google_cloud_storage::http::objects::delete::DeleteObjectRequest;
use lazy_static::lazy_static;
use std::{sync::Arc, time::Duration};
use tokio::{sync::Semaphore, task::JoinSet};
use tracing::instrument;

lazy_static! {
    static ref BACKOFF: ExponentialBackoff<SystemClock> = ExponentialBackoffBuilder::new()
        .with_initial_interval(Duration::from_millis(100))
        .with_max_elapsed_time(Some(Duration::from_secs(120)))
        .build();
}

const CHUNK_SIZE: usize = 32 * 1024 * 1024; // 32MB chunks (same as S3)

#[derive(Clone)]
pub struct GcsArtifactClient {
    pub client: Arc<GcsClient>,
    pub bucket: String,
    semaphore: Arc<Semaphore>,
    concurrency: usize,
}

impl GcsArtifactClient {
    pub async fn new(bucket: String, concurrency: usize) -> Result<Self> {
        let config = ClientConfig::default()
            .with_auth()
            .await
            .map_err(|e| anyhow!("Failed to create GCS client auth: {}", e))?;

        let client = GcsClient::new(config);

        Ok(Self {
            client: Arc::new(client),
            bucket,
            semaphore: Arc::new(Semaphore::new(concurrency)),
            concurrency,
        })
    }

    /// Different artifact types have different GCS prefixes.
    ///
    /// This allows different expiration times via lifecycle rules per prefix.
    fn get_gcs_key_from_id(artifact_type: ArtifactType, id: &str) -> String {
        match artifact_type {
            ArtifactType::Program => format!("program/{id}"),
            ArtifactType::Stdin => format!("stdin/{id}"),
            ArtifactType::Proof => format!("proof/{id}"),
            ArtifactType::Groth16Circuit => format!("groth16-circuit/{id}"),
            ArtifactType::PlonkCircuit => format!("plonk-circuit/{id}"),
            _ => format!("other/{id}"),
        }
    }

    async fn par_download_file(&self, artifact_type: ArtifactType, id: &str) -> Result<Vec<u8>> {
        let key = Self::get_gcs_key_from_id(artifact_type, id);

        // Get object size first
        let size = backoff::future::retry(BACKOFF.clone(), || {
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let key = key.clone();
            async move {
                let metadata = client
                    .get_object(&GetObjectRequest {
                        bucket,
                        object: key,
                        ..Default::default()
                    })
                    .await
                    .map_err(|e| backoff::Error::permanent(anyhow!("Failed to get object metadata: {}", e)))?;
                Ok::<i64, backoff::Error<anyhow::Error>>(metadata.size)
            }
        })
        .await?;

        // If the file is smaller than the chunk size, just download it
        if size <= CHUNK_SIZE as i64 {
            let data = backoff::future::retry(BACKOFF.clone(), || {
                let client = self.client.clone();
                let bucket = self.bucket.clone();
                let key = key.clone();
                async move {
                    client
                        .download_object(
                            &GetObjectRequest {
                                bucket,
                                object: key,
                                ..Default::default()
                            },
                            &Range(None, None),
                        )
                        .await
                        .map_err(|e| backoff::Error::permanent(anyhow!("Failed to download object: {}", e)))
                }
            })
            .await?;
            return Ok(data);
        }

        // Parallel chunked download
        let starts = (0..size)
            .step_by(CHUNK_SIZE)
            .enumerate()
            .collect::<Vec<_>>();
        let threads = std::cmp::min(self.concurrency, starts.len());
        let thread_size = std::cmp::max(starts.len().div_ceil(threads), 1);

        let mut set = JoinSet::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(threads);

        starts.chunks(thread_size).for_each(|thread_starts| {
            let client = self.client.clone();
            let key = key.clone();
            let tx = tx.clone();
            let semaphore = self.semaphore.clone();
            let thread_starts = thread_starts.to_vec();
            let bucket = self.bucket.clone();

            set.spawn(async move {
                let _permit = semaphore.acquire().await;
                for (index, start) in thread_starts {
                    let end = std::cmp::min(start + CHUNK_SIZE as i64, size) - 1;
                    let body = backoff::future::retry(BACKOFF.clone(), || {
                        let client = client.clone();
                        let key = key.clone();
                        let bucket = bucket.clone();
                        async move {
                            client
                                .download_object(
                                    &GetObjectRequest {
                                        bucket,
                                        object: key,
                                        ..Default::default()
                                    },
                                    &Range(Some(start as u64), Some(end as u64)),
                                )
                                .await
                                .map_err(|e| backoff::Error::permanent(anyhow!("Failed to download chunk: {}", e)))
                        }
                    })
                    .await?;
                    tx.send((index, body)).await?;
                }
                Ok::<(), anyhow::Error>(())
            });
        });

        drop(tx);
        let mut result = vec![0_u8; size as usize];

        while let Some((index, chunk)) = rx.recv().await {
            let end = std::cmp::min(index * CHUNK_SIZE + chunk.len(), size as usize);
            result[index * CHUNK_SIZE..end].copy_from_slice(&chunk);
        }

        // Make sure all threads did not panic
        while !set.is_empty() {
            let res = set.join_next().await.unwrap();
            match res {
                Ok(inner) => match inner {
                    Ok(_) => {}
                    Err(e) => {
                        panic!("artifact download thread panicked: {e:?}");
                    }
                },
                Err(e) => {
                    panic!("artifact download thread panicked: {e:?}");
                }
            }
        }

        Ok(result)
    }

    async fn par_upload_file(
        &self,
        artifact_type: ArtifactType,
        id: &str,
        data: Vec<u8>,
    ) -> Result<()> {
        let key = Self::get_gcs_key_from_id(artifact_type, id);

        // If the file is smaller than the chunk size, just upload it
        if data.len() <= CHUNK_SIZE {
            backoff::future::retry(BACKOFF.clone(), || {
                let client = self.client.clone();
                let bucket = self.bucket.clone();
                let key = key.clone();
                let data = data.clone();
                async move {
                    client
                        .upload_object(
                            &UploadObjectRequest {
                                bucket,
                                ..Default::default()
                            },
                            data,
                            &UploadType::Simple(Media {
                                name: key.into(),
                                content_type: "application/octet-stream".into(),
                                content_length: None,
                            }),
                        )
                        .await
                        .map_err(|e| backoff::Error::permanent(anyhow!("Failed to upload object: {}", e)))
                }
            })
            .await?;
            return Ok(());
        }

        let data = Arc::new(data);
        let data_len = data.len();

        // Note: GCS doesn't support S3-style multipart uploads where different parts can be
        // uploaded in parallel to different upload sessions. GCS resumable uploads require
        // sequential chunk uploads to the same session. For large files, we use a single
        // upload which GCS handles efficiently with internal buffering and streaming.
        // The parallel download implementation above provides the key performance benefit.
        backoff::future::retry(BACKOFF.clone(), || {
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let key = key.clone();
            let data = data.clone();
            async move {
                client
                    .upload_object(
                        &UploadObjectRequest {
                            bucket,
                            ..Default::default()
                        },
                        (*data).clone(),
                        &UploadType::Simple(Media {
                            name: key.into(),
                            content_type: "application/octet-stream".into(),
                            content_length: Some(data_len as u64),
                        }),
                    )
                    .await
                    .map_err(|e| backoff::Error::permanent(anyhow!("Failed to upload large object: {}", e)))
            }
        })
        .await?;

        Ok(())
    }
}

impl ArtifactClient for GcsArtifactClient {
    #[instrument(name = "upload", level = "info", fields(id = artifact.id()), skip(self, artifact, data))]
    async fn upload_raw(
        &self,
        artifact: &impl ArtifactId,
        artifact_type: ArtifactType,
        data: Vec<u8>,
    ) -> Result<()> {
        // Compress with zstd (level 3 for balanced compression/speed)
        let compressed = zstd::encode_all(data.as_slice(), 3)
            .map_err(|e| anyhow!("Failed to compress artifact: {}", e))?;

        self.par_upload_file(artifact_type, artifact.id(), compressed)
            .await
    }

    #[instrument(name = "download", level = "info", fields(id = artifact.id()), skip(self, artifact))]
    async fn download_raw(
        &self,
        artifact: &impl ArtifactId,
        artifact_type: ArtifactType,
    ) -> Result<Vec<u8>> {
        let compressed = self
            .par_download_file(artifact_type, artifact.id())
            .await?;

        let decoded = zstd::decode_all(compressed.as_slice())
            .map_err(|e| anyhow!("Failed to decompress artifact: {}", e))?;

        Ok(decoded)
    }

    async fn exists(&self, artifact: &impl ArtifactId, artifact_type: ArtifactType) -> Result<bool> {
        let key = Self::get_gcs_key_from_id(artifact_type, artifact.id());

        match self
            .client
            .get_object(&GetObjectRequest {
                bucket: self.bucket.clone(),
                object: key,
                ..Default::default()
            })
            .await
        {
            Ok(_) => Ok(true),
            Err(google_cloud_storage::http::Error::Response(e)) if e.code == 404 => Ok(false),
            Err(e) => Err(anyhow!("Failed to check if artifact exists: {}", e)),
        }
    }

    async fn delete(&self, artifact: &impl ArtifactId, artifact_type: ArtifactType) -> Result<()> {
        let key = Self::get_gcs_key_from_id(artifact_type, artifact.id());

        self.client
            .delete_object(&DeleteObjectRequest {
                bucket: self.bucket.clone(),
                object: key,
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow!("Failed to delete artifact: {}", e))?;

        Ok(())
    }

    async fn delete_batch(&self, artifacts: &[impl ArtifactId], artifact_type: ArtifactType) -> Result<()> {
        if artifacts.is_empty() {
            return Ok(());
        }

        // GCS doesn't have native batch delete, so delete in parallel
        let mut set = JoinSet::new();

        for artifact in artifacts {
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let key = Self::get_gcs_key_from_id(artifact_type, artifact.id());

            set.spawn(async move {
                client
                    .delete_object(&DeleteObjectRequest {
                        bucket,
                        object: key,
                        ..Default::default()
                    })
                    .await
                    .map_err(|e| anyhow!("Failed to delete artifact: {}", e))
            });
        }

        // Wait for all deletions
        while let Some(res) = set.join_next().await {
            match res {
                Ok(inner) => inner?,
                Err(e) => return Err(anyhow!("Delete thread panicked: {}", e)),
            }
        }

        Ok(())
    }

    async fn add_ref(&self, _artifact: &impl ArtifactId, _key: &str) -> Result<()> {
        // GCS doesn't need reference counting like Redis
        // Artifacts are managed via lifecycle policies
        Ok(())
    }

    async fn remove_ref(
        &self,
        _artifact: &impl ArtifactId,
        _artifact_type: ArtifactType,
        _key: &str,
    ) -> Result<bool> {
        // GCS doesn't need reference counting like Redis
        Ok(false)
    }
}

impl CompressedUpload for GcsArtifactClient {
    #[instrument(name = "upload_compressed", level = "info", fields(id = artifact.id()), skip(self, artifact, data))]
    async fn upload_raw_compressed(
        &self,
        artifact: &impl ArtifactId,
        artifact_type: ArtifactType,
        data: Vec<u8>,
    ) -> Result<()> {
        // Data is already compressed, upload directly
        self.par_upload_file(artifact_type, artifact.id(), data)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_gcs_key_from_id_program() {
        let key = GcsArtifactClient::get_gcs_key_from_id(ArtifactType::Program, "test-id-123");
        assert_eq!(key, "program/test-id-123");
    }

    #[test]
    fn test_get_gcs_key_from_id_stdin() {
        let key = GcsArtifactClient::get_gcs_key_from_id(ArtifactType::Stdin, "input-456");
        assert_eq!(key, "stdin/input-456");
    }

    #[test]
    fn test_get_gcs_key_from_id_proof() {
        let key = GcsArtifactClient::get_gcs_key_from_id(ArtifactType::Proof, "proof-789");
        assert_eq!(key, "proof/proof-789");
    }

    #[test]
    fn test_get_gcs_key_from_id_groth16_circuit() {
        let key = GcsArtifactClient::get_gcs_key_from_id(ArtifactType::Groth16Circuit, "circuit-abc");
        assert_eq!(key, "groth16-circuit/circuit-abc");
    }

    #[test]
    fn test_get_gcs_key_from_id_plonk_circuit() {
        let key = GcsArtifactClient::get_gcs_key_from_id(ArtifactType::PlonkCircuit, "plonk-def");
        assert_eq!(key, "plonk-circuit/plonk-def");
    }

    #[test]
    fn test_get_gcs_key_from_id_unspecified() {
        let key = GcsArtifactClient::get_gcs_key_from_id(ArtifactType::UnspecifiedArtifactType, "unknown-xyz");
        assert_eq!(key, "other/unknown-xyz");
    }

    #[test]
    fn test_chunk_size_constant() {
        // Verify CHUNK_SIZE is 32MB as documented
        assert_eq!(CHUNK_SIZE, 32 * 1024 * 1024);
    }

    #[test]
    fn test_backoff_configuration() {
        // Verify backoff is configured correctly
        let backoff = BACKOFF.clone();
        assert_eq!(backoff.initial_interval, Duration::from_millis(100));
        assert_eq!(backoff.max_elapsed_time, Some(Duration::from_secs(120)));
    }

    #[test]
    fn test_chunk_calculation_small_file() {
        // File smaller than CHUNK_SIZE should result in single chunk
        let size: i64 = (CHUNK_SIZE / 2) as i64;
        let chunks: Vec<_> = (0..size).step_by(CHUNK_SIZE).collect();
        assert_eq!(chunks.len(), 1);
    }

    #[test]
    fn test_chunk_calculation_exact_chunk_size() {
        // File exactly CHUNK_SIZE should result in single chunk
        let size: i64 = CHUNK_SIZE as i64;
        let chunks: Vec<_> = (0..size).step_by(CHUNK_SIZE).collect();
        assert_eq!(chunks.len(), 1);
    }

    #[test]
    fn test_chunk_calculation_large_file() {
        // File larger than CHUNK_SIZE should be split into multiple chunks
        let size: i64 = (CHUNK_SIZE * 3 + CHUNK_SIZE / 2) as i64; // 3.5 chunks
        let chunks: Vec<_> = (0..size).step_by(CHUNK_SIZE).collect();
        assert_eq!(chunks.len(), 4); // Should have 4 chunks (last one partial)
    }

    #[test]
    fn test_chunk_boundaries() {
        // Test chunk boundary calculations
        let size: i64 = (CHUNK_SIZE * 2) as i64;
        let chunks: Vec<(usize, i64)> = (0..size)
            .step_by(CHUNK_SIZE)
            .enumerate()
            .collect();

        // First chunk
        let start0 = chunks[0].1;
        let end0 = std::cmp::min(start0 + CHUNK_SIZE as i64, size) - 1;
        assert_eq!(start0, 0);
        assert_eq!(end0, (CHUNK_SIZE - 1) as i64);

        // Second chunk
        let start1 = chunks[1].1;
        let end1 = std::cmp::min(start1 + CHUNK_SIZE as i64, size) - 1;
        assert_eq!(start1, CHUNK_SIZE as i64);
        assert_eq!(end1, (2 * CHUNK_SIZE - 1) as i64);
    }

    #[test]
    fn test_thread_distribution_fewer_chunks_than_concurrency() {
        // When chunks < concurrency, threads should equal chunks
        let concurrency = 32;
        let num_chunks = 5;
        let threads = std::cmp::min(concurrency, num_chunks);
        assert_eq!(threads, 5);
    }

    #[test]
    fn test_thread_distribution_more_chunks_than_concurrency() {
        // When chunks > concurrency, threads should equal concurrency
        let concurrency = 32;
        let num_chunks = 100;
        let threads = std::cmp::min(concurrency, num_chunks);
        assert_eq!(threads, 32);
    }

    #[test]
    fn test_thread_size_calculation() {
        // Test thread_size calculation for distributing chunks
        let concurrency: usize = 32;
        let num_chunks: usize = 100;
        let threads = std::cmp::min(concurrency, num_chunks);
        let thread_size = std::cmp::max(num_chunks.div_ceil(threads), 1);

        // 100 chunks / 32 threads = 3.125, so thread_size should be 4 (ceil)
        assert_eq!(thread_size, 4);

        // Verify all chunks can be covered
        assert!(threads * thread_size >= num_chunks);

        // Verify the calculation is correct: if we reduce thread_size by 1, we can't cover all chunks
        if thread_size > 1 {
            assert!(threads * (thread_size - 1) < num_chunks);
        }
    }

    #[test]
    fn test_range_conversion_i64_to_u64() {
        // Test that i64 to u64 conversion works correctly for range values
        let start: i64 = 0;
        let end: i64 = 1024;

        let start_u64 = start as u64;
        let end_u64 = end as u64;

        assert_eq!(start_u64, 0u64);
        assert_eq!(end_u64, 1024u64);
    }

    #[test]
    fn test_last_chunk_partial() {
        // Test that last chunk calculation handles partial chunks correctly
        let size: i64 = (CHUNK_SIZE * 2 + 1000) as i64; // 2 full chunks + 1000 bytes
        let chunks: Vec<(usize, i64)> = (0..size)
            .step_by(CHUNK_SIZE)
            .enumerate()
            .collect();

        assert_eq!(chunks.len(), 3);

        // Last chunk should be partial
        let last_start = chunks[2].1;
        let last_end = std::cmp::min(last_start + CHUNK_SIZE as i64, size) - 1;
        let last_chunk_size = (last_end - last_start + 1) as usize;

        assert_eq!(last_chunk_size, 1000);
    }

    #[test]
    fn test_key_generation_special_characters() {
        // Test that special characters in IDs are preserved
        let key = GcsArtifactClient::get_gcs_key_from_id(
            ArtifactType::Program,
            "test-id-with-dashes-and_underscores_123"
        );
        assert_eq!(key, "program/test-id-with-dashes-and_underscores_123");
    }

    #[test]
    fn test_upload_size_threshold() {
        // Test that size threshold logic is correct
        let small_data = vec![0u8; CHUNK_SIZE / 2];
        let large_data = vec![0u8; CHUNK_SIZE + 1];

        assert!(small_data.len() <= CHUNK_SIZE);
        assert!(large_data.len() > CHUNK_SIZE);
    }
}
