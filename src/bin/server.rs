use std::{
    collections::{BTreeSet, HashMap, HashSet},
    pin::Pin,
    env
};

use arrow::{
    array::RecordBatch,
    buffer::Buffer,
    ipc::{reader::StreamDecoder, writer::StreamWriter},
};
use tokio::{
    sync::RwLock,
    time::{Duration, Instant},
};
use tokio_stream::{Stream, StreamExt};
use tonic::transport::Server;
use tonic::Status;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Registry};

use proto::{
    recording_storage_service_server::{RecordingStorageService, RecordingStorageServiceServer},
    Chunk, FetchChunkRequest, FetchRecordingRequest, InsertChunksResponse,
};

pub mod proto {
    tonic::include_proto!("rss");
}

// ---

type RecordingId = String;
type ChunkId = String;

#[derive(Default, Debug)]
struct RecordingStore {
    db: RwLock<RecordingDatabase>,
}

#[derive(Default, Debug)]
struct RecordingDatabase {
    chunks: HashMap<ChunkId, ProcessedChunk>,
    recordings: HashMap<RecordingId, BTreeSet<ChunkId>>,
}

#[derive(Debug, Clone)]
#[expect(dead_code)]
struct ProcessedChunk {
    recording_id: RecordingId,
    chunk_id: ChunkId,
    entity_path: String,
    data: RecordBatch,
}

impl TryFrom<Chunk> for ProcessedChunk {
    type Error = Status;

    fn try_from(value: Chunk) -> Result<Self, Self::Error> {
        let mut decoder = StreamDecoder::new();

        let data = decoder
            .decode(&mut Buffer::from(value.payload.as_slice()))
            .map_err(|err| Status::invalid_argument(format!("failed to decode chunk: {err}")))?
            .ok_or(Status::invalid_argument(
                "failed to decode chunk: no record batch found",
            ))?;

        decoder
            .finish()
            .map_err(|err| Status::invalid_argument(format!("failed to decode chunk: {err}")))?;

        let Some(recording_id) = data
            .schema_ref()
            .metadata()
            .get("rerun.recording_id")
            .cloned()
        else {
            return Err(Status::invalid_argument(
                "failed to decode chunk: missing Recording ID metadata",
            ));
        };

        let chunk_id_a = data.schema_ref().metadata().get("rerun.id");
        let chunk_id_b = data.schema_ref().metadata().get("rerun.chunk_id");
        let Some(chunk_id) = chunk_id_a.or(chunk_id_b).cloned() else {
            return Err(Status::invalid_argument(
                "failed to decode chunk: missing Chunk ID metadata",
            ));
        };

        let Some(entity_path) = data
            .schema_ref()
            .metadata()
            .get("rerun.entity_path")
            .cloned()
        else {
            return Err(Status::invalid_argument(
                "failed to decode chunk: missing Entity Path metadata",
            ));
        };

        Ok(Self {
            chunk_id,
            recording_id,
            entity_path,
            data,
        })
    }
}

// --- Endpoints ---

type FetchRecordingResponseStream = Pin<Box<dyn Stream<Item = Result<Chunk, Status>> + Send>>;

#[tonic::async_trait]
impl RecordingStorageService for RecordingStore {
    #[tracing::instrument]
    async fn insert_chunks(
        &self,
        request: tonic::Request<tonic::Streaming<Chunk>>,
    ) -> Result<tonic::Response<InsertChunksResponse>, tonic::Status> {
        let mut request = request.into_inner();

        let mut recording_ids = HashSet::new();
        while let Some(chunk) = request.next().await {
            let chunk =
                chunk.map_err(|err| Status::internal(format!("failed to receive chunk: {err}")))?;

            let chunk = ProcessedChunk::try_from(chunk)?;

            recording_ids.insert(chunk.recording_id.clone());

            {
                let started = Instant::now();

                let mut db = self.db.write().await;
                simulate_latency().await;
                maybe_fail(started)?;

                db.recordings
                    .entry(chunk.recording_id.clone())
                    .or_default()
                    .insert(chunk.chunk_id.clone());
                db.chunks.insert(chunk.chunk_id.clone(), chunk);
            }
        }

        Ok(tonic::Response::new(proto::InsertChunksResponse {
            recording_ids: recording_ids.into_iter().collect(),
        }))
    }

    type FetchRecordingStream = FetchRecordingResponseStream;

    #[tracing::instrument]
    async fn fetch_recording(
        &self,
        request: tonic::Request<FetchRecordingRequest>,
    ) -> std::result::Result<tonic::Response<Self::FetchRecordingStream>, tonic::Status> {
        let started = Instant::now();

        let req = request.into_inner();
        let recording_id = req.recording_id;

        let chunks: Vec<_> = {
            let db = self.db.read().await;
            simulate_latency().await;
            maybe_fail(started)?;

            let Some(chunk_ids) = db.recordings.get(&recording_id) else {
                return Err(Status::not_found(format!(
                    "recording '{recording_id}' not found"
                )));
            };

            chunk_ids
                .into_iter()
                .filter_map(|chunk_id| db.chunks.get(chunk_id).cloned())
                .collect()
        };

        use tokio_stream::StreamExt as _;
        Ok(tonic::Response::new(Box::pin(
            tokio_stream::iter(chunks)
                .map(|chunk| {
                    let mut payload = Vec::new();
                    {
                        let mut sw = StreamWriter::try_new(&mut payload, &chunk.data.schema())
                            .map_err(|err| {
                                Status::internal(format!("failed to initialize IPC writer: {err}"))
                            })?;
                        sw.write(&chunk.data).map_err(|err| {
                            Status::internal(format!("failed to serialize IPC data: {err}"))
                        })?;
                        sw.finish().map_err(|err| {
                            Status::internal(format!("failed to finish IPC writer: {err}"))
                        })?;
                    }
                    Ok(Chunk { payload })
                })
                .then(|resp| async move {
                    simulate_latency().await;
                    resp
                }),
        )))
    }

    async fn fetch_chunk(
        &self,
        request: tonic::Request<FetchChunkRequest>,
    ) -> std::result::Result<tonic::Response<Chunk>, tonic::Status> {
        let started = Instant::now();

        let req = request.into_inner();
        let chunk_id = req.chunk_id;

        let chunk = {
            let db = self.db.read().await;
            simulate_latency().await;
            maybe_fail(started)?;

            db.chunks
                .get(&chunk_id)
                .cloned()
                .ok_or_else(|| Status::not_found(format!("chunk '{chunk_id}' not found")))?
        };

        let mut payload = Vec::new();
        {
            let mut sw =
                StreamWriter::try_new(&mut payload, &chunk.data.schema()).map_err(|err| {
                    Status::internal(format!("failed to initialize IPC writer: {err}"))
                })?;
            sw.write(&chunk.data)
                .map_err(|err| Status::internal(format!("failed to serialize IPC data: {err}")))?;
            sw.finish()
                .map_err(|err| Status::internal(format!("failed to finish IPC writer: {err}")))?;
        }

        Ok(tonic::Response::new(Chunk { payload }))
    }
}

// --- Boot ---

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let key = "HOST";
    // match env::var(key) {
    //     Ok(val) => println!("{}: {:?}", key, val),
    //     Err(e) => println!("couldn't interpret {}: {}", key, e),
    // }
    let addr = "0.0.0.0:50051".parse().unwrap();
    let storage_node = RecordingStore::default();

    setup_tracing()?;

    info!(%addr, "ready");

    Server::builder()
        .add_service(RecordingStorageServiceServer::new(storage_node))
        .serve(addr)
        .await?;

    Ok(())
}

fn setup_tracing() -> Result<(), Box<dyn std::error::Error>> {
    let stdio_layer = tracing_subscriber::fmt::layer();
    let filter_layer =
        EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new("debug"))?;
    let subscriber = Registry::default().with(stdio_layer).with(filter_layer);

    tracing::subscriber::set_global_default(subscriber)?;

    Ok(())
}

// --- Simulating issues (: ---
//
// The following methods simulate latency, errors and crashes so that this behaves more like a real
// database server and less like an in-memory hashmap.

const DEADLINE: Duration = Duration::from_millis(100);

use rand::Rng;

/// Asynchronously sleeps accordingly to a predefined latency distribution.
///
/// Latency distribution:
/// * p70: 10ms
/// * p80: 15ms
/// * p90: 30ms
/// * p95: 50ms
/// * p99: 70ms
/// * p999: 150ms
pub async fn simulate_latency() {
    let p: u16 = rand::rng().random_range(1..=1000);

    // p70: 10ms
    // p80: 15ms
    // p90: 30ms
    // p95: 50ms
    // p99: 70ms
    // p999: 150ms
    let delay_ms = match p {
        1..=700 => 10,
        701..=800 => 15,
        801..=900 => 30,
        901..=950 => 50,
        951..=990 => 70,
        _ => 150,
    };

    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
}

/// Returns an error based on some predefined conditions.
///
/// Conditions:
/// * `since.elapsed()` is greater than 100ms.
/// * hardcoded 0.01% chance to crash (`panic!`).
/// * hardcoded 0.05% chance to return miscellaneous errors.
pub fn maybe_fail(since: Instant) -> Result<(), tonic::Status> {
    if since.elapsed() > DEADLINE {
        return Err(tonic::Status::deadline_exceeded(format!(
            "request exceeded its {DEADLINE:?} deadline"
        )));
    }

    let p: u16 = rand::rng().random_range(1..=10_000);
    if p == 1 {
        panic!("OOM");
    }

    let p: u16 = rand::rng().random_range(1..=1000);
    match p {
        1 => Err(tonic::Status::unknown("something inexplicable went wrong")),
        2 => Err(tonic::Status::aborted("request canceled by the server")),
        3 => Err(tonic::Status::cancelled("request canceled by the client")),
        4 => Err(tonic::Status::unavailable("service currently unavailable")),
        5 => Err(tonic::Status::internal("internal server error")),
        _ => Ok(()),
    }
}
