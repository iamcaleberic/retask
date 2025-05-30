use std::sync::Arc;

use anyhow::Context as _;
use arrow::{
    array::RecordBatch,
    buffer::Buffer,
    ipc::{reader::StreamDecoder, writer::StreamWriter},
};
use proto::{
    recording_storage_service_client::RecordingStorageServiceClient, Chunk, FetchChunkRequest,
    FetchRecordingRequest,
};

pub mod proto {
    tonic::include_proto!("rss");
}

// ---

use clap::{Parser, Subcommand};
use tokio_stream::StreamExt;

/// This client handles communication with a Recording Storage Service at specified address.
#[derive(Debug, Parser)]
struct Args {
    /// Recording Storage Service to connect to.
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    addr: String,

    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Loads a Rerun Recording (.rrd files) into the Recording Storage Service.
    Load {
        /// Path to a Rerun Recording (.rrd file) to be loaded into the Recording Storage Service.
        path_to_rrd: String,
    },

    /// Fetches the contents of a Rerun Recording (i.e. its Chunks) from the Recording Storage Service.
    FetchRecording {
        /// The ID of the Recording to fetch.
        ///
        /// Recording IDs are globally unique.
        recording_id: String,
    },

    /// Fetches a specific Chunk from the Recording Storage Service.
    FetchChunk {
        /// The ID of the Chunk to fetch.
        ///
        /// Chunk IDs are globally unique.
        chunk_id: String,
    },
}

// ---

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let conn = tonic::transport::Endpoint::new(args.addr)?
        .connect()
        .await?;
    let mut client = RecordingStorageServiceClient::new(conn);

    match args.cmd {
        Commands::Load { path_to_rrd } => {
            let batches = load_rrd(path_to_rrd)?;
            let batches = tokio_stream::iter(batches)
                .map(|batch| {
                    let mut payload = Vec::new();
                    {
                        let mut sw = StreamWriter::try_new(&mut payload, &batch.schema())?;
                        sw.write(&batch)?;
                        sw.finish()?;
                    }

                    Ok::<_, anyhow::Error>(Chunk { payload })
                })
                .filter_map(|res| res.ok());

            let resp = client.insert_chunks(batches).await?.into_inner();
            println!("updated recordings: {:?}", resp.recording_ids);
        }

        Commands::FetchRecording { recording_id } => {
            let mut resp = client
                .fetch_recording(FetchRecordingRequest { recording_id })
                .await?
                .into_inner();

            while let Some(resp) = resp.try_next().await? {
                let mut decoder = StreamDecoder::new();
                let batch = decoder.decode(&mut Buffer::from(resp.payload.as_slice()))?;
                decoder.finish()?;

                if let Some(batch) = batch {
                    let transposed = false;
                    println!("{}", rss::format_batch(&batch, transposed));
                }
            }
        }

        Commands::FetchChunk { chunk_id } => {
            let resp = client
                .fetch_chunk(FetchChunkRequest { chunk_id })
                .await?
                .into_inner();

            let mut decoder = StreamDecoder::new();
            let batch = decoder.decode(&mut Buffer::from(resp.payload.as_slice()))?;
            decoder.finish()?;

            if let Some(batch) = batch {
                let transposed = false;
                println!("{}", rss::format_batch(&batch, transposed));
            }
        }
    }

    Ok(())
}

// ---

/// Loads a Rerun Recording (.rrd file) as an iterator of `RecordBatch`es.
///
/// Note that this injects the `recording_id` into the Chunk metadata.
fn load_rrd(path_to_rrd: String) -> anyhow::Result<impl Iterator<Item = RecordBatch>> {
    let rrd_file = std::fs::File::open(&path_to_rrd)
        .with_context(|| format!("couldn't open {path_to_rrd:?}"))?;

    let decoder = re_log_encoding::decoder::Decoder::new(rrd_file)
        .with_context(|| format!("couldn't decode {path_to_rrd:?}"))?;

    let chunks = decoder
        .into_iter()
        .map(|res| {
            let msg = res.with_context(|| format!("couldn't decode message {path_to_rrd:?} "))?;
            match msg {
                // Only process recording messages
                re_log_types::LogMsg::ArrowMsg(
                    re_log_types::StoreId {
                        kind: re_log_types::StoreKind::Recording,
                        id: recording_id,
                    },
                    msg,
                ) => {
                    let mut schema = Arc::unwrap_or_clone(msg.batch.schema());
                    schema
                        .metadata
                        .retain(|k, _| ["rerun.id", "rerun.entity_path"].contains(&k.as_str()));
                    schema
                        .metadata
                        .insert("rerun.recording_id".to_owned(), recording_id.to_string());

                    let batch =
                        RecordBatch::try_new(Arc::new(schema), msg.batch.columns().to_vec())
                            .with_context(|| format!("couldn't decode chunk in {path_to_rrd:?}"))?;

                    Ok(Some(batch))
                }

                // ignore other messages
                _ => Ok(None),
            }
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(chunks.into_iter().flatten().map(|chunk| chunk))
}
