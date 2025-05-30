use arrow::array::RecordBatch;

/// Nicely formats any RecordBatch as a human-readable table.
///
/// For very wide but small batches (i.e. lots of columns, few rows), try using the `transposed` flag.
#[must_use]
pub fn format_batch(batch: &RecordBatch, transposed: bool) -> String {
    re_format_arrow::format_record_batch_opts(
        batch,
        &re_format_arrow::RecordBatchFormatOpts {
            transposed,
            width: None,
            include_metadata: true,
            include_column_metadata: false,
        },
    )
    .to_string()
}
