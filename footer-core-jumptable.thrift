// Core footer, Option 2 of 3: the current footer with duplicate/unused fields
// removed, plus a jump table for selective decode.
//
// Same nested array-of-structs shape and same standard Parquet enum types as Option
// 1, but every duplicate / derivable / deprecated field is dropped. The only addition
// is column_metadata_offsets: a flat array holding the byte offset of each column's
// ColumnMetaData within the serialized footer, so a reader can seek directly to a
// column chunk's metadata without parsing everything before it.

include "parquet.thrift"

namespace cpp footer.core.jumptable

struct SchemaElement {
  1: required string name;
  2: optional parquet.Type type;
  3: optional i32 type_length;
  4: optional parquet.FieldRepetitionType repetition_type;
  5: optional i32 num_children;
  6: optional i32 field_id;
  7: optional parquet.LogicalType logicalType;
}

struct ColumnMetaData {
  1: required parquet.CompressionCodec codec;
  2: required i64 num_values;
  3: required i64 total_uncompressed_size;
  4: required i64 total_compressed_size;
  5: required i64 data_page_offset;
  6: optional i64 dictionary_page_offset;
  7: optional parquet.Statistics statistics;      // per-column stats (stats-generator experiment)
}

struct RowGroup {
  1: required list<ColumnMetaData> columns;
  2: required i64 num_rows;
}

struct FileMetaData {
  1: required i32 version;
  2: required list<SchemaElement> schema;
  3: required list<RowGroup> row_groups;

  // Jump table: byte offset of each column's ColumnMetaData within the serialized
  // footer, row-group-major (length = num_row_groups * num_leaf_columns).
  4: required list<i32> column_metadata_offsets;
}
