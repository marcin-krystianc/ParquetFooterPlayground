// Core footer, Option 3 of 3: the struct-of-arrays (SoA) matrix representation.
//
// Carries the same information as the other options, but grouped per field instead of
// per element: the schema is a set of parallel arrays, and the column chunks are a set
// of parallel arrays (one array per attribute) rather than a list of structs.
//
// The chunk arrays are column-major with length num_leaf_columns * num_row_groups (all
// row groups of column 0, then column 1, ...), and row_groups.num_rows.size() is the
// number of row groups (so num_leaf_columns = data_page_offsets.size() /
// num_row_groups, and the chunk for (column c, row group g) is at index
// c * num_row_groups + g in every chunk array).
//
// Uses byte-tagged enums for the simple fields but preserves the original Parquet
// LogicalType union. SoA pays one field header per array; the array-of-structs layout
// pays one field header per field per element. This isolates the SoA-vs-AoS axis.

include "parquet.thrift"

namespace cpp footer.core.soa

// Parallel arrays, one entry per schema element (pre-order flattened tree).
struct SchemaMatrix {
  1: required list<string> names;
  2: required list<byte> physical_types;
  3: required list<byte> repetition_types;
  4: required list<parquet.LogicalType> logical_types;
  5: required list<i32> type_lengths;
  6: required list<i32> num_children;
  7: required list<i32> field_ids;
}

struct RowGroupMatrix {
  1: required list<i64> num_rows;  // one per row group
}

// Parallel arrays, column-major: num_leaf_columns * num_row_groups entries each.
struct ColumnChunkMatrix {
  1: required list<i64> data_page_offsets;
  2: required list<i64> dictionary_page_offsets;
  3: required list<i64> total_compressed_sizes;
  4: required list<i64> total_uncompressed_sizes;
  5: required list<i64> num_values;
  6: required list<byte> codecs;
  // per-chunk statistics as parallel arrays (stats-generator experiment)
  7: optional list<binary> stat_min_values;
  8: optional list<binary> stat_max_values;
  9: optional list<i64> stat_null_counts;
}

struct FileMetaData {
  1: required i32 layout_version = 1;
  2: optional SchemaMatrix schema;
  3: required RowGroupMatrix row_groups;
  4: required ColumnChunkMatrix chunks;
}
