// Core footer, Option 1 of 3: the CURRENT footer (nested array-of-structs).
//
// This is the baseline. It restricts the standard Parquet footer to the core
// placement-info domain (schema + row groups + column chunk locations/sizes) but
// KEEPS every duplicate / derivable / deprecated field that the standard footer
// carries, so it shows what that redundancy costs versus Options 2 and 3.
//
// Field ids and optional/required match the standard Parquet parquet.thrift exactly
// for every field present. Fields that carry different information (key/value
// metadata, created_by, column_orders, encryption, sorting_columns, statistics,
// size/geospatial statistics, bloom filter, page-index offsets, crypto) are omitted
// so all three options encode the same information. Gaps in field ids are exactly
// those omitted fields.

include "parquet.thrift"

namespace cpp footer.core.current

struct SchemaElement {
  1: optional parquet.Type type;
  2: optional i32 type_length;
  3: optional parquet.FieldRepetitionType repetition_type;
  4: required string name;
  5: optional i32 num_children;
  6: optional parquet.ConvertedType converted_type;  // DUP: superseded by logicalType
  7: optional i32 scale;                              // DUP: superseded by DecimalType
  8: optional i32 precision;                          // DUP: superseded by DecimalType
  9: optional i32 field_id;
  10: optional parquet.LogicalType logicalType;
}

struct ColumnMetaData {
  1: required parquet.Type type;                  // DUP: also carried in SchemaElement.type
  2: required list<parquet.Encoding> encodings;   // DUP: also in every page header
  3: required list<string> path_in_schema;        // DUP: derivable from schema tree
  4: required parquet.CompressionCodec codec;
  5: required i64 num_values;
  6: required i64 total_uncompressed_size;
  7: required i64 total_compressed_size;
  9: required i64 data_page_offset;
  10: optional i64 index_page_offset;             // unused in practice
  11: optional i64 dictionary_page_offset;
  12: optional parquet.Statistics statistics;     // per-column stats (stats-generator experiment)
}

struct ColumnChunk {
  1: optional string file_path;                   // deprecated, never set
  2: required i64 file_offset = 0;                 // deprecated by spec
  3: optional ColumnMetaData meta_data;           // optional in spec, required in practice
}

struct RowGroup {
  1: required list<ColumnChunk> columns;
  2: required i64 total_byte_size;                 // DUP: sum of chunk total_uncompressed_size
  3: required i64 num_rows;
  5: optional i64 file_offset;                     // DUP: first chunk page offset
  6: optional i64 total_compressed_size;           // DUP: sum of chunk total_compressed_size
  7: optional i16 ordinal;                         // DUP: index in row_groups
}

struct FileMetaData {
  1: required i32 version;
  2: required list<SchemaElement> schema;
  3: required i64 num_rows;                        // DUP: sum of RowGroup.num_rows
  4: required list<RowGroup> row_groups;
}
