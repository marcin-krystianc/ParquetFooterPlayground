"""# Parquet footer benchmarks (2026)

Parquet ([file format docs](https://parquet.apache.org/docs/file-format/)) stores its metadata
footer as a nested array of Thrift structs, which forces a reader to decode the whole footer even
when it only wants the byte offsets of a few columns. This benchmark measures the read cost of
alternative footer layouts and encodings, as input to designing a new footer format. Read cost is
the time to obtain `data_page_offset` and `total_compressed_size` for a projected subset of
columns.

Two sweeps compare footer encodings: `current`, `jumptable`, `soa`, and three FlatBuffers SoA
variants (`soa_fb`, `soa_fb_lz4`, `soa_fb_lz4_verified`). One sweep varies the number of columns,
the other the number of row groups. Both also measure a second category, FileMetaData producers,
over a real Parquet file: `pyarrow`, which parses the whole footer with no projection, and
G-Research PalletJack, which reads only the projected row groups and columns through an index.

The FlatBuffers SoA footer is read by a native C++ reader (`native_build/shim.cpp`) via three
angles: soa_fb (uncompressed, unverified), soa_fb_lz4 (LZ4_FRAME-compressed via the pyarrow
codec, decompressed in C++ with liblz4), and soa_fb_lz4_verified (adds the generated
FlatBuffers verifier before the scan). soa_fb_lz4 vs soa_fb isolates the compression cost;
soa_fb_lz4_verified vs soa_fb_lz4 isolates the verification cost. The Python flatbuffers
package + flatc `--python` accessors (`flatbuffers_gen/`) are used only to BUILD the footer;
flatc `--cpp` generates the reader's header at build time.

## Setup
Build the native extension once before running:

```bash
python setup.py build_ext --inplace
```

That requires the thrift compiler, libthrift, a C++ compiler, Cython, and (for flatbuffers)
flatc + libflatbuffers-dev + liblz4-dev, plus the flatbuffers package.

Every run recomputes all timings from scratch. Run as a script to render the plots and tables:

```bash
python parquet_footer_benchmarks_2026.py
```

PS (Ubuntu + uv):

```bash
apt-get install libflatbuffers-dev liblz4-dev thrift-compiler libthrift-dev g++
uv sync
uv run --with setuptools --with wheel --with cython python setup.py build_ext --inplace
uv run python parquet_footer_benchmarks_2026.py
```
"""

import gc, random, sys, time
from dataclasses import dataclass, field
from pathlib import Path

import humanize
import thriftpy2
import flatbuffers
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import matplotlib.pyplot as plt
import palletjack as pj
from thriftpy2.protocol import TCompactProtocol, TCompactProtocolFactory
from thriftpy2.transport import TMemoryBuffer
from thriftpy2.utils import serialize, deserialize

HERE = Path(__file__).resolve().parent
COMPACT_PROTOCOL = TCompactProtocolFactory()   # Thrift compact protocol, used to (de)serialize footers


def load_thrift(file, module):
    """Load a .thrift IDL as an importable Python module (cached in sys.modules)."""
    if module in sys.modules:
        return sys.modules[module]
    return thriftpy2.load(str(HERE / file), module_name=module, include_dirs=[str(HERE)])


# The three footer IDLs plus the full Parquet IDL they share (reached as `current_thrift.parquet`).
current_thrift = load_thrift("footer-core-current.thrift", "nb_current_thrift")
jumptable_thrift = load_thrift("footer-core-jumptable.thrift", "nb_jumptable_thrift")
soa_thrift = load_thrift("footer-core-soa.thrift", "nb_soa_thrift")
parquet_thrift = current_thrift.parquet

# Category A: offset/size scanners. All six encode the SAME synthetic footer contents (one Meta),
# so their read results must match; only the encoding/read strategy differs.
FORMATS = ["current", "jumptable", "soa", "soa_fb", "soa_fb_lz4", "soa_fb_lz4_verified"]
# Category B: pyarrow + palletjack read a REAL Parquet file and return a pyarrow FileMetaData
# object graph (much heavier per chunk than a sum); they are compared against each other.
DISPLAY_FORMATS = FORMATS + ["pyarrow", "palletjack"]

PROJECTIONS = [p / 10 for p in range(1, 10, 4)]              # fraction of columns read: 10%, 50%, 90%
COLUMN_COUNTS = [4, 8, 16, 32, 64, 128, 256, 512, 1024]     # columns sweep (row groups fixed)
ROW_GROUP_COUNTS = [4, 8, 16, 32, 64, 128, 256, 512, 1024]  # row-groups sweep (columns fixed)
ROW_GROUPS_FIXED = 100   # row groups held constant during the columns sweep
COLUMNS_FIXED = 100      # leaf columns held constant during the row-groups sweep
ROWS_PER_GROUP = 32_000
REPEATS = 7             # runs per measurement; the fastest (min) is reported (see fastest_ms)
PJ_CACHE = HERE / "palletjack_cache"   # cached real Parquet files + PalletJack indexes, keyed by shape

# ---------------------------------------------------------------------------
# Metadata model
# ---------------------------------------------------------------------------

# Short aliases for the Parquet enum values used when building schemas.
OPT = parquet_thrift.FieldRepetitionType.OPTIONAL
I32, I64, FLOAT, DOUBLE, BA = parquet_thrift.Type.INT32, parquet_thrift.Type.INT64, parquet_thrift.Type.FLOAT, parquet_thrift.Type.DOUBLE, parquet_thrift.Type.BYTE_ARRAY


@dataclass
class Node:
    """One schema element: a group node, or a primitive (leaf) column."""
    name: str
    typ: int | None = None          # parquet physical type; None for group nodes
    rep: int = OPT                  # repetition type
    children: int = 0               # number of child nodes (0 for a leaf)
    logical: object | None = None   # parquet LogicalType, or None
    converted: int | None = None    # legacy ConvertedType, or None
    type_length: int | None = None
    scale: int | None = None
    precision: int | None = None
    field_id: int | None = None


@dataclass
class Leaf:
    """A primitive column plus the parameters used to model its per-chunk sizes."""
    path: list[str]                 # dotted path from root to this column
    typ: int                        # parquet physical type
    kind: str                       # human label: "integer" / "string" / "double"
    width: float                    # modelled bytes per value (drives uncompressed size)
    ratio: float                    # modelled compressed / uncompressed ratio
    values_per_row: float = 1.0
    dictionary: bool = False        # dictionary-encoded (adds a dictionary page)
    encodings: list[int] = field(default_factory=list)


@dataclass
class Meta:
    """Everything a synthetic footer needs: a schema plus one value per (column, row group) chunk.

    The per-chunk lists are indexed column-major: chunk index = column * row_groups + row_group
    (see idx()). Every footer encoder (build_*) reads the same Meta, so all encodings carry
    identical information and their read results are equal.
    """
    columns: int
    row_groups: int
    top_fields: int                   # number of top-level schema children
    schema: list[Node]
    leaves: list[Leaf]
    rows: list[int]                   # number of rows in each row group
    num_values: list[int]             # per chunk
    uncompressed_sizes: list[int]     # per chunk, bytes
    compressed_sizes: list[int]       # per chunk, bytes
    data_page_offsets: list[int]      # per chunk, absolute byte offset in the hypothetical file
    dict_page_offsets: list[int]      # per chunk, -1 when the column is not dictionary-encoded
    codecs: list[int]                 # per chunk compression codec

    @property
    def chunks(self):
        return self.columns * self.row_groups

    @property
    def row_count(self):
        return sum(self.rows)

    def idx(self, column, row_group):
        """Column-major index of one chunk within the per-chunk lists."""
        return column * self.row_groups + row_group


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

def lt_string():
    return parquet_thrift.LogicalType(STRING=parquet_thrift.StringType())

def lt_int(bits):
    return parquet_thrift.LogicalType(INTEGER=parquet_thrift.IntType(bitWidth=bits, isSigned=True))

def lt_empty():
    return parquet_thrift.LogicalType()

def node(name, children, logical=None, converted=None):
    """A group (non-leaf) schema node with `children` child nodes."""
    return Node(name, children=children, logical=logical, converted=converted)

def prim(name, typ, **kw):
    """A primitive schema node of physical type `typ`."""
    return Node(name, typ=typ, **kw)

def encodings(leaf):
    """The encoding list a real writer would record for this column type."""
    if leaf.dictionary:
        return [parquet_thrift.Encoding.RLE_DICTIONARY, parquet_thrift.Encoding.PLAIN, parquet_thrift.Encoding.RLE]
    if leaf.typ in (FLOAT, DOUBLE):
        return [parquet_thrift.Encoding.BYTE_STREAM_SPLIT, parquet_thrift.Encoding.PLAIN, parquet_thrift.Encoding.RLE]
    if leaf.typ in (I32, I64) and leaf.kind != "decimal":
        return [parquet_thrift.Encoding.DELTA_BINARY_PACKED, parquet_thrift.Encoding.PLAIN, parquet_thrift.Encoding.RLE]
    if leaf.typ == BA:
        return [parquet_thrift.Encoding.DELTA_LENGTH_BYTE_ARRAY, parquet_thrift.Encoding.PLAIN, parquet_thrift.Encoding.RLE]
    return [parquet_thrift.Encoding.PLAIN, parquet_thrift.Encoding.RLE]

def leaf(path, typ, kind, width, ratio, **kw):
    """Build a Leaf and fill in its encodings."""
    out = Leaf(list(path), typ, kind, width, ratio, **kw)
    out.encodings = encodings(out)
    return out

def opt_offset(x):
    """A dictionary page offset: the int itself, or None (field absent) when it is -1."""
    return int(x) if x >= 0 else None

def thrift_schema(mod, meta, legacy=False):
    """The flat SchemaElement list a Thrift footer stores: a root node followed by every field."""
    out = [mod.SchemaElement(name="root", num_children=meta.top_fields)]
    for n in meta.schema:
        kw = dict(type=n.typ, repetition_type=n.rep, name=n.name,
                  num_children=n.children or None, field_id=n.field_id, logicalType=n.logical)
        if n.type_length is not None:
            kw["type_length"] = n.type_length
        if legacy:   # the "current" footer also keeps the deprecated/duplicate fields
            kw.update(converted_type=n.converted, scale=n.scale, precision=n.precision)
        out.append(mod.SchemaElement(**kw))
    return out


# ---------------------------------------------------------------------------
# Footer builders — each turns one Meta into serialized footer bytes
# ---------------------------------------------------------------------------

def build_current(meta):
    """Current Parquet layout: nested array-of-structs (row groups -> columns -> ColumnMetaData)."""
    row_groups = []
    for rg in range(meta.row_groups):
        columns = []
        for col, leaf_def in enumerate(meta.leaves):
            i = meta.idx(col, rg)
            columns.append(current_thrift.ColumnChunk(
                file_offset=0,
                meta_data=current_thrift.ColumnMetaData(
                    type=leaf_def.typ,
                    encodings=leaf_def.encodings,
                    path_in_schema=leaf_def.path,
                    codec=meta.codecs[i],
                    num_values=meta.num_values[i],
                    total_uncompressed_size=meta.uncompressed_sizes[i],
                    total_compressed_size=meta.compressed_sizes[i],
                    data_page_offset=meta.data_page_offsets[i],
                    dictionary_page_offset=opt_offset(meta.dict_page_offsets[i]),
                ),
            ))
        row_groups.append(current_thrift.RowGroup(
            columns=columns,
            total_byte_size=sum(meta.uncompressed_sizes[meta.idx(col, rg)] for col in range(meta.columns)),
            num_rows=meta.rows[rg],
            total_compressed_size=sum(meta.compressed_sizes[meta.idx(col, rg)] for col in range(meta.columns)),
        ))
    buf = serialize(current_thrift.FileMetaData(version=2, schema=thrift_schema(current_thrift, meta, legacy=True),
                                     num_rows=meta.row_count, row_groups=row_groups), COMPACT_PROTOCOL)
    return dict(fmt="current", buf=buf, num_columns=meta.columns, num_row_groups=meta.row_groups, size=len(buf))


def build_jumptable(meta):
    """Header holds a flat table of byte offsets; each ColumnMetaData is serialized separately into
    the body, so a reader can seek straight to a selected chunk instead of decoding everything."""
    chunk_offsets = [0] * meta.chunks   # row-group-major: chunk_offsets[rg * columns + col]
    body = bytearray()
    for rg in range(meta.row_groups):
        for col in range(meta.columns):
            i = meta.idx(col, rg)
            chunk_offsets[rg * meta.columns + col] = len(body)
            body += serialize(jumptable_thrift.ColumnMetaData(
                codec=meta.codecs[i],
                num_values=meta.num_values[i],
                total_uncompressed_size=meta.uncompressed_sizes[i],
                total_compressed_size=meta.compressed_sizes[i],
                data_page_offset=meta.data_page_offsets[i],
                dictionary_page_offset=opt_offset(meta.dict_page_offsets[i]),
            ), COMPACT_PROTOCOL)
    header = serialize(jumptable_thrift.FileMetaData(
        version=2,
        schema=thrift_schema(jumptable_thrift, meta),
        row_groups=[jumptable_thrift.RowGroup(columns=[], num_rows=r) for r in meta.rows],
        column_metadata_offsets=chunk_offsets,
    ), COMPACT_PROTOCOL)
    buf = header + bytes(body)
    return dict(fmt="jumptable", buf=buf, header_len=len(header),
                num_columns=meta.columns, num_row_groups=meta.row_groups, size=len(buf))


def build_soa(meta):
    """Struct-of-arrays footer: one parallel Thrift list per field, across all chunks."""
    schema_matrix = soa_thrift.SchemaMatrix(
        names=["root"] + [n.name for n in meta.schema],
        physical_types=[0] + [n.typ or 0 for n in meta.schema],
        repetition_types=[0] + [n.rep for n in meta.schema],
        logical_types=[lt_empty()] + [n.logical or lt_empty() for n in meta.schema],
        type_lengths=[0] + [n.type_length or 0 for n in meta.schema],
        num_children=[meta.top_fields] + [n.children or 0 for n in meta.schema],
        field_ids=[0] + [n.field_id or 0 for n in meta.schema],
    )
    chunk_matrix = soa_thrift.ColumnChunkMatrix(
        data_page_offsets=meta.data_page_offsets,
        dictionary_page_offsets=meta.dict_page_offsets,
        total_compressed_sizes=meta.compressed_sizes,
        total_uncompressed_sizes=meta.uncompressed_sizes,
        num_values=meta.num_values,
        codecs=meta.codecs,
    )
    buf = serialize(soa_thrift.FileMetaData(layout_version=1, schema=schema_matrix,
                                     row_groups=soa_thrift.RowGroupMatrix(num_rows=meta.rows),
                                     chunks=chunk_matrix), COMPACT_PROTOCOL)
    return dict(fmt="soa", buf=buf, num_columns=meta.columns, num_row_groups=meta.row_groups, size=len(buf))


# ---------------------------------------------------------------------------
# FlatBuffers struct-of-arrays footer
# ---------------------------------------------------------------------------
# Same content as build_soa, encoded as one FlatBuffers FileMetaData (see
# footer-core-flatbuffers.fbs). FlatBuffers vectors are directly addressable, so the reader
# indexes only the two vectors it needs at only the selected chunks (no decode pass).
# The flatc-generated Python accessors live in flatbuffers_gen/ and are produced by setup.py
# (flatc --python); load_fb() just imports them.

def lt_ipc_bytes(lt):
    """A LogicalType union serialized to compact-thrift bytes (FlatBuffers has no native union)."""
    return serialize(lt, COMPACT_PROTOCOL)

_fb = None

def load_fb():
    """Import the flatc-generated FlatBuffers accessors (generated into flatbuffers_gen/ by setup.py)."""
    global _fb
    if _fb is None:
        import types
        gen = HERE / "flatbuffers_gen"
        if str(gen) not in sys.path:
            sys.path.insert(0, str(gen))
        from FooterCoreFb import FileMetaData, SchemaMatrix, RowGroupMatrix, ColumnChunkMatrix, ByteBlob
        _fb = types.SimpleNamespace(FMD=FileMetaData, SM=SchemaMatrix, RGM=RowGroupMatrix,
                                    CCM=ColumnChunkMatrix, BB=ByteBlob)
    return _fb

# --- small helpers for building FlatBuffers vectors ---
def _fb_long_vec(builder, values):
    return builder.CreateNumpyVector(np.asarray(values, dtype=np.int64))

def _fb_int_vec(builder, values):
    return builder.CreateNumpyVector(np.asarray(values, dtype=np.int32))

def _fb_offset_vec(builder, offsets, start_vector):
    """Build a vector of already-built objects (strings or tables). FlatBuffers vectors are
    prepended in reverse so that index 0 ends up first."""
    start_vector(builder, len(offsets))
    for off in reversed(offsets):
        builder.PrependUOffsetTRelative(off)
    return builder.EndVector()

def _fb_blob_vec(builder, blobs, start_vector, ByteBlob):
    """Build a vector of ByteBlob tables, one per raw bytes object."""
    offsets = []
    for raw in blobs:
        data = builder.CreateByteVector(bytes(raw))
        ByteBlob.Start(builder)
        ByteBlob.AddData(builder, data)
        offsets.append(ByteBlob.End(builder))
    return _fb_offset_vec(builder, offsets, start_vector)


def _build_fb_raw(meta):
    """Build the (uncompressed) FlatBuffers SoA footer and return its bytes."""
    fb = load_fb()
    SM, RGM, CCM, BB, FMD = fb.SM, fb.RGM, fb.CCM, fb.BB, fb.FMD
    builder = flatbuffers.Builder(1024)

    # All leaf vectors must be created before the table that references them is opened
    # (FlatBuffers forbids building a nested object while a parent is in progress).
    names = ["root"] + [n.name for n in meta.schema]
    logical_blobs = [lt_ipc_bytes(lt_empty())] + [lt_ipc_bytes(n.logical or lt_empty()) for n in meta.schema]
    names_vec = _fb_offset_vec(builder, [builder.CreateString(s) for s in names], SM.StartNamesVector)
    physical_vec = _fb_int_vec(builder, [0] + [n.typ or 0 for n in meta.schema])
    repetition_vec = _fb_int_vec(builder, [0] + [n.rep for n in meta.schema])
    type_length_vec = _fb_int_vec(builder, [0] + [n.type_length or 0 for n in meta.schema])
    num_children_vec = _fb_int_vec(builder, [meta.top_fields] + [n.children or 0 for n in meta.schema])
    field_id_vec = _fb_int_vec(builder, [0] + [n.field_id or 0 for n in meta.schema])
    logical_vec = _fb_blob_vec(builder, logical_blobs, SM.StartLogicalTypesVector, BB)
    SM.Start(builder)
    SM.AddNames(builder, names_vec)
    SM.AddPhysicalTypes(builder, physical_vec)
    SM.AddRepetitionTypes(builder, repetition_vec)
    SM.AddLogicalTypes(builder, logical_vec)
    SM.AddTypeLengths(builder, type_length_vec)
    SM.AddNumChildren(builder, num_children_vec)
    SM.AddFieldIds(builder, field_id_vec)
    schema_off = SM.End(builder)

    num_rows_vec = _fb_long_vec(builder, meta.rows)
    RGM.Start(builder)
    RGM.AddNumRows(builder, num_rows_vec)
    row_groups_off = RGM.End(builder)

    data_page_vec = _fb_long_vec(builder, meta.data_page_offsets)
    dict_page_vec = _fb_long_vec(builder, meta.dict_page_offsets)
    compressed_vec = _fb_long_vec(builder, meta.compressed_sizes)
    uncompressed_vec = _fb_long_vec(builder, meta.uncompressed_sizes)
    num_values_vec = _fb_long_vec(builder, meta.num_values)
    codecs_vec = _fb_int_vec(builder, meta.codecs)
    CCM.Start(builder)
    CCM.AddDataPageOffsets(builder, data_page_vec)
    CCM.AddDictionaryPageOffsets(builder, dict_page_vec)
    CCM.AddTotalCompressedSizes(builder, compressed_vec)
    CCM.AddTotalUncompressedSizes(builder, uncompressed_vec)
    CCM.AddNumValues(builder, num_values_vec)
    CCM.AddCodecs(builder, codecs_vec)
    chunks_off = CCM.End(builder)

    FMD.Start(builder)
    FMD.AddLayoutVersion(builder, 1)
    FMD.AddSchema(builder, schema_off)
    FMD.AddRowGroups(builder, row_groups_off)
    FMD.AddChunks(builder, chunks_off)
    builder.Finish(FMD.End(builder))
    return bytes(builder.Output())


# The three FlatBuffers angles all read one of these two buffers with the native C++ reader
# (see read_soa_fb* below). raw_size is the uncompressed length, which the reader needs to size
# its decompress buffer; for the uncompressed footer raw_size == size.
def build_soa_fb(meta):
    """Uncompressed FlatBuffers footer (soa_fb)."""
    raw = _build_fb_raw(meta)
    return dict(fmt="soa_fb", buf=raw, num_columns=meta.columns, num_row_groups=meta.row_groups,
                size=len(raw), raw_size=len(raw))

def build_soa_fb_lz4(meta):
    """LZ4_FRAME-compressed FlatBuffers footer (soa_fb_lz4 / soa_fb_lz4_verified share this buffer)."""
    raw = _build_fb_raw(meta)
    buf = pa.compress(raw, codec="lz4", asbytes=True)
    return dict(fmt="soa_fb_lz4", buf=buf, num_columns=meta.columns, num_row_groups=meta.row_groups,
                size=len(buf), raw_size=len(raw))

def build_soa_fb_lz4_verified(meta):
    """Same compressed buffer as soa_fb_lz4; only the reader differs (it runs the verifier)."""
    return {**build_soa_fb_lz4(meta), "fmt": "soa_fb_lz4_verified"}


BUILDERS = {
    "current": build_current,
    "jumptable": build_jumptable,
    "soa": build_soa,
    "soa_fb": build_soa_fb,
    "soa_fb_lz4": build_soa_fb_lz4,
    "soa_fb_lz4_verified": build_soa_fb_lz4_verified,
}

def build_all(meta):
    """Build every format's footer from one Meta, keyed by format name."""
    return {name: build(meta) for name, build in BUILDERS.items()}


# ---------------------------------------------------------------------------
# Synthetic footer generator
# ---------------------------------------------------------------------------

def set_field_ids(schema):
    """Assign each schema node a sequential field id (1-based)."""
    for i, n in enumerate(schema, 1):
        n.field_id = i


def flat_branch(n_columns):
    """A single group with n_columns primitive leaves, cycling int64 / string / double.

    Isolates the column-count axis: the schema is one level deep, so column count is the only
    thing that grows.
    """
    group = "cols"
    schema = [node(group, n_columns)]
    leaves = []
    for c in range(n_columns):
        kind = c % 3
        if kind == 0:
            name = f"id_{c:04d}"
            schema.append(prim(name, I64, logical=lt_int(64), converted=parquet_thrift.ConvertedType.INT_64))
            leaves.append(leaf([group, name], I64, "integer", width=8, ratio=0.28))
        elif kind == 1:
            name = f"name_{c:04d}"
            schema.append(prim(name, BA, logical=lt_string(), converted=parquet_thrift.ConvertedType.UTF8))
            leaves.append(leaf([group, name], BA, "string", width=24, ratio=0.24, dictionary=True))
        else:
            name = f"val_{c:04d}"
            schema.append(prim(name, DOUBLE))
            leaves.append(leaf([group, name], DOUBLE, "double", width=8, ratio=0.70))
    return schema, leaves


def make_metadata_flat(columns, row_groups):
    """A flat-schema Meta with `columns` leaves and `row_groups` row groups."""
    schema, leaves = flat_branch(columns)
    return assemble_meta(schema, leaves, top_fields=1, row_groups=row_groups)


def assemble_meta(schema, leaves, top_fields, row_groups):
    """Fill in the per-chunk size/offset arrays for a schema, then wrap them in a Meta.

    Two passes: (1) model each chunk's num_values / uncompressed / compressed size, and
    (2) lay the chunks out end to end to derive absolute data_page_offsets.
    """
    set_field_ids(schema)
    columns = len(leaves)
    chunks = columns * row_groups
    rows = [ROWS_PER_GROUP] * row_groups

    num_values = [0] * chunks
    uncompressed_sizes = [0] * chunks
    compressed_sizes = [0] * chunks
    data_page_offsets = [0] * chunks
    dict_page_offsets = [-1] * chunks           # -1 == column is not dictionary-encoded
    codecs = [parquet_thrift.CompressionCodec.SNAPPY] * chunks

    # Pass 1: model per-chunk sizes. A bounded, deterministic per-chunk jitter (seeded per shape)
    # keeps the sizes (and the offsets derived from them) from being a perfect arithmetic
    # progression. Staying within +/-20% of the type's base size keeps the footer plausible and
    # reproducible. This makes the thrift-varint vs LZ4 size comparison fairer: a perfectly regular
    # footer over-favours both varint length and LZ4's repetition dedup.
    rng = random.Random(20260728 + chunks)
    for col, leaf_def in enumerate(leaves):
        for rg in range(row_groups):
            i = col * row_groups + rg
            num_values[i] = round(ROWS_PER_GROUP * leaf_def.values_per_row)
            base_uncompressed = num_values[i] * leaf_def.width + 1024
            uncompressed_sizes[i] = max(128, round(base_uncompressed * rng.uniform(0.8, 1.2)))
            compressed_sizes[i] = max(96, round(uncompressed_sizes[i] * leaf_def.ratio * rng.uniform(0.8, 1.2)))

    # Pass 2: lay chunks out sequentially (row-group-major, as a real file would) to get the
    # absolute byte offset of each chunk's first page. A dictionary column also gets a dictionary
    # page just before its data page.
    offset = 4   # 4-byte magic "PAR1" at the start of the file
    for rg in range(row_groups):
        for col, leaf_def in enumerate(leaves):
            i = col * row_groups + rg
            if leaf_def.dictionary:
                dict_page_offsets[i] = offset
                data_page_offsets[i] = offset + min(8192, compressed_sizes[i] // 3) + 32
            else:
                data_page_offsets[i] = offset + 32
            offset += compressed_sizes[i] + 64

    return Meta(columns=columns, row_groups=row_groups, top_fields=top_fields,
                schema=schema, leaves=leaves, rows=rows,
                num_values=num_values, uncompressed_sizes=uncompressed_sizes,
                compressed_sizes=compressed_sizes, data_page_offsets=data_page_offsets,
                dict_page_offsets=dict_page_offsets, codecs=codecs)


# ---------------------------------------------------------------------------
# Native readers (what the sweep measures) + timing
# ---------------------------------------------------------------------------
# The scanners run in native C++ via the `native_readers` Cython extension (built from
# native_build/ by setup.py). Each reader decodes/reads a footer and returns
# (chunks_visited, sum_of_data_page_offset, sum_of_total_compressed_size) over the selected
# columns. thriftpy2 is used only to build the footers and to validate these readers (below).

_native = None

def load_native():
    """Import the native_readers extension (assumes it is already built:
    `python setup.py build_ext --inplace`)."""
    global _native
    if _native is None:
        if str(HERE) not in sys.path:
            sys.path.insert(0, str(HERE))
        import native_readers
        _native = native_readers
    return _native


def read_current(footer, columns):
    return load_native().current(footer["buf"], columns)

def read_soa(footer, columns):
    return load_native().soa(footer["buf"], columns)

def read_jumptable(footer, columns):
    return load_native().jumptable(footer["buf"], columns,
                                   footer["header_len"], footer["num_columns"], footer["num_row_groups"])

# The three FlatBuffers angles all call the same native reader with (verify, compressed) flags:
#   soa_fb               verify=0 compressed=0  (pure FlatBuffers, buffer trusted)
#   soa_fb_lz4           verify=0 compressed=1  (LZ4 decompress + scan)
#   soa_fb_lz4_verified  verify=1 compressed=1  (decompress + VerifyFileMetaDataBuffer + scan)
def read_soa_fb(footer, columns):
    return load_native().flatbuffers(footer["buf"], columns, footer["raw_size"], footer["num_row_groups"], 0, 0)

def read_soa_fb_lz4(footer, columns):
    return load_native().flatbuffers(footer["buf"], columns, footer["raw_size"], footer["num_row_groups"], 0, 1)

def read_soa_fb_lz4_verified(footer, columns):
    return load_native().flatbuffers(footer["buf"], columns, footer["raw_size"], footer["num_row_groups"], 1, 1)


READERS = {
    "current": read_current,
    "jumptable": read_jumptable,
    "soa": read_soa,
    "soa_fb": read_soa_fb,
    "soa_fb_lz4": read_soa_fb_lz4,
    "soa_fb_lz4_verified": read_soa_fb_lz4_verified,
}


# --- thriftpy2 reference readers, kept only to validate the native decoders ---
def read_current_tp2(footer, columns):
    meta = deserialize(current_thrift.FileMetaData(), footer["buf"], COMPACT_PROTOCOL)
    count = offset_sum = size_sum = 0
    for row_group in meta.row_groups:
        for col in columns:
            m = row_group.columns[col].meta_data
            count += 1
            offset_sum += m.data_page_offset
            size_sum += m.total_compressed_size
    return count, offset_sum, size_sum

def read_soa_tp2(footer, columns):
    meta = deserialize(soa_thrift.FileMetaData(), footer["buf"], COMPACT_PROTOCOL)
    n_row_groups = footer["num_row_groups"]
    offsets = meta.chunks.data_page_offsets
    sizes = meta.chunks.total_compressed_sizes
    count = offset_sum = size_sum = 0
    for col in columns:
        for rg in range(n_row_groups):
            i = col * n_row_groups + rg
            count += 1
            offset_sum += offsets[i]
            size_sum += sizes[i]
    return count, offset_sum, size_sum

def read_jumptable_tp2(footer, columns):
    buf = footer["buf"]
    header_len = footer["header_len"]
    n_columns = footer["num_columns"]
    n_row_groups = footer["num_row_groups"]
    chunk_offsets = deserialize(jumptable_thrift.FileMetaData(), buf[:header_len], COMPACT_PROTOCOL).column_metadata_offsets
    body_len = len(buf) - header_len
    count = offset_sum = size_sum = 0
    for rg in range(n_row_groups):
        for col in columns:
            j = rg * n_columns + col
            start = chunk_offsets[j]
            end = chunk_offsets[j + 1] if j + 1 < len(chunk_offsets) else body_len
            m = jumptable_thrift.ColumnMetaData()
            m.read(TCompactProtocol(TMemoryBuffer(buf[header_len + start: header_len + end])))
            count += 1
            offset_sum += m.data_page_offset
            size_sum += m.total_compressed_size
    return count, offset_sum, size_sum

TP2_READERS = {"current": read_current_tp2, "jumptable": read_jumptable_tp2, "soa": read_soa_tp2}


def projected_columns(meta, projection, seed):
    """A deterministic random subset of columns of size `projection` (fraction), at least one."""
    rng = random.Random(seed)
    count = max(1, round(meta.columns * projection))
    return sorted(rng.sample(range(meta.columns), count))


def fastest_ms(fn, repeats=REPEATS):
    """Fastest wall-clock time of fn() over `repeats` runs, in milliseconds.

    Warm up once (load the library, warm caches) and exclude it, then run `repeats` times and
    take the minimum. The fastest run is the least noisy estimate: background interference can
    only ever add time, never subtract it. GC is paused so a collection can't land inside a
    measured call.
    """
    fn()
    gc_was_on = gc.isenabled()
    gc.disable()
    try:
        best = float("inf")
        for _ in range(repeats):
            start = time.perf_counter()
            fn()
            dt = time.perf_counter() - start
            best = min(best, dt)
    finally:
        if gc_was_on:
            gc.enable()
    return best * 1_000


# ---------------------------------------------------------------------------
# Real-file readers: pyarrow (baseline) and PalletJack
# ---------------------------------------------------------------------------
# These read a real on-disk Parquet file and return a pyarrow FileMetaData — a heavier task than
# the offset/size scanners above (they build a ColumnChunkMetaData object graph, not a sum). They
# form their own comparison group:
#   pyarrow     parses the WHOLE footer every time (no projection) - the un-accelerated baseline;
#               `read_ms` is flat across projection_pct.
#   palletjack  reads metadata for only the selected row groups/columns via its index - it
#               accelerates the pyarrow baseline, converging to it as projection -> 100%.
# For each shape we write one real Parquet file (1 row per row group) and cache it, plus a
# PalletJack index. footer_bytes = real Parquet footer size for pyarrow, index size for palletjack.

def real_parquet_path(n_columns, n_row_groups):
    """Path to a cached real Parquet file of this shape, writing it on first use."""
    PJ_CACHE.mkdir(exist_ok=True)
    path = PJ_CACHE / f"c{n_columns}_rg{n_row_groups}.parquet"
    if not path.exists():
        table = pa.table({f"c{i}": pa.array(range(n_row_groups)) for i in range(n_columns)})
        pq.write_table(table, path, row_group_size=1)
    return path


def pj_index_bytes(n_columns, n_row_groups):
    """PalletJack index bytes for this shape, generating and caching them on first use."""
    idx_path = PJ_CACHE / f"c{n_columns}_rg{n_row_groups}.idx"
    if not idx_path.exists():
        pj.generate_metadata_index(str(real_parquet_path(n_columns, n_row_groups)), str(idx_path))
    return idx_path.read_bytes()


def validate_native():
    """Confirm every native scanner returns exactly what its thriftpy2 reference reader does,
    across a couple of shapes and projections, before trusting the timings."""
    for meta in [make_metadata_flat(16, 20), make_metadata_flat(64, 8)]:
        footers = build_all(meta)
        for projection in (0.1, 0.5, 1.0):
            cols = projected_columns(meta, projection, 1009 * meta.columns + int(projection * 100))
            for fmt, reference in TP2_READERS.items():
                got = READERS[fmt](footers[fmt], cols)
                assert got == reference(footers[fmt], cols), (fmt, meta.columns, meta.row_groups, projection, got)
    print("native decoder validated against thriftpy2")


# ---------------------------------------------------------------------------
# Run sweeps
# ---------------------------------------------------------------------------

def meta_summary(meta):
    """Descriptive columns attached to every result row for this shape."""
    return dict(
        columns=meta.columns, row_groups=meta.row_groups, chunks=meta.chunks,
        schema_elements=len(meta.schema) + 1,
        max_path_depth=max(len(l.path) for l in meta.leaves),
    )


def run_read_case(sweep, x_value, meta, real_file=False):
    """Time every format on one shape, across all projections. Returns a list of result rows."""
    footers = build_all(meta)

    # Sanity: all scanners must agree on the offset/size sums for a fixed column set.
    check_cols = sorted({0, meta.columns // 2, meta.columns - 1})
    checks = {fmt: READERS[fmt](footers[fmt], check_cols) for fmt in FORMATS}
    assert len(set(checks.values())) == 1, checks

    # Load the real Parquet file and the PalletJack index fully into memory HERE, before any
    # timing, so the measured read_metadata calls parse in-memory buffers and never touch disk.
    pa_buf = pa_footer_bytes = pj_index = pj_row_groups = None
    if real_file:
        pa_buf = pa.py_buffer(real_parquet_path(meta.columns, meta.row_groups).read_bytes())
        pa_footer_bytes = pq.read_metadata(pa.BufferReader(pa_buf)).serialized_size
        pj_index = pj_index_bytes(meta.columns, meta.row_groups)
        pj_row_groups = list(range(meta.row_groups))

    rows, info = [], meta_summary(meta)
    for projection in PROJECTIONS:
        cols = projected_columns(meta, projection,
                                 sum(map(ord, sweep)) + int(x_value) * 1009 + round(projection * 10_000))
        row_common = dict(sweep=sweep, x_value=x_value, projection=projection,
                          projection_pct=round(projection * 100))

        for fmt in FORMATS:
            reader, footer = READERS[fmt], footers[fmt]
            rows.append(dict(
                **row_common, format=fmt, footer_bytes=footer["size"],
                read_ms=fastest_ms(lambda reader=reader, footer=footer, cols=cols: reader(footer, cols)),
                **info,
            ))

        if pa_buf is not None:   # pyarrow reads the whole footer regardless of projection
            rows.append(dict(
                **row_common, format="pyarrow", footer_bytes=pa_footer_bytes,
                read_ms=fastest_ms(lambda buf=pa_buf: pq.read_metadata(pa.BufferReader(buf))),
                **info,
            ))
        if pj_index is not None:
            rows.append(dict(
                **row_common, format="palletjack", footer_bytes=len(pj_index),
                read_ms=fastest_ms(lambda idx=pj_index, rg=pj_row_groups, cols=cols:
                                  pj.read_metadata(index_data=idx, row_groups=rg, column_indices=cols)),
                **info,
            ))
    return rows


def run_benchmark():
    """Run both sweeps and return all result rows as a DataFrame."""
    real_note = "+ pyarrow + palletjack"
    if not (PJ_CACHE / f"c{COLUMN_COUNTS[-1]}_rg{ROW_GROUPS_FIXED}.parquet").exists():
        print("first run: writing real Parquet files (+ PalletJack indexes) into "
              f"{PJ_CACHE.name}/ for the pyarrow/palletjack readers; largest shapes take ~1 min each")

    rows = []
    print(f"[columns sweep] varying leaf columns, {ROW_GROUPS_FIXED} row groups, {real_note}")
    for n in COLUMN_COUNTS:
        print(f"  {n} columns")
        rows += run_read_case("columns", n, make_metadata_flat(n, ROW_GROUPS_FIXED), real_file=True)

    print(f"[row-groups sweep] varying row groups, {COLUMNS_FIXED} columns, {real_note}")
    for n in ROW_GROUP_COUNTS:
        print(f"  {n} row groups")
        rows += run_read_case("row groups", n, make_metadata_flat(COLUMNS_FIXED, n), real_file=True)

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Plots and tables
# ---------------------------------------------------------------------------

def plot_filename(sweep):
    """PNG filename for a sweep's plot (also the relative link used in the README)."""
    return f"{sweep.replace(' ', '_')}_sweep.png"


colors = {
    "current": "#d62728", "jumptable": "#1f77b4", "soa": "#2ca02c",
    "soa_fb": "#8c564b", "soa_fb_lz4": "#e377c2", "soa_fb_lz4_verified": "#17becf",
    "pyarrow": "#7f7f7f", "palletjack": "#ff7f0e",
}


def plot_grid(table, sweep, x_label, title, path, *, xscale=None):
    """Render one grid of read_ms-vs-x line charts (one panel per projection) to a PNG at `path`."""
    data = table[table.sweep == sweep]
    fig, axes = plt.subplots(1, len(PROJECTIONS), figsize=(6 * len(PROJECTIONS), 5))
    present = [f for f in DISPLAY_FORMATS if f in set(data.format)]
    for ax, projection in zip(np.atleast_1d(axes), PROJECTIONS):
        for fmt in present:
            series = data[(data.projection == projection) & (data.format == fmt)].sort_values("x_value")
            ax.plot(series.x_value, series.read_ms, marker="o", color=colors[fmt], label=fmt)
        if xscale:
            ax.set_xscale(xscale)
        ax.set_title(f"{projection:.0%} projection")
        ax.set_xlabel(x_label)
        ax.set_ylabel("read ms")
        ax.grid(alpha=.3, which="both")
    np.atleast_1d(axes)[0].legend()
    fig.suptitle(title, y=1.02)
    fig.tight_layout()
    fig.savefig(path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    return path



FORMATS_DOC = """\
## Formats explained

A Parquet footer stores, per column chunk, where that chunk's data lives (data_page_offset)
and how big it is (total_compressed_size), plus type/encoding/row-count bookkeeping. A
"projected" read wants those fields for only a subset of columns. The formats below differ in
how the footer is laid out and therefore in how cheaply a projected read can reach just the
wanted chunks.

- current — the layout Parquet uses today. Nested array-of-structs: FileMetaData -> row groups ->
  columns -> one `ColumnMetaData` struct per chunk, serialized with Thrift's compact protocol.
  The protocol is linear, so to read any column you must decode every field of every chunk from
  the front. Cost grows with the whole footer, not with how many columns you asked for.

- jumptable — the same per-chunk `ColumnMetaData` structs, but the header carries a flat table of
  byte offsets, one per chunk, pointing into a body blob where each chunk's metadata is
  serialized independently. A reader looks up the selected chunks in the offset table and
  decodes only those, seeking past everything else. Trades a small offset table for projected
  reads that scale with the number of columns requested, not the footer size.

- soa (struct-of-arrays) — instead of one struct per chunk, store one parallel array per field
  across all chunks: all data_page_offsets together, all total_compressed_sizes together, and so
  on. Still Thrift compact and still decoded whole, but dropping the repeated per-chunk field
  tags makes it markedly smaller and faster to decode than "current".

- soa_fb — the same struct-of-arrays layout encoded with FlatBuffers instead of Thrift.
  FlatBuffers stores fixed-width vectors that are addressable in place: there is no decode pass,
  the reader points at the buffer and indexes the two vectors it needs at the selected chunk
  positions. This variant is uncompressed and trusted (the buffer is read without validation).

- soa_fb_lz4 — the soa_fb footer compressed with LZ4_FRAME. Smaller on disk, but LZ4_FRAME is not
  random-access, so the reader must decompress the whole footer into memory before the in-place
  scan. Isolates the size/speed cost of compression versus soa_fb.

- soa_fb_lz4_verified — soa_fb_lz4 plus a run of the generated FlatBuffers verifier
  (bounds/offset checks over the buffer) before reading. This is the safe way to read a buffer
  from an untrusted source; the gap to soa_fb_lz4 is what that safety costs.

- pyarrow — the un-accelerated baseline. Parse the entire real Parquet footer with Arrow and
  build a full FileMetaData object graph on every call, with no projection. This is what reading
  metadata costs today without any acceleration.

- palletjack — G-Research PalletJack. A one-time sidecar index built over a real Parquet file's
  metadata. Read fetches only the selected row groups/columns through the index instead of
  parsing the whole footer, so it accelerates the pyarrow baseline and converges to it as the
  projection approaches 100%.
"""

LEGEND = """\
## Reading these tables

Values are `read_ms`: fastest wall-clock milliseconds over REPEATS runs. Note that the two categories
are NOT directly comparable to each other:

A) **Offset/size scanners** - decode the footer and SUM `data_page_offset` +
   `total_compressed_size` over the projected columns (return three integers):
- current — native Thrift C++ — decodes the WHOLE footer (compact protocol is linear)
- jumptable — native Thrift C++ — seeks to only the selected chunks via its offset table
- soa — native Thrift C++ — decodes the whole struct-of-arrays footer
- soa_fb — native FlatBuffers C++ — reads the uncompressed SoA footer directly and sums the
             two chunk vectors over the selected columns. Buffer is trusted (no verification).
- soa_fb_lz4 — same, but the footer is LZ4_FRAME-compressed; the reader decompresses the whole
             footer (C++ liblz4) before the scan.
- soa_fb_lz4_verified — same as soa_fb_lz4, but runs the generated FlatBuffers verifier
             (`VerifyFileMetaDataBuffer`) before the scan.

B) **FileMetaData producers** - return a full pyarrow `FileMetaData` object graph (one
   ColumnChunkMetaData per selected chunk); much heavier per chunk than a sum:
- pyarrow — baseline — parses the WHOLE real Parquet footer every call (no projection),
             so `read_ms` is flat across projection_pct. This is the un-accelerated cost.
- palletjack — reads metadata for ONLY the selected row groups +
             columns via its index, so it accelerates the pyarrow baseline and converges
             to it as projection -> 100%. (columns / row-groups sweeps only.)

Each sweep prints two tables: `read_ms` (by projection) and `footer_bytes`. The `footer_bytes` is
the serialized footer size each producer emits (independent of projection).
It is a size-on-disk metric, not necessarily the bytes touched by a projected read.
"""

CONCLUSIONS = """\
## Conclusions

Read speed:
- soa is faster than "current". The struct-of-arrays layout drops the nested
  `ColumnChunk`/`ColumnMetaData` framing and the per-chunk field tags that "current" repeats for
  every chunk. The Thrift compact decoder therefore parses far fewer fields for the same
  information.
- jumptable wins most at low projection. It decodes the header's offset table once, then fully
  decodes only the selected chunks and seeks past the rest; current and soa decode every chunk
  regardless of projection. jumptable still reads the whole offset table, so its cost is that
  pass plus the selected chunks (not purely the projection), and its advantage shrinks as
  projection approaches 100%.
- soa_fb is faster than soa. FlatBuffers needs no decode pass at all: "soa" runs the Thrift
  compact decoder over the entire footer (varint parsing + building vectors), while "soa_fb"
  casts the buffer and reads the two chunk vectors it needs directly. Its cost is O(selected
  chunks) rather than O(whole footer).
- soa_fb_lz4 is slower than soa_fb. The reader must decompress the entire footer before the 
  in-place scan. The gap (soa_fb_lz4 minus soa_fb) is the whole-footer decompression cost,
  and the read no longer scales with only the selected chunks. LZ4 decompression is
  about 4x faster than the Thrift compact decoder, so decompressing the whole footer here is
  still far cheaper than the full Thrift decode that "current" and "soa" pay.
- soa_fb_lz4_verified is about the same as soa_fb_lz4. The FlatBuffers verifier is a single
  bounds/offset-checking pass over the buffer (cheap integer comparisons) and is dwarfed by the
  decompress + scan already paid. Validating an untrusted buffer is nearly free here.

Footer size (`footer_bytes`):
- current is larger than soa. "current" keeps the nested framing plus duplicate/deprecated
  fields; "soa" stores the same information as parallel arrays with no per-chunk tags.
- soa_fb (uncompressed) is larger than soa (Thrift). FlatBuffers stores fixed-width 8-byte
  integers, whereas Thrift compact varint-encodes them, so the raw FlatBuffers footer is bigger.
- soa_fb_lz4 is close to soa (Thrift). LZ4-compressing the fixed-width FlatBuffers footer
  recovers most of what varint bought Thrift: per-value varint and block compression are two
  routes to the same compactness, so they land in the same neighborhood.
- current is much smaller than pyarrow as it is a reduced
  footer that omits statistics, key/value metadata, created_by, column_orders and page-index
  offsets, while pyarrow measures a real, full footer that carries all of them.

Decoder caveat:
- The current, jumptable and soa readers use the generic Thrift compact decoder emitted by the
  Thrift compiler, which is not necessarily the most optimal Thrift parser; a hand-written decoder
  could do less per-field work. The current layout has the most headroom, since much of its cost is
  per-field dispatch across the nested per-chunk structs. For the soa layout the generic decoder is
  close to optimal: the struct-of-arrays collapses the footer into a few primitive arrays, so the
  decode is already little more than a varint scan with little per-field work left to remove.
"""


# One entry per sweep: (sweep name, extra pivot index columns, x-axis label, table title).
SWEEPS = [
    ("columns", ["chunks"], "n_columns", f"COLUMNS sweep — {ROW_GROUPS_FIXED} row groups fixed"),
    ("row groups", ["chunks"], "n_row_groups", f"ROW-GROUPS sweep — {COLUMNS_FIXED} columns fixed"),
]


def _present_formats(pivot):
    """DISPLAY_FORMATS that actually appear as columns in this pivot, in display order."""
    return [f for f in DISPLAY_FORMATS if f in pivot.columns]


def _read_pivot(results, sweep, extra_index, x_name):
    """read_ms pivoted as (projection, swept variable) rows x format columns."""
    sub = results[results.sweep == sweep]
    pivot = sub.pivot_table(index=["projection_pct", "x_value"] + extra_index, columns="format", values="read_ms")
    return pivot.reindex(columns=_present_formats(pivot)).round(3).rename_axis(index={"x_value": x_name})


def _size_pivot(results, sweep, extra_index, x_name):
    """footer_bytes pivoted by the swept variable only (it is constant across projection)."""
    sub = results[results.sweep == sweep]
    pivot = sub.pivot_table(index=["x_value"] + extra_index, columns="format", values="footer_bytes", aggfunc="first")
    pivot = pivot.reindex(columns=_present_formats(pivot)).astype("int64")
    return pivot.apply(lambda col: col.map(humanize.naturalsize)).rename_axis(index={"x_value": x_name})


def print_tables(results):
    """Print the legend and, per sweep, the `read_ms` and footer_bytes tables to stdout."""
    with pd.option_context("display.max_rows", None, "display.max_columns", None, "display.width", None):
        print("\n" + FORMATS_DOC)
        print("\n" + LEGEND)
        for sweep, extra, x_name, title in SWEEPS:
            print(f"\n=== {title} ===")
            print("-- `read_ms` --")
            print(_read_pivot(results, sweep, extra, x_name))
            print("-- footer_bytes --")
            print(_size_pivot(results, sweep, extra, x_name))
        print("\n" + CONCLUSIONS)


def markdown_report(results):
    """Render the module header, legend, and tables as a Markdown document string.

    The module docstring (this file's header) is included as the intro. The pivot tables are
    monospace-aligned, so each is wrapped in a fenced code block (Markdown would otherwise collapse
    the whitespace); the legend is fenced for the same reason. Headings and bold labels render
    normally.
    """
    out = [__doc__,
           FORMATS_DOC,
           LEGEND]
    with pd.option_context("display.max_rows", None, "display.max_columns", None, "display.width", None):
        for sweep, extra, x_name, title in SWEEPS:
            out += [
                f"\n## {title}\n",
                f"![{title}]({plot_filename(sweep)})\n",
                "**read_ms**\n",
                "```text", _read_pivot(results, sweep, extra, x_name).to_string(), "```",
                "\n**footer_bytes**\n",
                "```text", _size_pivot(results, sweep, extra, x_name).to_string(), "```",
            ]
    out += ["\n",CONCLUSIONS]
    return "\n".join(out) + "\n"


def main():
    validate_native()
    results = run_benchmark()

    # Write the plots first, then the README that links to them (README embeds plot_filename(sweep)).
    columns_png = plot_grid(results, "columns", "number of columns",
                            "Columns sweep by projection", HERE / plot_filename("columns"))
    rowgroups_png = plot_grid(results, "row groups", "number of row groups",
                              "Row-group sweep by projection", HERE / plot_filename("row groups"))
    print(f"wrote plots to {columns_png} and {rowgroups_png}")

    readme = HERE / "README.md"
    readme.write_text(markdown_report(results))
    print(f"wrote Markdown report to {readme}")

    print_tables(results)


if __name__ == "__main__":
    main()
