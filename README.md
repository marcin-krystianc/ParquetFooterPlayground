# Parquet footer benchmarks (2026)

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


## COLUMNS sweep — 100 row groups fixed

![COLUMNS sweep — 100 row groups fixed](columns_sweep.png)

**read_ms**

```text
format                           current  jumptable    soa  soa_fb  soa_fb_lz4  soa_fb_lz4_verified  pyarrow  palletjack
projection_pct n_columns chunks                                                                                         
10             4         400       0.123      0.024  0.014   0.000       0.006                0.006    0.205       0.055
               8         800       0.225      0.027  0.025   0.000       0.011                0.011    0.391       0.055
               16        1600      0.437      0.048  0.048   0.000       0.020                0.020    0.779       0.109
               32        3200      0.869      0.073  0.094   0.000       0.038                0.038    1.538       0.152
               64        6400      1.690      0.159  0.184   0.001       0.060                0.063    2.933       0.278
               128       12800     3.392      0.305  0.359   0.001       0.092                0.093    5.590       0.666
               256       25600     6.674      0.607  0.731   0.001       0.190                0.192   11.136       1.198
               512       51200    13.075      1.170  1.514   0.002       0.399                0.402   22.879       2.298
               1024      102400   26.381      2.317  2.950   0.005       0.771                0.773   46.211       4.615
50             4         400       0.117      0.042  0.014   0.000       0.006                0.006    0.213       0.107
               8         800       0.231      0.083  0.025   0.000       0.011                0.011    0.413       0.196
               16        1600      0.443      0.149  0.049   0.001       0.020                0.020    0.777       0.381
               32        3200      0.867      0.305  0.094   0.001       0.038                0.039    1.485       0.777
               64        6400      1.675      0.585  0.182   0.002       0.059                0.062    2.816       1.441
               128       12800     3.313      1.163  0.364   0.003       0.094                0.096    5.611       2.794
               256       25600     6.352      2.286  0.717   0.006       0.190                0.192   10.902       5.440
               512       51200    12.745      4.556  1.453   0.011       0.390                0.391   23.622      10.950
               1024      102400   26.129      9.318  2.947   0.022       0.795                0.801   46.367      22.816
90             4         400       0.121      0.080  0.014   0.000       0.006                0.006    0.207       0.193
               8         800       0.226      0.133  0.025   0.001       0.010                0.011    0.408       0.329
               16        1600      0.438      0.258  0.048   0.001       0.020                0.020    0.779       0.659
               32        3200      0.877      0.519  0.095   0.001       0.038                0.038    1.495       1.329
               64        6400      1.684      1.030  0.184   0.003       0.063                0.063    2.827       2.551
               128       12800     3.302      2.013  0.360   0.005       0.097                0.099    5.558       4.873
               256       25600     6.438      3.982  0.718   0.009       0.199                0.196   11.248       9.666
               512       51200    12.893      8.116  1.472   0.019       0.398                0.401   22.905      20.146
               1024      102400   27.083     16.232  3.060   0.039       0.802                0.800   46.702      41.998
```

**footer_bytes**

```text
format             current jumptable       soa    soa_fb soa_fb_lz4 soa_fb_lz4_verified   pyarrow palletjack
n_columns chunks                                                                                            
4         400      20.9 kB   10.1 kB    6.7 kB   19.0 kB     7.1 kB              7.1 kB   43.8 kB    47.1 kB
8         800      41.0 kB   20.4 kB   13.4 kB   36.8 kB    14.1 kB             14.1 kB   86.1 kB    91.1 kB
16        1600     80.6 kB   40.8 kB   27.0 kB   72.5 kB    27.5 kB             27.5 kB  171.4 kB   179.8 kB
32        3200    161.5 kB   83.2 kB   55.3 kB  143.9 kB    55.4 kB             55.4 kB  342.3 kB   357.4 kB
64        6400    321.7 kB  166.6 kB  111.0 kB  286.6 kB   110.0 kB            110.0 kB  683.9 kB   712.5 kB
128       12800   643.8 kB  334.8 kB  223.4 kB  572.0 kB   220.7 kB            220.7 kB    1.4 MB     1.4 MB
256       25600     1.3 MB  670.0 kB  447.4 kB    1.1 MB   439.8 kB            439.8 kB    2.8 MB     2.9 MB
512       51200     2.6 MB    1.3 MB  896.1 kB    2.3 MB   881.0 kB            881.0 kB    5.6 MB     5.8 MB
1024      102400    5.1 MB    2.7 MB    1.8 MB    4.6 MB     1.8 MB              1.8 MB   11.2 MB    11.7 MB
```

## ROW-GROUPS sweep — 100 columns fixed

![ROW-GROUPS sweep — 100 columns fixed](row_groups_sweep.png)

**read_ms**

```text
format                              current  jumptable    soa  soa_fb  soa_fb_lz4  soa_fb_lz4_verified  pyarrow  palletjack
projection_pct n_row_groups chunks                                                                                         
10             4            400       0.122      0.023  0.022   0.000       0.006                0.007    0.355       0.028
               8            800       0.219      0.034  0.033   0.000       0.010                0.010    0.534       0.049
               16           1600      0.667      0.077  0.082   0.001       0.021                0.016    0.936       0.084
               32           3200      0.864      0.086  0.101   0.000       0.028                0.028    1.638       0.165
               64           6400      1.706      0.156  0.187   0.001       0.048                0.051    2.915       0.304
               128          12800     3.312      0.289  0.361   0.001       0.093                0.095    5.525       0.602
               256          25600     6.712      0.565  0.697   0.001       0.188                0.185   10.718       1.181
               512          51200    13.338      1.129  1.414   0.002       0.388                0.387   22.260       2.316
               1024         102400   26.470      2.253  2.793   0.004       0.779                0.776   45.947       4.704
50             4            400       0.117      0.051  0.022   0.000       0.006                0.007    0.358       0.128
               8            800       0.228      0.087  0.033   0.001       0.010                0.010    0.542       0.212
               16           1600      0.436      0.163  0.056   0.001       0.016                0.016    0.885       0.405
               32           3200      0.842      0.295  0.097   0.001       0.027                0.028    1.579       0.776
               64           6400      1.649      0.573  0.183   0.001       0.047                0.050    2.916       1.398
               128          12800     3.222      1.202  0.363   0.003       0.095                0.096    5.619       2.804
               256          25600     6.692      2.326  0.717   0.005       0.193                0.193   11.036       5.575
               512          51200    12.817      4.688  1.428   0.009       0.404                0.409   22.021      10.971
               1024         102400   27.070      9.103  2.830   0.018       0.808                0.808   45.004      23.295
90             4            400       0.121      0.079  0.022   0.001       0.006                0.007    0.339       0.217
               8            800       0.223      0.143  0.033   0.001       0.010                0.010    0.555       0.390
               16           1600      0.438      0.277  0.057   0.001       0.016                0.016    0.893       0.728
               32           3200      0.845      0.522  0.102   0.001       0.029                0.029    1.636       1.342
               64           6400      1.644      1.026  0.182   0.002       0.050                0.048    2.836       2.569
               128          12800     3.351      2.092  0.371   0.005       0.099                0.100    5.663       4.972
               256          25600     6.618      4.099  0.738   0.009       0.197                0.197   11.406      10.179
               512          51200    13.506      8.043  1.446   0.016       0.409                0.408   22.397      20.129
               1024         102400   27.402     16.125  2.911   0.032       0.806                0.805   45.312      40.727
```

**footer_bytes**

```text
format                current jumptable       soa    soa_fb soa_fb_lz4 soa_fb_lz4_verified   pyarrow palletjack
n_row_groups chunks                                                                                            
4            400      21.8 kB   11.6 kB    8.2 kB   24.0 kB     9.6 kB              9.6 kB   50.1 kB    53.9 kB
8            800      41.3 kB   21.5 kB   14.7 kB   41.6 kB    16.0 kB             16.0 kB   92.6 kB    98.0 kB
16           1600     81.3 kB   42.2 kB   28.4 kB   76.9 kB    29.6 kB             29.6 kB  177.4 kB   186.1 kB
32           3200    161.5 kB   83.9 kB   56.1 kB  147.4 kB    56.7 kB             56.7 kB  347.2 kB   362.5 kB
64           6400    322.0 kB  167.2 kB  111.7 kB  288.4 kB   110.8 kB            110.8 kB  686.6 kB   715.3 kB
128          12800   643.0 kB  334.0 kB  222.7 kB  570.6 kB   219.6 kB            219.6 kB    1.4 MB     1.4 MB
256          25600     1.3 MB  667.4 kB  444.8 kB    1.1 MB   438.7 kB            438.7 kB    2.8 MB     2.9 MB
512          51200     2.6 MB    1.3 MB  888.9 kB    2.3 MB   890.6 kB            890.6 kB    5.5 MB     5.7 MB
1024         102400    5.1 MB    2.7 MB    1.8 MB    4.5 MB     1.8 MB              1.8 MB   11.1 MB    11.5 MB
```


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

