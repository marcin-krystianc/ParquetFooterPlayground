# Parquet footer benchmarks (2026)

Parquet ([file format docs](https://parquet.apache.org/docs/file-format/)) stores its metadata
footer as a nested array of Thrift structs, which forces a reader to decode the whole footer even
when it only wants the byte offsets of a few columns. This benchmark measures the read cost of
alternative footer layouts and encodings, as input to designing a new footer format. Read cost is
the time to obtain `data_page_offset` and `total_compressed_size` for a projected subset of
columns.

Two sweeps compare footer encodings: `current`, `soa`, and three FlatBuffers SoA
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
format                           current    soa  soa_fb  soa_fb_lz4  soa_fb_lz4_verified  pyarrow  palletjack
projection_pct n_columns chunks                                                                              
10             4         400       0.126  0.014   0.000       0.006                0.006    0.196       0.055
               8         800       0.230  0.025   0.000       0.011                0.011    0.367       0.054
               16        1600      0.490  0.049   0.000       0.020                0.020    0.767       0.102
               32        3200      0.926  0.096   0.000       0.039                0.039    1.427       0.148
               64        6400      1.756  0.191   0.001       0.061                0.064    2.704       0.283
               128       12800     3.529  0.369   0.001       0.096                0.096    5.324       0.622
               256       25600     6.925  0.744   0.001       0.196                0.196   10.845       1.179
               512       51200    13.512  1.489   0.003       0.402                0.406   21.644       2.229
               1024      102400   27.465  3.065   0.005       0.800                0.802   44.007       4.287
50             4         400       0.121  0.014   0.000       0.006                0.006    0.192       0.100
               8         800       0.240  0.025   0.000       0.011                0.011    0.373       0.187
               16        1600      0.459  0.049   0.001       0.020                0.021    0.754       0.366
               32        3200      0.907  0.094   0.001       0.038                0.039    1.426       0.719
               64        6400      1.740  0.187   0.002       0.061                0.064    2.717       1.379
               128       12800     3.486  0.371   0.003       0.096                0.098    5.308       2.732
               256       25600     6.779  0.749   0.006       0.199                0.200   10.919       5.374
               512       51200    13.555  1.525   0.012       0.408                0.409   21.778      10.591
               1024      102400   27.657  3.093   0.024       0.816                0.823   44.160      22.119
90             4         400       0.121  0.014   0.000       0.006                0.006    0.191       0.191
               8         800       0.238  0.025   0.001       0.011                0.011    0.368       0.302
               16        1600      0.453  0.049   0.001       0.020                0.020    0.743       0.643
               32        3200      0.864  0.096   0.002       0.039                0.041    1.429       1.268
               64        6400      1.764  0.187   0.003       0.065                0.062    2.712       2.426
               128       12800     3.451  0.372   0.005       0.099                0.101    5.330       4.776
               256       25600     6.980  0.771   0.010       0.206                0.209   10.894       9.931
               512       51200    13.634  1.523   0.020       0.415                0.417   21.478      19.824
               1024      102400   27.963  3.069   0.041       0.831                0.835   44.191      39.940
```

**footer_bytes**

```text
format             current       soa    soa_fb soa_fb_lz4 soa_fb_lz4_verified   pyarrow palletjack
n_columns chunks                                                                                  
4         400      20.9 kB    6.7 kB   19.0 kB     7.1 kB              7.1 kB   43.8 kB    47.1 kB
8         800      41.0 kB   13.4 kB   36.8 kB    14.1 kB             14.1 kB   86.1 kB    91.1 kB
16        1600     80.6 kB   27.0 kB   72.5 kB    27.5 kB             27.5 kB  171.4 kB   179.8 kB
32        3200    161.5 kB   55.3 kB  143.9 kB    55.4 kB             55.4 kB  342.3 kB   357.4 kB
64        6400    321.7 kB  111.0 kB  286.6 kB   110.0 kB            110.0 kB  683.9 kB   712.5 kB
128       12800   643.8 kB  223.4 kB  572.0 kB   220.7 kB            220.7 kB    1.4 MB     1.4 MB
256       25600     1.3 MB  447.4 kB    1.1 MB   439.8 kB            439.8 kB    2.8 MB     2.9 MB
512       51200     2.6 MB  896.1 kB    2.3 MB   881.0 kB            881.0 kB    5.6 MB     5.8 MB
1024      102400    5.1 MB    1.8 MB    4.6 MB     1.8 MB              1.8 MB   11.2 MB    11.7 MB
```

## ROW-GROUPS sweep — 100 columns fixed

![ROW-GROUPS sweep — 100 columns fixed](row_groups_sweep.png)

**read_ms**

```text
format                              current    soa  soa_fb  soa_fb_lz4  soa_fb_lz4_verified  pyarrow  palletjack
projection_pct n_row_groups chunks                                                                              
10             4            400       0.122  0.022   0.000       0.006                0.007    0.321       0.029
               8            800       0.231  0.034   0.000       0.010                0.010    0.496       0.046
               16           1600      0.447  0.058   0.000       0.016                0.016    0.857       0.083
               32           3200      0.922  0.103   0.000       0.029                0.029    1.517       0.144
               64           6400      1.781  0.192   0.001       0.053                0.050    2.815       0.282
               128          12800     3.460  0.364   0.001       0.096                0.097    5.284       0.586
               256          25600     7.076  0.727   0.001       0.193                0.194   10.574       1.125
               512          51200    13.669  1.477   0.002       0.391                0.390   21.374       2.293
               1024         102400   27.971  2.960   0.004       0.812                0.810   43.148       4.432
50             4            400       0.125  0.022   0.000       0.006                0.007    0.316       0.122
               8            800       0.234  0.034   0.000       0.010                0.011    0.495       0.208
               16           1600      0.452  0.057   0.001       0.016                0.017    0.861       0.370
               32           3200      0.899  0.102   0.001       0.028                0.029    1.477       0.723
               64           6400      1.778  0.189   0.002       0.048                0.052    2.724       1.392
               128          12800     3.468  0.368   0.003       0.098                0.099    5.271       2.775
               256          25600     6.787  0.727   0.005       0.196                0.198   10.490       5.241
               512          51200    13.913  1.465   0.010       0.397                0.397   21.282      10.815
               1024         102400   28.791  3.130   0.019       0.824                0.823   43.200      22.730
90             4            400       0.122  0.023   0.001       0.006                0.007    0.320       0.211
               8            800       0.228  0.034   0.001       0.010                0.011    0.479       0.364
               16           1600      0.439  0.057   0.001       0.016                0.016    0.838       0.671
               32           3200      0.898  0.102   0.001       0.028                0.029    1.493       1.287
               64           6400      1.771  0.194   0.003       0.052                0.053    2.733       2.405
               128          12800     3.522  0.373   0.005       0.098                0.101    5.306       4.701
               256          25600     6.935  0.731   0.009       0.200                0.199   10.322       9.423
               512          51200    14.213  1.479   0.017       0.405                0.404   21.302      19.656
               1024         102400   28.585  2.989   0.034       0.835                0.835   43.318      39.256
```

**footer_bytes**

```text
format                current       soa    soa_fb soa_fb_lz4 soa_fb_lz4_verified   pyarrow palletjack
n_row_groups chunks                                                                                  
4            400      21.8 kB    8.2 kB   24.0 kB     9.6 kB              9.6 kB   50.1 kB    53.9 kB
8            800      41.3 kB   14.7 kB   41.6 kB    16.0 kB             16.0 kB   92.6 kB    98.0 kB
16           1600     81.3 kB   28.4 kB   76.9 kB    29.6 kB             29.6 kB  177.4 kB   186.1 kB
32           3200    161.5 kB   56.1 kB  147.4 kB    56.7 kB             56.7 kB  347.2 kB   362.5 kB
64           6400    322.0 kB  111.7 kB  288.4 kB   110.8 kB            110.8 kB  686.6 kB   715.3 kB
128          12800   643.0 kB  222.7 kB  570.6 kB   219.6 kB            219.6 kB    1.4 MB     1.4 MB
256          25600     1.3 MB  444.8 kB    1.1 MB   438.7 kB            438.7 kB    2.8 MB     2.9 MB
512          51200     2.6 MB  888.9 kB    2.3 MB   890.6 kB            890.6 kB    5.5 MB     5.7 MB
1024         102400    5.1 MB    1.8 MB    4.5 MB     1.8 MB              1.8 MB   11.1 MB    11.5 MB
```


## Conclusions

Read speed:
- soa is faster than "current". The struct-of-arrays layout drops the nested
  `ColumnChunk`/`ColumnMetaData` framing and the per-chunk field tags that "current" repeats for
  every chunk. The Thrift compact decoder therefore parses far fewer fields for the same
  information.
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
- The current and soa readers use the generic Thrift compact decoder emitted by the
  Thrift compiler, which is not necessarily the most optimal Thrift parser; a hand-written decoder
  could do less per-field work. The current layout has the most headroom, since much of its cost is
  per-field dispatch across the nested per-chunk structs. For the soa layout the generic decoder is
  close to optimal: the struct-of-arrays collapses the footer into a few primitive arrays, so the
  decode is already little more than a varint scan with little per-field work left to remove.

