"""Build the native_readers Cython extension for the parquet-footer benchmarks.

Generates C++ from the footer thrift IDLs with the Apache Thrift compiler and from the
FlatBuffers schema with flatc (both `--cpp` for the native reader and `--python` for the
accessors the benchmark uses to build footers, into flatbuffers_gen/), then compiles the
generated C++ plus the shim and Cython wrapper into a Python extension linked against
libthrift and liblz4. Project metadata and dependencies live in pyproject.toml; this file
only carries the codegen step and the extension definition.

Build in place (module lands next to this file):

    python setup.py build_ext --inplace

or install the project (runs the same build):

    pip install .
"""
import glob
import os
import re
import shutil
import subprocess
from pathlib import Path

from setuptools import setup, Extension
from Cython.Build import cythonize

HERE = Path(__file__).resolve().parent
GEN = HERE / "native_build"  # holds the hand-written shim/wrapper and the generated C++


def codegen():
    GEN.mkdir(exist_ok=True)
    # Thrift derives C++ include-guard identifiers from the filename, and hyphens are
    # illegal in C++ identifiers, so generate from underscore-named copies.
    for f in ("current", "soa"):
        shutil.copyfile(HERE / f"footer-core-{f}.thrift", GEN / f"footer_core_{f}.thrift")

    # parquet.thrift carries an augmented supplementary-index block (struct SchemaLayout
    # onward) that forward-references unions the C++ generator emits as incomplete types
    # and has a field named `explicit` (a C++ keyword). None of it is reachable from the
    # footer structs we decode, so drop it in this build-only copy.
    text = (HERE / "parquet.thrift").read_text()
    text = re.split(r"(?m)^struct SchemaLayout \{", text)[0]
    (GEN / "parquet.thrift").write_text(text)

    thrift = shutil.which("thrift")
    if thrift is None:
        raise SystemExit("thrift compiler not found on PATH (install thrift-compiler matching libthrift)")
    for f in ("current", "soa"):
        subprocess.run([thrift, "-r", "--gen", "cpp", "-out", str(GEN), str(GEN / f"footer_core_{f}.thrift")], check=True)

    flatc = shutil.which("flatc")
    if flatc is None:
        raise SystemExit("flatc not found on PATH (install flatbuffers-compiler matching libflatbuffers-dev)")

    # FlatBuffers C++ header for the native reader. flatc names the output <stem>_generated.h,
    # so generate from an underscore-named copy to get a valid C++ include.
    shutil.copyfile(HERE / "footer-core-flatbuffers.fbs", GEN / "footer_core_flatbuffers.fbs")
    subprocess.run([flatc, "--cpp", "-o", str(GEN), str(GEN / "footer_core_flatbuffers.fbs")], check=True)

    # FlatBuffers Python accessors used by the benchmark to BUILD footers (load_fb imports these).
    py_gen = HERE / "flatbuffers_gen"
    py_gen.mkdir(exist_ok=True)
    subprocess.run([flatc, "--python", "-o", str(py_gen), str(HERE / "footer-core-flatbuffers.fbs")], check=True)


codegen()

# Release by default; DEBUG=ON produces an unoptimized build with debug info plus
# Cython gdb support / line tracing (pattern from JollyJack/setup.py).
extra_compile_args = ["-std=c++17"]
extra_link_args = []
debug = False
if os.getenv("DEBUG", "") == "ON":
    print("Building with DEBUG information!")
    extra_compile_args.extend(["-O0", "-g", "-DDEBUG"])
    extra_link_args.append("-g")
    debug = True
else:
    extra_compile_args.append("-O3")

sources = [str(GEN / "native_readers.pyx"), str(GEN / "shim.cpp")] + sorted(glob.glob(str(GEN / "*_types.cpp")))
ext = Extension(
    "native_readers",
    sources=sources,
    include_dirs=[str(GEN)],
    libraries=["thrift", "lz4"],  # lz4: LZ4_FRAME decompression in the FlatBuffers reader (flatbuffers C++ is header-only)
    language="c++",
    extra_compile_args=extra_compile_args,
    extra_link_args=extra_link_args,
)
setup(ext_modules=cythonize([ext], language_level=3, force=True, gdb_debug=debug, emit_linenums=debug))
