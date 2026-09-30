"""Exercise the protoc plugin protocol and negative schema contracts."""
import pathlib
import subprocess
import sys
import tempfile

protoc, plugin, fixtures = sys.argv[1:]


def generate(root, output, names, parameter=""):
    output.mkdir(parents=True, exist_ok=True)
    return subprocess.run(
        [protoc, f"--proto_path={root}", f"--cpp_out={output}",
         f"--rpc_out={parameter + ':' if parameter else ''}{output}",
         f"--plugin=protoc-gen-rpc={plugin}", *names],
        capture_output=True, text=True, timeout=10,
    )


with tempfile.TemporaryDirectory(prefix="lrpc-codegen-") as directory:
    root = pathlib.Path(directory)
    names = ["payload.proto", "echo.proto", "nested/lite.proto"]
    for run in ("a", "b"):
        result = generate(fixtures, root / run, names)
        assert result.returncode == 0, result.stderr
    first = {p.relative_to(root / "a"): p.read_bytes() for p in (root / "a").rglob("*") if p.is_file()}
    second = {p.relative_to(root / "b"): p.read_bytes() for p in (root / "b").rglob("*") if p.is_file()}
    assert first == second and len(first) == 12
    assert b"delete_(" in first[pathlib.Path("echo.rpc.hpp")]
    assert b"::test::messages::Container_Item" in first[pathlib.Path("echo.rpc.hpp")]
    assert b"add_Echo_service" in first[pathlib.Path("echo.rpc.hpp")]
    assert b"channel.bind(::demo::Echo_service_descriptor())" in first[pathlib.Path("echo.rpc.cpp")]
    for kind in (b"unary", b"client_streaming", b"server_streaming", b"bidirectional"):
        assert b"::rpc::method_kind::" + kind in first[pathlib.Path("echo.rpc.cpp")]
    assert b"::rpc::bind_stream_method" in first[pathlib.Path("echo.rpc.cpp")]
    assert b"::rpc::open_stream<" in first[pathlib.Path("echo.rpc.cpp")]

    cases = [
        ('message M{} service S {rpc delete(M) returns(M); rpc delete_(stream M) returns(stream M);}', "collision"),
        ('message M{} service S {rpc delete(M) returns(M); rpc delete_(M) returns(M);}', "collision"),
        ('message EchoService{} service Echo{}', "collision"),
        ('message Outer {message EchoService{}} service Outer_Echo{}', "collision"),
        ('enum E {EchoService=0;} service Echo{}', "collision"),
        ('enum Echo_service {ZERO=0;} service Echo{}', "collision"),
        ('message add_Echo_service{} service Echo{}', "collision"),
        ('message M{} service S{rpc lrpc_methods_(M) returns(M);}', "collision"),
        ('option cc_generic_services=true; service Echo{}', "cc_generic_services"),
    ]
    for index, (body, error) in enumerate(cases):
        (root / "bad.proto").write_text('syntax="proto3"; package bad; ' + body)
        result = generate(root, root / f"bad-{index}", ["bad.proto"])
        assert result.returncode != 0 and error in result.stderr, result.stderr
        assert not list((root / f"bad-{index}").rglob("*.rpc.*"))
    result = generate(fixtures, root / "parameter", ["payload.proto"], "unknown=true")
    assert result.returncode != 0 and "parameters" in result.stderr
    # Preserve directories with identical basenames; no package is also valid.
    for path, package in (("one/same.proto", "one"), ("two/same.proto", "two")):
        p = root / path; p.parent.mkdir(exist_ok=True)
        p.write_text(f'syntax="proto3"; package {package}; message M{{}} service S{{rpc X(M) returns(M);}}')
    result = generate(root, root / "same", ["one/same.proto", "two/same.proto"])
    assert result.returncode == 0, result.stderr
    assert (root / "same/one/same.rpc.hpp").is_file() and (root / "same/two/same.rpc.hpp").is_file()
    (root / "global.proto").write_text('syntax="proto3"; message M{} service S{rpc X(M) returns(M);}')
    result = generate(root, root / "global", ["global.proto"])
    assert result.returncode == 0, result.stderr
    # Cross-file flattened names must be checked before any output is emitted.
    (root / "one.proto").write_text('syntax="proto3"; package p; message Outer{message EchoService{}}')
    (root / "two.proto").write_text('syntax="proto3"; package p; service Outer_Echo{}')
    result = generate(root, root / "collision", ["one.proto", "two.proto"])
    assert result.returncode != 0 and "collision" in result.stderr
print("PASS deterministic generation, imports, namespaces, collisions and mixed RPC kinds")
