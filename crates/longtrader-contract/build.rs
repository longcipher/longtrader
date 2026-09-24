#![allow(missing_docs)]

use std::path::{Path, PathBuf};

/// Recursively collect every `.proto` under the workspace-managed contract
/// tree (`<workspace>/proto`). Sorted for deterministic codegen.
fn collect_protos(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_protos(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "proto") {
            out.push(path);
        }
    }
}

fn main() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set");
    // In the workspace the canonical contract lives at `<workspace>/proto`.
    // In a published crate that directory is outside the package, so fall back
    // to the vendored copy shipped under this crate's `proto/` directory.
    let repo_proto = Path::new(&manifest_dir).join("../../proto");
    let proto_root = if repo_proto.exists() {
        repo_proto.canonicalize().unwrap_or(repo_proto)
    } else {
        Path::new(&manifest_dir).join("proto")
    };

    let mut protos = Vec::new();
    collect_protos(&proto_root, &mut protos);
    protos.sort();
    assert!(!protos.is_empty(), "no .proto files found under {}", proto_root.display());

    // Rerun whenever any tracked .proto file changes (not just the directory
    // entry), so contract edits actually regenerate the Rust types.
    for p in &protos {
        println!("cargo:rerun-if-changed={}", p.display());
    }

    let file_strs: Vec<String> = protos.iter().map(|p| p.to_string_lossy().into_owned()).collect();
    let file_refs: Vec<&str> = file_strs.iter().map(String::as_str).collect();

    connectrpc_build::Config::new()
        .files(&file_refs)
        .includes(&[proto_root.to_string_lossy().into_owned()])
        .include_file("_connectrpc.rs")
        .compile()
        .expect("failed to compile longtrader contract protos");
}
