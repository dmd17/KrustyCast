//! Links Aeron's C client, built from source (1.54.0, the cluster's
//! version) with `make aeron_static`: AERON_C_LIB (default
//! ~/.local/share/krusty-tools/aeron-c-src/aeron-1.54.0/build/lib).
fn main() {
    let dir = std::env::var("AERON_C_LIB").unwrap_or_else(|_| {
        format!(
            "{}/.local/share/krusty-tools/aeron-c-src/aeron-1.54.0/build/lib",
            std::env::var("HOME").unwrap()
        )
    });
    println!("cargo:rerun-if-env-changed=AERON_C_LIB");
    println!("cargo:rustc-link-search=native={dir}");
    println!("cargo:rustc-link-lib=static=aeron_static");
    for l in ["bsd", "pthread", "m", "dl", "uuid"] {
        println!("cargo:rustc-link-lib={l}");
    }
}
