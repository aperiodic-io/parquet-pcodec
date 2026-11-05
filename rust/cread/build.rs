//! Gives the shared library a soname, so that programs linked against it (e.g. through the
//! CMake package's `tparquet::read`) record `libtparquet_read.so`, not the build path.
fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux") {
        println!("cargo:rustc-cdylib-link-arg=-Wl,-soname,libtparquet_read.so");
    }
}
