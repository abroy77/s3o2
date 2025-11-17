#![allow(unused_imports)]
#[macro_export]
macro_rules! assert_file_content {
    ($path: expr, $expected: expr) => {
        assert!($path.exists(), "File should exist at {:?}", $path);
        let actual_contents = ::tokio::fs::read(&$path).await.unwrap();
        assert_eq!(actual_contents, $expected, "File contents mismatch");
    };
}
pub(crate) use assert_file_content;
