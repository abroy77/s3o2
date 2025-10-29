use std::{
    fs::File,
    io::{BufWriter, Write},
};

use aws_config::BehaviorVersion;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bucket_name = "roy-s3o2";
    let key = "texty.txt";
    let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
    // dbg!(&config);
    let s3_client = aws_sdk_s3::Client::new(&config);
    let head = s3_client
        .head_object()
        .bucket(bucket_name)
        .key(key)
        .send()
        .await?;
    let size_mb = head.content_length().unwrap() as f64 / (1024.0 * 1024.0);
    println!("about to download {:.2}mb", size_mb);
    let mut result = s3_client
        .get_object()
        .bucket("roy-s3o2")
        .key("texty.txt")
        .send()
        .await?;
    // dbg!(&result);
    let file = File::create("downloaded.txt").unwrap();
    let mut writer = BufWriter::new(file);
    let mut byte_count = 0;
    while let Some(bytes) = result.body.try_next().await? {
        let bytes_len = bytes.len();
        writer.write_all(&bytes)?;
        byte_count += bytes_len;
    }
    println!("bytes downloaded {}", byte_count);

    Ok(())
}
