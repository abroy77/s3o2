use crate::errors::S3O2Error;
pub fn parse_uri(uri: &str) -> Result<(String, String), S3O2Error> {
    let uri = match uri.strip_prefix("s3://") {
        Some(uri) => uri,
        None => {
            return Err(S3O2Error::Value(format!(
                "Could not parse S3 uri.\n \
            Must be in format 's3://<bucket>/<key>.\n \
            Received {uri}"
            )));
        }
    };
    if uri.is_empty() {
        return Err(S3O2Error::Value(
            "S3 URI cannot be empty after s3://".to_string(),
        ));
    }

    let (bucket, key) = match uri.split_once('/') {
        Some((b, k)) => (b, k),
        None => (uri, ""),
    };

    if bucket.is_empty() {
        return Err(S3O2Error::Value("Bucket name cannot be empty".to_string()));
    }
    validate_bucket_name(bucket)?;

    Ok((bucket.to_string(), key.to_string()))
}

fn validate_bucket_name(bucket: &str) -> Result<(), S3O2Error> {
    if !(3..=63).contains(&bucket.len()) {
        return Err(S3O2Error::Value(
            "Bucket name must be between 3 and 63 characters long".to_string(),
        ));
    }
    if !bucket
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
    {
        return Err(S3O2Error::Value(
            "Bucket can only contain lowercase characters, digits, hyphens, and periods."
                .to_string(),
        ));
    }
    if bucket.starts_with('-') || bucket.ends_with('-') {
        return Err(S3O2Error::Value(
            "Bucket cannot start or end with a hyphen".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_parse_uri_success() {
        let input = "s3://bucket/key";
        let (expected_bucket, expected_key) = ("bucket".to_owned(), "key".to_owned());
        let parsed = parse_uri(input).unwrap();
        assert_eq!(parsed, (expected_bucket, expected_key));
    }
    #[test]
    fn test_parse_uri_empty() {
        let input = "";
        let result = parse_uri(input);
        assert!(matches!(result, Err(S3O2Error::Value(_))));
    }
    #[test]
    fn test_parse_uri_bucket_empty() {
        let input = "s3:://chicken";
        let result = parse_uri(input);
        assert!(matches!(result, Err(S3O2Error::Value(_))));
    }
    #[test]
    fn test_parse_uri_no_s3_prefix() {
        let input = "bucket/key";
        let result = parse_uri(input);
        assert!(matches!(result, Err(S3O2Error::Value(_))));
    }
    #[test]
    fn test_parse_uri_no_slash() {
        let input = "s3://bucket";
        let (expected_bucket, expected_prefix) = ("bucket".to_string(), "".to_string());
        let parsed = parse_uri(input).unwrap();
        assert_eq!(parsed, (expected_bucket, expected_prefix));
    }
}
