use super::bucket::Bucket;

pub struct S3 {
    client: aws_sdk_s3::Client,
}

impl S3 {
    /// Create an `S3` resource with the AWS configuration loaded from the
    /// environment.
    pub async fn default() -> Self {
        let aws_sdk_config = aws_config::from_env().load().await;
        Self::with_aws_sdk_config(&aws_sdk_config)
    }

    /// Create an `S3` resource with the provided AWS `config`.
    pub fn with_aws_sdk_config(config: &aws_config::SdkConfig) -> Self {
        Self {
            client: aws_sdk_s3::Client::new(config),
        }
    }

    pub fn bucket(&self, name: String) -> Bucket {
        Bucket::new(name, self.client.clone())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum S3Error {
    /// An error that happened during an AWS S3 api operation.
    #[error(transparent)]
    AWSS3Error(aws_sdk_s3::Error),
    #[error("{0}")]
    Other(String),
}
