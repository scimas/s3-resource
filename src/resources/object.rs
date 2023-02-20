use std::{
    collections::HashMap,
    io::{self, Read, Seek},
    ops::RangeInclusive,
};

use tokio::io::AsyncRead;

pub struct Object {
    pub bucket_name: String,
    pub key: String,
    position: usize,
    length: Option<usize>,
    last_modified: Option<aws_sdk_s3::types::DateTime>,
    client: aws_sdk_s3::Client,
}

impl Object {
    pub(crate) fn new(bucket_name: String, key: String, client: aws_sdk_s3::Client) -> Self {
        Self {
            bucket_name,
            key,
            position: 0usize,
            length: None,
            last_modified: None,
            client,
        }
    }

    pub async fn get(&self) -> Result<aws_sdk_s3::types::ByteStream, ObjectOperationError> {
        let get_object_request = self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(&self.key);
        let response = get_object_request.send().await?;
        Ok(response.body)
    }

    pub async fn reader(&mut self) -> Result<impl AsyncRead, ObjectOperationError> {
        Ok(self.get().await?.into_async_read())
    }

    async fn get_range(
        &self,
        range: RangeInclusive<usize>,
    ) -> Result<aws_sdk_s3::types::ByteStream, ObjectOperationError> {
        let get_object_request = self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(&self.key)
            .range(format!(
                "bytes={start}-{end}",
                start = range.start(),
                end = range.end()
            ));
        let response = get_object_request.send().await?;
        Ok(response.body)
    }

    pub async fn refresh_metadata(&mut self) -> Result<(), ObjectOperationError> {
        let mut head_object_request = self
            .client
            .head_object()
            .bucket(&self.bucket_name)
            .key(&self.key);
        if let Some(dt) = &self.last_modified {
            head_object_request = head_object_request.if_modified_since(*dt);
        }
        let response = head_object_request.send().await;
        match response {
            Err(sdk_err) => match &sdk_err {
                aws_sdk_s3::types::SdkError::ServiceError(svc_err) => {
                    if let Some(code) = svc_err.err().code() {
                        if code.starts_with("304") {
                            Ok(())
                        } else {
                            Err(ObjectOperationError::from(sdk_err))
                        }
                    } else {
                        Err(ObjectOperationError::from(sdk_err))
                    }
                }
                _ => Err(ObjectOperationError::from(sdk_err)),
            },
            Ok(output) => {
                self.length = Some(usize::try_from(output.content_length()).map_err(|_| {
                    ObjectOperationError::Other {
                        msg: "object content length does not fit into into a usize".into(),
                        data: HashMap::from([
                            ("bucket_name".into(), self.bucket_name.clone()),
                            ("key".into(), self.key.clone()),
                        ]),
                    }
                })?);
                self.last_modified = output.last_modified().cloned();
                Ok(())
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ObjectOperationError {
    #[error(transparent)]
    GetObject(#[from] aws_sdk_s3::types::SdkError<aws_sdk_s3::error::GetObjectError>),
    #[error(transparent)]
    HeadObject(#[from] aws_sdk_s3::types::SdkError<aws_sdk_s3::error::HeadObjectError>),
    #[error("{msg}")]
    Other {
        msg: String,
        data: HashMap<String, String>,
    },
}

impl Read for Object {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.length.is_none() {
            let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect(
                "expect to be able to build a tokio runtime, without which the rest of the code cannot be executed",
            );
            rt.block_on(self.refresh_metadata())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        }
        if self.position >= self.length.unwrap() {
            return Ok(0);
        }
        let num_bytes_to_read = buf.len();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect(
                "expect to be able to build a tokio runtime, without which the rest of the code cannot be executed",
            );
        match rt.block_on(self.get_range(self.position..=(self.position + num_bytes_to_read - 1))) {
            Err(ooe) => match ooe {
                ObjectOperationError::GetObject(sdk_err) => match sdk_err {
                    aws_sdk_s3::types::SdkError::TimeoutError(_) => {
                        Err(io::Error::new(io::ErrorKind::TimedOut, sdk_err))
                    }
                    aws_sdk_s3::types::SdkError::ConstructionFailure(_)
                    | aws_sdk_s3::types::SdkError::DispatchFailure(_)
                    | aws_sdk_s3::types::SdkError::ResponseError(_) => {
                        Err(io::Error::new(io::ErrorKind::Other, sdk_err))
                    }
                    aws_sdk_s3::types::SdkError::ServiceError(svc_err) => {
                        let go_error = svc_err.into_err();
                        match go_error.kind {
                            aws_sdk_s3::error::GetObjectErrorKind::InvalidObjectState(_) => {
                                Err(io::Error::new(io::ErrorKind::InvalidData, go_error))
                            }
                            aws_sdk_s3::error::GetObjectErrorKind::NoSuchKey(_) => {
                                Err(io::Error::new(io::ErrorKind::NotFound, go_error))
                            }
                            aws_sdk_s3::error::GetObjectErrorKind::Unhandled(_) => {
                                Err(io::Error::new(io::ErrorKind::Other, go_error))
                            }
                            _ => Err(io::Error::new(io::ErrorKind::Other, go_error)),
                        }
                    }
                    _ => Err(io::Error::new(io::ErrorKind::Other, sdk_err)),
                },
                _ => unreachable!("received a type of ObjectOperationError from get_range that should not be possible!!!")
            },
            Ok(byte_stream) => match rt.block_on(byte_stream.collect()) {
                Err(err) => Err(io::Error::new(io::ErrorKind::Other, err)),
                Ok(agg_bytes) => {
                    let bytes = agg_bytes.into_bytes();
                    let received_num_bytes = bytes.len();
                    buf[..received_num_bytes].copy_from_slice(&bytes);
                    self.position += received_num_bytes;
                    Ok(received_num_bytes)
                }
            },
        }
    }
}

impl Seek for Object {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        if self.length.is_none() {
            let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect(
                "expect to be able to build a tokio runtime, without which the rest of the code cannot be executed",
            );
            rt.block_on(self.refresh_metadata())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        }
        match pos {
            io::SeekFrom::Start(s) => {
                if s > u64::try_from(self.length.unwrap()).map_err(|_| {
                    io::Error::new(io::ErrorKind::Other, "object length is too long for u64")
                })? {
                    self.position = self.length.unwrap();
                } else {
                    self.position = usize::try_from(s).map_err(|_| {
                        io::Error::new(io::ErrorKind::Other, "seek offset is too large for usize")
                    })?;
                }
            }
            io::SeekFrom::End(s) => {
                if s >= 0 {
                    return self.seek(io::SeekFrom::Start(
                        u64::try_from(self.length.unwrap()).map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                "object length is too long for u64",
                            )
                        })?,
                    ));
                } else if usize::try_from(-s).map_err(|_| {
                    io::Error::new(io::ErrorKind::Other, "seek offset is too large for usize")
                })? > self.length.unwrap()
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "tried to seek to a negative offset",
                    ));
                } else {
                    self.position = self.length.unwrap()
                        - usize::try_from(-s).map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                "seek offset is too large for usize",
                            )
                        })?;
                }
            }
            io::SeekFrom::Current(s) => {
                if s >= 0 {
                    return self.seek(io::SeekFrom::Start(
                        u64::try_from(self.position).map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                "current offset is too large for u64",
                            )
                        })? + u64::try_from(s).unwrap(),
                    ));
                } else if usize::try_from(-s).map_err(|_| {
                    io::Error::new(io::ErrorKind::Other, "seek offset is too large for usize")
                })? > self.position
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "tried to seek to a negative offset",
                    ));
                } else {
                    self.position -= usize::try_from(-s).map_err(|_| {
                        io::Error::new(io::ErrorKind::Other, "seek offset is too large for usize")
                    })?;
                }
            }
        }
        u64::try_from(self.position).map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "seeked position is too large for u64")
        })
    }
}
