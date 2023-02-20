use super::object::Object;

pub struct Bucket {
    pub name: String,
    client: aws_sdk_s3::Client,
}

impl Bucket {
    pub(crate) fn new(name: String, client: aws_sdk_s3::Client) -> Self {
        Self { name, client }
    }

    pub fn object(&self, key: String) -> Object {
        Object::new(self.name.clone(), key, self.client.clone())
    }
}
