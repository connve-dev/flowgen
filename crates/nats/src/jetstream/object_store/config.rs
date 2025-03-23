use serde::{Deserialize, Serialize};

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Source {
    pub credentials: String,
    pub bucket: String,
    pub stream: String,
    pub subject: String,
    pub durable_name: String,
    pub batch_size: Option<usize>,
    pub delay_secs: Option<u64>,
    pub has_header: Option<bool>,
}
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Target {
    pub credentials: String,
    pub stream: String,
    pub stream_description: Option<String>,
    pub subjects: Vec<String>,
    pub max_age: Option<u64>,
}
