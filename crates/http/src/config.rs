use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize, Clone, Debug)]
pub struct Processor {
    pub endpoint: String,
    pub payload: Option<HashMap<String, String>>,
    pub headers: Option<HashMap<String, String>>,
    pub bearer_auth: Option<String>,
    pub inputs: Option<HashMap<String, String>>,
    pub outputs: Option<HashMap<String, String>>,
}
