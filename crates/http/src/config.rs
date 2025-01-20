use flowgen_core::config::Input;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize, Clone, Debug)]
pub struct Processor {
    pub endpoint: String,
    pub payload: Option<HashMap<String, String>>,
    pub headers: Option<HashMap<String, String>>,
    pub credentials: Option<String>,
    pub inputs: Option<HashMap<String, Input>>,
}
