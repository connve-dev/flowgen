use flowgen_core::input::Input;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;

#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
pub struct Source {
    pub path: String,
}

#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
pub struct Target {
    pub path: String,
}
