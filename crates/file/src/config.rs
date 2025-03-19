use serde::{Deserialize, Serialize};

#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
pub struct Source {
    pub path: String,
}

#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
pub struct Target {
    pub path: String,
}
