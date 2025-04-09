use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Writer {
    pub credentials: String,
    pub path: PathBuf,
}
