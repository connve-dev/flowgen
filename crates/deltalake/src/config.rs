#[derive(serde::Deserialize, Clone, Debug)]
pub struct Target {
    pub credentials: String,
    pub uri: String,
}
