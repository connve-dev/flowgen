use flowgen_core::connect::client::Client as FlowgenClientTrait;
use std::path::PathBuf;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Nats Client Auth error returned.
    #[error(transparent)]
    NatsClientAuth(#[from] crate::client::Error),
    #[error(transparent)]
    NatsKVEntry(#[from] async_nats::jetstream::kv::EntryError),
    #[error(transparent)]
    NatsKVPut(#[from] async_nats::jetstream::kv::PutError),
    #[error(transparent)]
    NatsKVBucketCreate(#[from] async_nats::jetstream::context::CreateKeyValueError),
    /// An expected buffer value was empty.
    #[error("no value in provided buffer")]
    EmptyBuffer(),
    /// Internal error: The Cache Store reference was unexpectedly missing.
    #[error("missing required value Cache Store")]
    MissingCacheStore(),
    /// An expected attribute or configuration value was missing.
    #[error("missing required event attribute")]
    MissingRequiredAttribute(String),
}

#[derive(Debug, Default)]
pub struct Cache {
    credentials_path: PathBuf,
    store: Option<async_nats::jetstream::kv::Store>,
}

impl flowgen_core::cache::Cache for Cache {
    type Error = Error;

    async fn init(mut self, bucket: &str) -> Result<Self, Self::Error> {
        let client = crate::client::ClientBuilder::new()
            .credentials_path(self.credentials_path.clone())
            .build()
            .map_err(Error::NatsClientAuth)?
            .connect()
            .await
            .map_err(Error::NatsClientAuth)?;

        let jetstream = client.jetstream.unwrap();
        let store = match jetstream.get_key_value(bucket).await {
            Ok(store) => store,
            Err(_) => jetstream
                .create_key_value(async_nats::jetstream::kv::Config {
                    bucket: bucket.to_string(),
                    history: 10,
                    ..Default::default()
                })
                .await
                .map_err(Error::NatsKVBucketCreate)?,
        };

        self.store = Some(store);
        Ok(self)
    }

    async fn put(&self, key: &str, value: bytes::Bytes) -> Result<(), Self::Error> {
        let store = self.store.as_ref().ok_or(Error::MissingCacheStore())?;
        store.put(key, value).await.map_err(Error::NatsKVPut)?;
        Ok(())
    }
    async fn get(&self, key: &str) -> Result<bytes::Bytes, Self::Error> {
        let store = self.store.as_ref().ok_or(Error::MissingCacheStore())?;
        let bytes = store
            .get(key)
            .await
            .map_err(Error::NatsKVEntry)?
            .ok_or(Error::EmptyBuffer())?;
        Ok(bytes)
    }
}

#[derive(Default)]
pub struct CacheBuilder {
    credentials_path: Option<PathBuf>,
}

impl CacheBuilder {
    /// Creates a new `CacheBuilder` with default values.
    pub fn new() -> CacheBuilder {
        CacheBuilder {
            ..Default::default()
        }
    }

    /// Sets the broadcast channel receiver for incoming events.
    ///
    /// # Arguments
    /// * `receiver` - The `Receiver<Event>` end of the broadcast channel.
    pub fn credentials_path(mut self, credentials_path: PathBuf) -> Self {
        self.credentials_path = Some(credentials_path);
        self
    }

    /// Builds the `Cache` instance.
    ///
    /// Consumes the builder and returns a `Writer` if all required fields (`config`, `rx`)
    /// have been set.
    ///
    /// # Returns
    /// * `Ok(Writer)` if construction is successful.
    /// * `Err(Error::MissingRequiredAttribute)` if `config` or `rx` was not provided.
    pub fn build(self) -> Result<Cache, Error> {
        Ok(Cache {
            credentials_path: self
                .credentials_path
                .ok_or_else(|| Error::MissingRequiredAttribute("credentials".to_string()))?,
            store: None,
        })
    }
}
