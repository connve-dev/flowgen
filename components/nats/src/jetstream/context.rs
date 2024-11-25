use flowgen_core::client::Client;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error with connecting to a Nats Server.")]
    FlowgenNatsClientAuth(#[source] crate::client::Error),
    #[error("Nats Client is missing / not initialized properly.")]
    FlowgenNatsClientMissing(),
    #[error("Failed to publish message to a nats jetstream")]
    Publish(#[source] async_nats::jetstream::context::PublishError),
}

pub struct Context {
    pub jetstream: async_nats::jetstream::Context,
}

pub struct Builder {
    config: super::config::Target,
}

impl Builder {
    // Creates a new instance of a Builder.
    pub fn new(config: super::config::Target) -> Builder {
        Builder { config }
    }

    pub async fn build(self) -> Result<Context, Error> {
        // Connect to Nats Server.
        let client = crate::client::Builder::new()
            .with_credentials_path(self.config.credentials.into())
            .build()
            .map_err(Error::FlowgenNatsClientAuth)?
            .connect()
            .await
            .map_err(Error::FlowgenNatsClientAuth)?;

        if let Some(nats_client) = client.nats_client {
            let context = async_nats::jetstream::new(nats_client);
            Ok(Context { jetstream: context })
        } else {
            Err(Error::FlowgenNatsClientMissing())
        }
    }
}
