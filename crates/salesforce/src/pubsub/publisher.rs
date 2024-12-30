use flowgen_core::client::Client;
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error with PubSub context.")]
    FlowgenSalesforcePubSub(#[source] super::context::Error),
    #[error("There was an error with Salesforce authentication.")]
    FlowgenSalesforceAuth(#[source] crate::client::Error),
}

pub struct Publisher {
    pub pubsub: Arc<super::context::Context>,
}

pub struct Builder {
    service: flowgen_core::service::Service,
    config: super::config::Target,
}

impl Builder {
    // Creates a new instance of a Builder.
    pub fn new(config: super::config::Target, service: flowgen_core::service::Service) -> Builder {
        Builder { config, service }
    }

    pub async fn build(self) -> Result<Publisher, Error> {
        let sfdc_client = crate::client::Builder::new()
            .with_credentials_path(self.config.credentials.into())
            .build()
            .map_err(Error::FlowgenSalesforceAuth)?
            .connect()
            .await
            .map_err(Error::FlowgenSalesforceAuth)?;

        let pubsub = super::context::Builder::new(self.service)
            .with_client(sfdc_client)
            .build()
            .map_err(Error::FlowgenSalesforcePubSub)?;

        Ok(Publisher {
            pubsub: Arc::new(pubsub),
        })
    }
}
