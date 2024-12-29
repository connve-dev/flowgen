use flowgen_core::{
    client::Client,
    message::{ChannelMessage, SalesforcePubSubMessage},
};
use futures_util::future::TryJoinAll;
use salesforce_pubsub::eventbus::v1::{FetchRequest, ProducerEvent, TopicInfo, TopicRequest};
use std::sync::Arc;
use tokio::{
    sync::{broadcast::Sender, Mutex},
    task::JoinHandle,
};
use tokio_stream::StreamExt;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error with PubSub context.")]
    FlowgenSalesforcePubSub(#[source] super::context::Error),
    #[error("There was an error with Salesforce authentication.")]
    FlowgenSalesforceAuth(#[source] crate::client::Error),
    #[error("There was an error executing async task.")]
    TokioJoin(#[source] tokio::task::JoinError),
    #[error("There was an error with sending message over channel.")]
    TokioSendMessage(#[source] tokio::sync::broadcast::error::SendError<ChannelMessage>),
    #[error("There was an error deserializing data into binary format.")]
    Bincode(#[source] bincode::Error),
}

pub trait ProducerEventConverter {
    type Error;
    fn to_bytes(&self) -> Result<Vec<u8>, Self::Error>;
}

impl ProducerEventConverter for ProducerEvent {
    type Error = Error;
    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let event = bincode::serialize(&self).map_err(Error::Bincode)?;
        Ok(event)
    }
}

pub struct Subscriber {
    handle_list: Vec<JoinHandle<Result<(), Error>>>,
}

impl Subscriber {
    pub async fn subscribe(self) -> Result<(), Error> {
        tokio::spawn(async move {
            let _ = self
                .handle_list
                .into_iter()
                .collect::<TryJoinAll<_>>()
                .await
                .map_err(Error::TokioJoin);
        });
        Ok(())
    }
}

pub struct Builder {
    service: flowgen_core::service::Service,
    config: super::config::Source,
    tx: Sender<ChannelMessage>,
}

impl Builder {
    // Creates a new instance of a Builder.
    pub fn new(
        service: flowgen_core::service::Service,
        config: super::config::Source,
        tx: &Sender<ChannelMessage>,
    ) -> Builder {
        Builder {
            service,
            config,
            tx: tx.clone(),
        }
    }

    /// Builds a new FlowgenSalesforcePubsub Subscriber.
    pub async fn build(self) -> Result<Subscriber, Error> {
        // Connect to Salesforce.
        let sfdc_client = crate::client::Builder::new()
            .with_credentials_path(self.config.credentials.into())
            .build()
            .map_err(Error::FlowgenSalesforceAuth)?
            .connect()
            .await
            .map_err(Error::FlowgenSalesforceAuth)?;

        // Get PubSub context.
        let pubsub = super::context::Builder::new(self.service)
            .with_client(sfdc_client)
            .build()
            .map_err(Error::FlowgenSalesforcePubSub)?;

        let mut handle_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        let pubsub = Arc::new(Mutex::new(pubsub));

        for topic in self.config.topic_list.iter() {
            let pubsub: Arc<Mutex<super::context::Context>> = Arc::clone(&pubsub);
            let topic = topic.clone();

            let tx = self.tx.clone();
            let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                let topic_info: TopicInfo = pubsub
                    .lock()
                    .await
                    .get_topic(TopicRequest {
                        topic_name: topic.clone(),
                    })
                    .await
                    .map_err(Error::FlowgenSalesforcePubSub)?
                    .into_inner();

                let mut stream = pubsub
                    .lock()
                    .await
                    .subscribe(FetchRequest {
                        topic_name: topic,
                        num_requested: 200,
                        ..Default::default()
                    })
                    .await
                    .map_err(Error::FlowgenSalesforcePubSub)?
                    .into_inner();

                while let Some(received) = stream.next().await {
                    match received {
                        Ok(fr) => {
                            let m = SalesforcePubSubMessage {
                                fetch_response: fr,
                                topic_info: topic_info.clone(),
                            };
                            tx.send(ChannelMessage::salesforce_pubsub(m))
                                .map_err(Error::TokioSendMessage)?;
                        }
                        Err(e) => {
                            return Err(Error::FlowgenSalesforcePubSub(
                                super::context::Error::RPCFailed(e),
                            ));
                        }
                    }
                }
                Ok(())
            });
            handle_list.push(handle);
        }

        let s = Subscriber { handle_list };

        Ok(s)
    }
}
