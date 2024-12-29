use super::config;
use arrow::array::RecordBatch;
use async_nats::jetstream::context::Publish;
use chrono::Utc;
use flowgen_core::{client::Client, message::ChannelMessage};
use flowgen_file::subscriber::RecordBatchConverter;
use flowgen_salesforce::pubsub::subscriber::ProducerEventConverter;
use futures::future::{try_join_all, TryJoinAll};
use std::{any::Any, ops::DerefMut, path::PathBuf, sync::Arc};
use tokio::{
    sync::{
        broadcast::{Receiver, Sender},
        Mutex,
    },
    task::JoinHandle,
};
use tracing::{error, event, info, Level};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Cannot open/read the credentials file at path {1}")]
    OpenFile(#[source] std::io::Error, PathBuf),
    #[error("Cannot parse config file")]
    ParseConfig(#[source] serde_json::Error),
    #[error("Cannot setup Flowgen Client")]
    FlowgenService(#[source] flowgen_core::service::Error),
    #[error("Failed to setup Salesforce PubSub as flow source.")]
    FlowgenSalesforcePubSubSubscriberError(#[source] flowgen_salesforce::pubsub::subscriber::Error),
    #[error("There was an error with Flowgen Nats JetStream Publisher.")]
    FlowgenNatsJetStreamPublisher(#[source] flowgen_nats::jetstream::publisher::Error),
    #[error("There was an error with Flowgen Nats JetStream Subscriber.")]
    FlowgenNatsJetStreamSubscriber(#[source] flowgen_nats::jetstream::subscriber::Error),
    #[error("There was an error with Flowgen File Subscriber.")]
    FlowgenFileSubscriberError(#[source] flowgen_file::subscriber::Error),
    #[error("Failed to publish message to Nats Jetstream.")]
    NatsPublish(#[source] async_nats::jetstream::context::PublishError),
    #[error("Cannot execute async task.")]
    TokioJoin(#[source] tokio::task::JoinError),
}

#[allow(non_camel_case_types)]
pub enum Source {
    file(flowgen_file::subscriber::Subscriber),
    salesforce_pubsub(flowgen_salesforce::pubsub::subscriber::Subscriber),
    gcp_storage(flowgen_google::storage::subscriber::Subscriber),
    nats_jetstream(flowgen_nats::jetstream::subscriber::Subscriber),
}

#[allow(non_camel_case_types)]
pub enum Processor {}

#[allow(non_camel_case_types)]
pub enum Target {
    nats_jetstream(flowgen_nats::jetstream::publisher::Publisher),
    deltalake(flowgen_deltalake::publisher::Publisher),
}

pub struct Flow {
    config: config::Config,
    // source_channel: Option<Channel>,
    // target_channel: Option<Channel>,
    pub source: Option<Source>,
    pub processor: Option<Processor>,
    pub target: Option<Target>,
}

impl Flow {
    pub async fn run(self) -> Result<Self, Error> {
        // Setup Flowgen service.
        let service = flowgen_core::service::Builder::new()
            .with_endpoint(format!("{0}:443", "https://api.pubsub.salesforce.com"))
            .build()
            .map_err(Error::FlowgenService)?
            .connect()
            .await
            .map_err(Error::FlowgenService)?;

        let config = self.config.clone();

        let mut task_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        let (tx, mut rx): (Sender<ChannelMessage>, Receiver<ChannelMessage>) =
            tokio::sync::broadcast::channel(1000);

        // Setup source subscribers.
        match config.flow.source {
            config::Source::nats_jetstream(config) => {
                flowgen_nats::jetstream::subscriber::Builder::new(config, &tx)
                    .build()
                    .await
                    .map_err(Error::FlowgenNatsJetStreamSubscriber)?
                    .subscribe()
                    .await
                    .map_err(Error::FlowgenNatsJetStreamSubscriber)?;
            }
            config::Source::file(config) => {
                flowgen_file::subscriber::Builder::new(config, &tx)
                    .build()
                    .await
                    .map_err(Error::FlowgenFileSubscriberError)?
                    .subscribe()
                    .await
                    .map_err(Error::FlowgenFileSubscriberError)?;
            }
            config::Source::salesforce_pubsub(config) => {
                flowgen_salesforce::pubsub::subscriber::Builder::new(service.clone(), config, &tx)
                    .build()
                    .await
                    .map_err(Error::FlowgenSalesforcePubSubSubscriberError)?
                    .subscribe()
                    .await
                    .map_err(Error::FlowgenSalesforcePubSubSubscriberError)?;
            }
            _ => {
                info!("unimplemented");
            }
        }

        // Setup target publishers.
        match config.flow.target {
            config::Target::nats_jetstream(config) => {
                let publisher = flowgen_nats::jetstream::publisher::Builder::new(config)
                    .build()
                    .await
                    .map_err(Error::FlowgenNatsJetStreamPublisher)?;

                // let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                while let Ok(message) = rx.recv().await {
                    match message {
                        ChannelMessage::file(m) => {
                            let event = m.record_batch.to_bytes().unwrap();
                            let subject = format!("filedrop.in.{}", m.file_chunk);

                            publisher
                                .jetstream
                                .send_publish(subject, Publish::build().payload(event.into()))
                                .await
                                .map_err(Error::NatsPublish)?;

                            event!(Level::INFO, "file_chunk: {}", m.file_chunk);
                        }
                        ChannelMessage::salesforce_pubsub(m) => {
                            for ce in m.fetch_response.events {
                                if let Some(pe) = ce.event {
                                    let event = pe.to_bytes().unwrap();
                                    let s =
                                        m.topic_info.topic_name.replace('/', ".").to_lowercase();
                                    let event_name = &s[1..];
                                    let subject =
                                        format!("salesforce.pubsub.in.{}.{}", event_name, pe.id);

                                    publisher
                                        .jetstream
                                        .send_publish(
                                            subject,
                                            Publish::build().payload(event.into()),
                                        )
                                        .await
                                        .map_err(Error::NatsPublish)?;

                                    event!(Level::INFO, "salesforce_pubsub: {}", pe.id);
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            config::Target::deltalake(config) => {
                // let publisher = flowgen_deltalake::publisher::Builder::new(config)
                //     .build()
                //     .await
                //     .unwrap();

                let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                    while let Ok(message) = rx.recv().await {
                        println!("{:?}", "delta_target");
                        // match message {
                        //     ChannelMessage::FileMessage(m) => {
                        //         let event = m.record_batch.to_bytes().unwrap();
                        //         let subject = format!("filedrop.in.{}", m.file_chunk);
                        //         publisher
                        //             .jetstream
                        //             .send_publish(subject, Publish::build().payload(event.into()))
                        //             .await
                        //             .map_err(Error::NatsPublish)?;
                        //         event!(Level::INFO, "event: file processed {}", m.file_chunk);
                        //     }
                        // }
                    }
                    Ok(())
                });
                task_list.push(task);
            }
        }

        tokio::spawn(async move { task_list.into_iter().collect::<TryJoinAll<_>>().await });
        Ok(self)
    }
}

#[derive(Default)]
pub struct Builder {
    config_path: PathBuf,
}

impl Builder {
    pub fn new(config_path: PathBuf) -> Builder {
        Builder { config_path }
    }
    pub fn build(&mut self) -> Result<Flow, Error> {
        let c = std::fs::read_to_string(&self.config_path)
            .map_err(|e| Error::OpenFile(e, self.config_path.clone()))?;
        let config: config::Config = serde_json::from_str(&c).map_err(Error::ParseConfig)?;
        let f = Flow {
            config,
            // processor_receiver: None,
            // target_receiver: None,
            // target_channel: None,
            // source_channel: None,
            source: None,
            processor: None,
            target: None,
        };
        Ok(f)
    }
}
