use arrow::{
    array::{MapArray, StringArray},
    datatypes::DataType,
};
use flowgen_core::{client::Client, event::Event};
use handlebars::Handlebars;
use salesforce_pubsub::eventbus::v1::{
    ProducerEvent, PublishRequest, SchemaInfo, SchemaRequest, TopicRequest,
};
use serde_json::Value;
use std::{collections::HashMap, str::FromStr, sync::Arc};
use tokio::{
    sync::{broadcast::Receiver, Mutex},
    task::JoinHandle,
};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error with PubSub context.")]
    FlowgenSalesforcePubSub(#[source] super::context::Error),
    #[error("There was an error with Salesforce authentication.")]
    FlowgenSalesforceAuth(#[source] crate::client::Error),
    #[error("Missing required event attrubute.")]
    MissingRequiredAttribute(String),
}

pub struct Publisher {
    service: flowgen_core::service::Service,
    config: super::config::Target,
    rx: Receiver<Event>,
    current_task_id: usize,
}

impl Publisher {
    pub async fn publish(mut self) -> Result<(), Error> {
        let handlebars = Handlebars::new();

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

        let pubsub = Arc::new(Mutex::new(pubsub));

        let topic_info = pubsub
            .lock()
            .await
            .get_topic(TopicRequest {
                topic_name: self.config.topic.clone(),
            })
            .await
            .unwrap()
            .into_inner();

        let schema_info = pubsub
            .lock()
            .await
            .get_schema(SchemaRequest {
                schema_id: topic_info.schema_id,
            })
            .await
            .unwrap()
            .into_inner();

        let pubsub = pubsub.clone();
        tokio::spawn(async move {
            let topic_name = &self.config.topic;
            let schema_id = &schema_info.schema_id;
            while let Ok(event) = self.rx.recv().await {
                if event.current_task_id == Some(self.current_task_id - 1) {
                    let mut data = HashMap::new();
                    if let Some(inputs) = &self.config.inputs {
                        for (key, input) in inputs {
                            let value = input.extract_from(&event.data, &event.extensions);
                            if let Ok(value) = value {
                                data.insert(key.to_string(), value.to_string());
                            }
                        }
                    }

                    let template = serde_json::to_string(&self.config.payload).unwrap();
                    let payload = handlebars.render_template(&template, &data).unwrap();
                    let value = serde_json::Value::from_str(&payload).unwrap();

                    let mut bytes: Vec<u8> = Vec::new();
                    serde_json::to_writer(&mut bytes, &value).unwrap();

                    let mut events = Vec::new();
                    let pe = ProducerEvent {
                        schema_id: schema_id.to_string(),
                        payload: bytes,
                        ..Default::default()
                    };

                    println!("{:?}", value);

                    events.push(pe);
                    let test = pubsub
                        .lock()
                        .await
                        .publish(PublishRequest {
                            topic_name: topic_name.to_string(),
                            events,
                            ..Default::default()
                        })
                        .await
                        .unwrap();
                    println!("{:?}", test);
                }
            }
        });
        Ok(())
    }
}

#[derive(Default)]
pub struct PublisherBuilder {
    service: Option<flowgen_core::service::Service>,
    config: Option<super::config::Target>,
    rx: Option<Receiver<Event>>,
    current_task_id: usize,
}

impl PublisherBuilder {
    pub fn new() -> PublisherBuilder {
        PublisherBuilder {
            ..Default::default()
        }
    }

    pub fn service(mut self, service: flowgen_core::service::Service) -> Self {
        self.service = Some(service);
        self
    }

    pub fn config(mut self, config: super::config::Target) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = current_task_id;
        self
    }

    pub async fn build(self) -> Result<Publisher, Error> {
        Ok(Publisher {
            service: self
                .service
                .ok_or_else(|| Error::MissingRequiredAttribute("data".to_string()))?,
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("subject".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("subject".to_string()))?,
            current_task_id: self.current_task_id,
        })
    }
}
