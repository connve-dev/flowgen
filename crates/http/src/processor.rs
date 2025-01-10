use arrow::{
    array::{RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use flowgen_core::message::Message;
use futures_util::future::TryJoinAll;
use serde_json::Value;
use std::sync::Arc;
use tokio::{
    sync::broadcast::{Receiver, Sender},
    task::JoinHandle,
};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error reading/writing/seeking file.")]
    InputOutput(#[source] std::io::Error),
    #[error("There was an error executing async task.")]
    TokioJoin(#[source] tokio::task::JoinError),
    #[error("There was an error with sending message over channel.")]
    TokioSendMessage(#[source] tokio::sync::broadcast::error::SendError<Message>),
}

pub struct Processor {
    handle_list: Vec<JoinHandle<Result<(), Error>>>,
}

impl Processor {
    pub async fn process(self) -> Result<(), Error> {
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

/// A builder of the http processor.
pub struct Builder {
    config: super::config::Processor,
    tx: Sender<Message>,
    rx: Receiver<Message>,
}

impl Builder {
    /// Creates a new instance of a Builder.
    pub fn new(config: super::config::Processor, tx: &Sender<Message>) -> Builder {
        Builder {
            config,
            tx: tx.clone(),
            rx: tx.subscribe(),
        }
    }

    pub async fn build(mut self) -> Result<Processor, Error> {
        let mut handle_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        let client = reqwest::Client::builder().https_only(true).build().unwrap();

        let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            while let Ok(m) = self.rx.recv().await {
                println!("{:?}", m);
                let response = client
                    .get(self.config.endpoint.as_str())
                    .send()
                    .await
                    .unwrap()
                    .json::<Value>()
                    .await
                    .unwrap();

                let mut fields = Vec::new();
                let mut values = Vec::new();

                for (key, value) in response.as_object().unwrap() {
                    fields.push(Field::new(key, DataType::Utf8, false));
                    values.push(value.to_string());
                }

                let schema = Schema::new(fields);
                let arrays = StringArray::from(values);
                let data = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arrays)]).unwrap();
                let subject = "test".to_string();

                let m = Message { data, subject };
                self.tx.send(m).map_err(Error::TokioSendMessage)?;
            }

            Ok(())
        });

        handle_list.push(handle);

        Ok(Processor { handle_list })
    }
}
