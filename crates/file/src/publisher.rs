use arrow::csv::WriterBuilder;
use chrono::Utc;
use flowgen_core::event::Event;
use futures::future::try_join_all;
use std::{fs::File, sync::Arc};
use tokio::{sync::broadcast::Receiver, task::JoinHandle};
use tracing::{event, Level};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error opening/creating file")]
    IO(#[source] std::io::Error),
    #[error("error deserializing data into binary format")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("missing required event attrubute")]
    MissingRequiredAttribute(String),
}

pub struct Publisher {
    config: Arc<super::config::Target>,
    rx: Receiver<Event>,
    current_task_id: usize,
}

impl flowgen_core::publisher::Publisher for Publisher {
    type Error = Error;
    async fn publish(mut self) -> Result<(), Self::Error> {
        let mut handle_list = Vec::new();

        while let Ok(event) = self.rx.recv().await {
            let config = Arc::clone(&self.config);
            let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                if event.current_task_id == Some(self.current_task_id - 1) {
                    let file = File::create(&config.path).map_err(Error::IO)?;

                    WriterBuilder::new()
                        .with_header(true)
                        .build(file)
                        .write(&event.data)
                        .map_err(Error::Arrow)?;

                    let timestamp = Utc::now().timestamp_micros();
                    let filename = match &config.path.split("/").last() {
                        Some(filename) => filename,
                        None => "filename",
                    };

                    let subject = format!("{}.{}", filename, timestamp);

                    event!(Level::INFO, "event processed: {}", subject);
                }
                Ok(())
            });

            handle_list.push(handle);
        }

        let _ = try_join_all(handle_list.iter_mut()).await;

        Ok(())
    }
}

#[derive(Default)]
pub struct PublisherBuilder {
    config: Option<Arc<super::config::Target>>,
    rx: Option<Receiver<Event>>,
    current_task_id: usize,
}

impl PublisherBuilder {
    pub fn new() -> PublisherBuilder {
        PublisherBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Target>) -> Self {
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
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
            current_task_id: self.current_task_id,
        })
    }
}
