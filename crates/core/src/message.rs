#[derive(Debug, Clone)]
pub struct FileMessage {
    pub record_batch: arrow::array::RecordBatch,
    pub file_chunk: String,
}

#[derive(Debug, Clone)]
pub struct SalesforcePubSubMessage {
    pub fetch_response: salesforce_pubsub::eventbus::v1::FetchResponse,
    pub topic_info: salesforce_pubsub::eventbus::v1::TopicInfo,
}

#[derive(Debug, Clone)]
#[allow(non_camel_case_types)]
pub enum ChannelMessage {
    file(FileMessage),
    nats_jetstream(async_nats::message::Message),
    salesforce_pubsub(SalesforcePubSubMessage),
}
