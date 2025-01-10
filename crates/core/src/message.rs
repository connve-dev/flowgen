use arrow::{
    array::{Array, RecordBatch, StringArray},
    datatypes::{DataType, Field},
    ipc::writer::StreamWriter,
};
use serde::Serialize;
use serde_json::ser;
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Cannot parse config file")]
    ParseConfig(#[source] serde_json::Error),
    #[error("There was an error deserializing data into binary format.")]
    Arrow(#[source] arrow::error::ArrowError),
}
pub trait RecordBatchExt {
    type Error;
    fn to_recordbatch(&self) -> Result<arrow::array::RecordBatch, Self::Error>;
}

impl RecordBatchExt for serde_json::Value {
    type Error = Error;
    fn to_recordbatch(&self) -> Result<arrow::array::RecordBatch, Self::Error> {
        let map = self.as_object().unwrap();
        let mut fields = Vec::new();
        let mut values = Vec::new();

        for (key, value) in map {
            fields.push(Field::new(key, DataType::Utf8, true));
            let array = StringArray::from(vec![Some(value.to_string())]);
            values.push(Arc::new(array));
        }

        let columns = values
            .into_iter()
            .map(|x| x as Arc<dyn Array>)
            .collect::<Vec<Arc<dyn Array>>>();

        let schema = arrow::datatypes::Schema::new(fields);
        let batch = RecordBatch::try_new(Arc::new(schema), columns).unwrap();
        Ok(batch)
    }
}

impl TryFrom<Message> for Vec<u8> {
    type Error = Error;
    fn try_from(value: Message) -> Result<Self, Self::Error> {
        let buffer: Vec<u8> = Vec::new();
        let mut stream_writer =
            StreamWriter::try_new(buffer, &value.data.schema()).map_err(Error::Arrow)?;
        stream_writer.write(&value.data).map_err(Error::Arrow)?;
        stream_writer.finish().map_err(Error::Arrow)?;
        Ok(stream_writer.get_mut().to_vec())
    }
}

#[derive(Debug, Clone)]
pub struct Message {
    pub data: arrow::array::RecordBatch,
    pub subject: String,
}
