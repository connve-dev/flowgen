use deltalake::arrow::{
    array::{Array, RecordBatch, TimestampMicrosecondArray, TimestampMillisecondArray},
    datatypes::{DataType, Field, Schema, TimeUnit},
};
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("error with an Apache Arrow data")]
    Arrow(#[source] arrow::error::ArrowError),
}
pub trait EventExt {
    type Error;
    fn adjust_data_precision(&mut self) -> Result<(), Self::Error>;
}

impl EventExt for flowgen_core::stream::event::Event {
    type Error = Error;
    fn adjust_data_precision(&mut self) -> Result<(), Self::Error> {
        let columns = self.data.columns();
        let schema = self.data.schema();

        let mut new_fields: Vec<Arc<Field>> = Vec::new();
        let mut new_columns = Vec::new();

        for (i, field) in schema.fields().iter().enumerate() {
            match field.data_type() {
                DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                    // Update field to microsecond precision
                    let new_field = Arc::new(Field::new(
                        field.name(),
                        DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                        field.is_nullable(),
                    ));
                    new_fields.push(new_field);

                    // Convert column data
                    let old_array = &columns[i];
                    let millis_array = old_array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();

                    let micros_data: Vec<Option<i64>> = millis_array
                        .iter()
                        .map(|val| val.map(|ms| ms * 1000))
                        .collect();

                    let new_array = TimestampMicrosecondArray::from(micros_data);
                    new_columns.push(Arc::new(new_array) as Arc<dyn Array>);
                }
                _ => {
                    new_fields.push(field.clone());
                    new_columns.push(columns[i].clone());
                }
            }
        }

        let new_schema = Arc::new(Schema::new(new_fields));
        let new_batch = RecordBatch::try_new(new_schema, new_columns).map_err(Error::Arrow)?;

        self.data = new_batch;
        Ok(())
    }
}
