use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Configuration for the data writer component.
///
/// This struct defines the necessary parameters to configure how data
/// should be written to a target destination. It includes credentials,
/// the target path, the write operation mode, an optional predicate
/// for merge operations, and optional creation parameters if the
/// target needs to be created.
///
/// # Example: Append Operation
///
/// ```json
/// {
///     "writer": {
///         "credentials": "my_secret_credentials",
///         "path": "/path/to/target/data",
///         "operation": "Append"
///     }
/// }
/// ```
///
/// # Example: Merge Operation with Creation Options
///
/// ```json
/// {
///     "writer": {
///         "credentials": "connection_string_or_token",
///         "path": "database/schema/table_name",
///         "operation": "Merge",
///         "predicate": "target.id = source.id",
///         "create_options": {
///             "columns": [
///                 {"name": "id", "data_type": "Utf8", "nullable": false},
///                 {"name": "value", "data_type": "Utf8", "nullable": true},
///                 {"name": "timestamp", "data_type": "Utf8", "nullable": false}
///             ]
///         }
///     }
/// }
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Writer {
    /// Credentials required for accessing the target data store or system.
    /// The specific format depends on the target (e.g., connection string, token, etc.).
    pub credentials: String,
    /// Path identifying the target location (e.g., file path, table identifier).
    pub path: PathBuf,
    /// The writing operation to perform. See `Operation` enum.
    pub operation: Operation,
    /// An optional condition used primarily for the `Merge` operation.
    /// This string typically defines how source and target records are matched
    /// (e.g., `"target.id = source.id"`).
    pub predicate: Option<String>,
    /// Optional parameters for creating the target resource (e.g., a table)
    /// if it does not already exist. See `CreateOpts`.
    pub create_options: CreateOptions,
}

/// Defines the properties of a single column, typically used for schema definition.
///
/// Part of `CreateOpts` to specify the structure of a target table or dataset
/// when it needs to be created.
///
/// # Example:
///
/// ```json
/// {
///     "name": "user_email",
///     "data_type": "Utf8",
///     "nullable": true
/// }
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Column {
    /// The name of the column.
    pub name: String,
    /// The data type of the column. See `DataType` enum.
    pub data_type: DataType,
    /// Indicates whether the column can contain null (missing) values.
    pub nullable: bool,
}

/// Specifies the data type for a column.
///
/// Currently, only a limited set of types might be defined.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum DataType {
    /// Represents a UTF-8 encoded string type. (Default)
    #[default]
    Utf8,
    // Add other potential data types here in the future, e.g.:
    // Int64, Float64, Boolean, Timestamp, etc.
}

/// Defines the write strategy or operation mode for the writer.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum Operation {
    /// Appends new data to the target. If the target is a table,
    /// new rows are added. If it's a file, data is appended to the end. (Default)
    #[default]
    Append,
    /// Merges new data with existing data in the target based on a predicate.
    /// This typically involves updating existing records that match the predicate
    /// and inserting new records that don't match. Requires `predicate` to be set
    /// in the `Writer` configuration.
    Merge,
    // Add other potential operations here, e.g.:
    // Overwrite, Upsert, etc.
}

/// Options specifically for creating the target resource if it doesn't exist.
///
/// Used within the `Writer` configuration when the operation might need
/// to initialize the target structure (e.g., create a database table).
///
/// # Example:
///
/// ```json
/// {
///     "create_options": {
///         "columns": [
///             {"name": "id", "data_type": "Utf8", "nullable": false},
///             {"name": "data", "data_type": "Utf8", "nullable": true}
///         ]
///     }
/// }
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct CreateOptions {
    /// A list of column definitions (`Column`) specifying the schema
    /// of the target to be created.
    pub create_if_not_exist: bool,
    pub columns: Option<Vec<Column>>,
}
