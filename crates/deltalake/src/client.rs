//! # Delta Lake Client Module.
//!
//! This module provides a client implementation for interacting with Delta Lake tables,
//! potentially hosted on Google Cloud Platform (GCP), as indicated by the use of
//! `deltalake_gcp::register_handlers`.
//!
//! It defines:
//! - `Client`: A struct representing the connection parameters and the active Delta table connection.
//!   It implements the `flowgen_core::connect::client::Client` trait.
//! - `ClientBuilder`: A builder pattern for constructing `Client` instances.
//! - `Error`: An enum encompassing errors specific to client operations (connection, creation).
//!
//! The client handles both connecting to existing Delta tables and creating new ones
//! if they don't exist, provided that `create_options` are supplied with the
//! `create_if_not_exist` flag enabled and a valid schema (`columns`).

use deltalake::{
    kernel::{DataType, PrimitiveType, StructField},
    DeltaOps, DeltaTable,
};
use std::{collections::HashMap, path::PathBuf};

// NOTE: Assuming `flowgen_core::connect::client::Client` trait and `super::config::CreateOptions` struct are defined elsewhere.
// E.g., pub struct CreateOptions { pub create_if_not_exist: bool, pub columns: Option<Vec<Column>> }

/// Errors that can occur during Delta Lake client operations (connection, creation).
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Error originating from the Parquet library used by Delta Lake.
    #[error(transparent)]
    Parquet(#[from] deltalake::parquet::errors::ParquetError),
    /// Error originating from Delta Lake table operations (opening, creating).
    #[error(transparent)]
    DeltaTable(#[from] deltalake::DeltaTableError),
    /// Error occurring when joining Tokio tasks (if applicable in calling code).
    #[error(transparent)]
    TaskJoin(#[from] tokio::task::JoinError),
    /// An expected attribute or configuration value was missing.
    #[error("missing required event attrubute")]
    // Note: "attrubute" typo exists in original code
    MissingRequiredAttribute(String),
    /// The required `path` configuration for the Delta table was not provided or invalid.
    #[error("missing required config value path")]
    MissingPath(),
    /// Could not extract a filename from the configured Delta table path.
    #[error("no filename in provided path")]
    EmptyFileName(),
    /// An expected string value was empty (e.g., filename conversion).
    #[error("no value in provided str")]
    EmptyStr(),
}

/// Represents a client connection to a Delta Lake table.
///
/// Stores credentials, path, optional creation parameters, and holds the
/// active `DeltaTable` instance once connected.
pub struct Client {
    /// Credentials required for accessing the Delta table storage (e.g., GCP service account key).
    credentials: String,
    /// The storage path (URI) to the Delta Lake table.
    path: PathBuf,
    /// Optional parameters used only when creating the table if it doesn't exist.
    /// Contains flags like `create_if_not_exist` and the schema (`columns`).
    create_options: Option<super::config::CreateOptions>,
    /// Holds the active `DeltaTable` instance after a successful `connect` call.
    /// Marked `pub(crate)` allowing access only within the same crate.
    pub(crate) table: Option<DeltaTable>,
}

/// Implementation of the core connection logic for the Delta Lake `Client`.
impl flowgen_core::connect::client::Client for Client {
    type Error = Error;

    /// Attempts to connect to the specified Delta Lake table.
    ///
    /// This method performs the following steps:
    /// 1. Registers GCP storage handlers using `deltalake_gcp`.
    /// 2. Prepares storage options using the provided `credentials`.
    /// 3. Tries to open the Delta table at the specified `path`.
    /// 4. If opening succeeds, the `DeltaTable` instance is stored in `self.table`.
    /// 5. If opening fails *and* `self.create_options` is provided *and* its
    ///    `create_if_not_exist` flag is true *and* it contains a `columns` definition:
    ///    a. Translates the configuration schema (`super::config::Column`) from `create_options.columns`
    ///       into Delta Lake `StructField`s.
    ///    b. Attempts to *create* a new Delta table at the `path` with the specified schema.
    ///    c. If creation succeeds, the new `DeltaTable` instance is stored in `self.table`.
    ///    d. If any condition in step 5 is not met (e.g., `create_options` is None,
    ///       `create_if_not_exist` is false, or `columns` are missing within options),
    ///       no creation attempt is made.
    /// 6. Returns the `Client` instance (potentially updated with the `table`) or an `Error`
    ///    if a fatal error occurred during connection or creation attempts.
    ///
    /// Consumes `self` and returns a new `Client` instance within the `Result`.
    async fn connect(mut self) -> Result<Client, Error> {
        // Ensure GCP storage handlers are registered for gcs:// paths.
        deltalake_gcp::register_handlers(None);
        let mut storage_options = HashMap::new();
        // Assuming credentials are a GCP service account JSON string.
        storage_options.insert(
            "google_service_account".to_string(),
            self.credentials.clone(),
        );

        let path = self.path.to_str().ok_or_else(Error::MissingPath)?;

        // Create DeltaOps for potential table creation.
        let ops = DeltaOps::try_from_uri_with_storage_options(path, storage_options.clone())
            .await
            .map_err(Error::DeltaTable)?;

        // Try opening the table first.
        match deltalake::open_table_with_storage_options(path, storage_options).await {
            Ok(table) => {
                self.table = Some(table);
            }
            Err(_) => {
                // Table likely doesn't exist or other error occurred.
                // Check if creation is requested and possible based on create_options.
                if let Some(ref create_options) = self.create_options {
                    // Only proceed if the flag explicitly allows creation.
                    if create_options.create_if_not_exist {
                        // Only proceed if a schema is actually provided within the options.
                        if let Some(ref config_columns) = create_options.columns {
                            let mut columns = Vec::new();

                            // Convert config schema to Delta Lake schema.
                            for c in config_columns {
                                let data_type = match c.data_type {
                                    crate::config::DataType::Utf8 => {
                                        DataType::Primitive(PrimitiveType::String)
                                    }
                                };
                                let struct_field =
                                    StructField::new(c.name.to_string(), data_type, c.nullable);
                                columns.push(struct_field);
                            }
                            // Attempt to create the table.
                            let table = ops
                                .create()
                                .with_columns(columns)
                                .await
                                .map_err(Error::DeltaTable)?;

                            self.table = Some(table);
                        }
                    }
                }
            }
        };
        Ok(self)
    }
}

/// Builder for configuring and creating a [`Client`] instance.
///
/// Provides an API for setting credentials, path, and optional table creation options
/// before constructing the `Client`.
#[derive(Default)]
pub struct ClientBuilder {
    /// Storage for credentials during building.
    credentials: Option<String>,
    /// Storage for the Delta table path during building.
    path: Option<PathBuf>,
    /// Storage for optional table creation parameters during building.
    create_options: Option<super::config::CreateOptions>,
}

impl ClientBuilder {
    /// Creates a new `ClientBuilder` with default values (all fields `None`).
    pub fn new() -> ClientBuilder {
        ClientBuilder {
            ..Default::default()
        }
    }

    /// Sets the credentials for the `Client`.
    ///
    /// # Arguments
    /// * `credentials` - A string containing the credentials (e.g., GCP service account key).
    pub fn credentials(mut self, credentials: String) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Sets the path to the Delta Lake table for the `Client`.
    ///
    /// # Arguments
    /// * `path` - A `PathBuf` representing the URI or local path to the table.
    pub fn path(mut self, path: PathBuf) -> Self {
        self.path = Some(path);
        self
    }

    /// Sets the optional table creation options for the `Client`.
    ///
    /// These options (like `create_if_not_exist` flag and the table schema `columns`)
    /// are used by the `connect` method only if the Delta table needs to be created
    /// because it doesn't already exist at the specified path.
    ///
    /// # Arguments
    /// * `create_options` - A [`super::config::CreateOptions`] struct containing parameters
    ///   needed for potential table creation.
    pub fn create_options(mut self, create_options: super::config::CreateOptions) -> Self {
        self.create_options = Some(create_options);
        self
    }

    /// Consumes the builder and creates a `Client` instance.
    ///
    /// This method verifies that required fields (`credentials`, `path`) have been set.
    /// The `table` field in the returned `Client` will be `None` initially; the connection
    /// and potential table creation happen when calling the `connect` method.
    ///
    /// # Returns
    /// * `Ok(Client)` if the required fields are set.
    /// * `Err(Error::MissingRequiredAttribute)` if `credentials` or `path` is missing.
    pub fn build(self) -> Result<Client, Error> {
        Ok(Client {
            credentials: self
                .credentials
                .ok_or_else(|| Error::MissingRequiredAttribute("credentials".to_string()))?,
            path: self
                .path
                .ok_or_else(|| Error::MissingRequiredAttribute("path".to_string()))?,
            create_options: self.create_options,
            table: None,
        })
    }
}
