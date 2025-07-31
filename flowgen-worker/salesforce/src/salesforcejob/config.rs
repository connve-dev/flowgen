use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Processor for creating Salesforce bulk jobs.:
/// ```json
/// {
///     "salesforce_query": {
///    "label": "salesforce_query_job",
///         "credentials": "/etc/sfdc_dev.json",
///         "operation": "query",
///         "query": "Select Id from Account",
///         "content_type": "CSV",
///        "column_delimiter": "CARET",
///         "line_ending": "CRLF"
///     }
///  }
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Processor {
    /// Optional human-readable label for identifying this subscriber configuration.
    pub label: Option<String>,
    /// Reference to credential store entry containing Salesforce authentication details.
    pub credentials: String,
    /// SOQL query to create the bulk job.
    pub query: String,
    /// Operation name related to Salesforce bulk job.
    pub operation: Operation,
    /// Output file format for the bulk job.
    pub content_type: ContentType,
    /// Column delimeter for output file for the bulk job.
    pub column_delimiter: ColumnDelimiter,
    /// Line ending for output file for the bulk job.
    pub line_ending: LineEnding,
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum Operation {
    /// Defaults to query job.
    #[default]
    query,
    ///insert,
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum ContentType {
    #[default]
    CSV, ///currently only supports CSV.
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum ColumnDelimiter {
    /// Defaults to comma as column delimiter.
    #[default]
    COMMA,
    TAB,
    SEMICOLON,
    PIPE,
    CARET,
    BACKQUOTE
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
enum LineEnding {
    /// Defaults to CRLF as line ending.
    #[default]
    CRLF,
    LF,
}

impl ConfigExt for Processor {}
