//! Typed errors for the connectors crate.

use std::fmt;

/// Errors that can occur in data source connectors.
#[derive(Debug)]
pub enum ConnectorError {
    /// Failed to establish a connection to the data source.
    ConnectionFailed(String),
    /// The requested table does not exist.
    TableNotFound(String),
    /// Schema of the source does not match expectations.
    SchemaMismatch(String),
    /// The URI scheme is not supported by any connector.
    UnsupportedUri(String),
    /// A query against the data source failed.
    QueryFailed(String),
    /// Invalid or missing configuration.
    ConfigError(String),
}

impl fmt::Display for ConnectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectorError::ConnectionFailed(msg) => write!(f, "connection failed: {}", msg),
            ConnectorError::TableNotFound(msg) => write!(f, "table not found: {}", msg),
            ConnectorError::SchemaMismatch(msg) => write!(f, "schema mismatch: {}", msg),
            ConnectorError::UnsupportedUri(msg) => write!(f, "unsupported URI: {}", msg),
            ConnectorError::QueryFailed(msg) => write!(f, "query failed: {}", msg),
            ConnectorError::ConfigError(msg) => write!(f, "config error: {}", msg),
        }
    }
}

impl std::error::Error for ConnectorError {}

impl From<anyhow::Error> for ConnectorError {
    fn from(e: anyhow::Error) -> Self {
        ConnectorError::QueryFailed(e.to_string())
    }
}

impl From<sqlx::Error> for ConnectorError {
    fn from(e: sqlx::Error) -> Self {
        ConnectorError::ConnectionFailed(e.to_string())
    }
}

impl From<datafusion::error::DataFusionError> for ConnectorError {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        ConnectorError::QueryFailed(e.to_string())
    }
}
