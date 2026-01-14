pub mod api;
pub mod backend;
pub mod error;
pub mod metadata;

/// Store Schema Version, must be updated on any breaking change
/// An upgrade to a newer schema version invalidates currently stored data, requiring a re-sync.
pub const STORE_SCHEMA_VERSION: u64 = 1;

/// Name of the file storing the metadata about the database
pub const STORE_METADATA_FILENAME: &str = "metadata.json";
