//! Kalla AI - LLM integration for natural language to recipe generation
//!
//! This crate provides:
//! - Schema extraction from data sources (no PII)
//! - Prompt building for LLM recipe generation
//! - LLM API client (OpenAI/Anthropic)
//! - Recipe validation against schemas

pub mod client;
pub mod prompt;
pub mod schema_extractor;

pub use client::LlmClient;
pub use schema_extractor::{extract_schema, ColumnMeta, SanitizedSchema};
