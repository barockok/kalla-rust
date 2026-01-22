//! Kalla AI - LLM integration for natural language to recipe generation
//!
//! This crate provides:
//! - Schema extraction from data sources (no PII)
//! - Prompt building for LLM recipe generation
//! - LLM API client (OpenAI/Anthropic)
//! - Recipe validation against schemas

pub mod schema_extractor;
pub mod prompt;
pub mod client;

pub use schema_extractor::{SanitizedSchema, ColumnMeta, extract_schema};
pub use client::LlmClient;
