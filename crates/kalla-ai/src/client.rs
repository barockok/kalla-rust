//! LLM API client for recipe generation

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

/// LLM provider configuration
#[derive(Debug, Clone)]
pub enum LlmProvider {
    OpenAI { api_key: String, model: String },
    Anthropic { api_key: String, model: String },
}

/// LLM client for generating recipes
pub struct LlmClient {
    provider: LlmProvider,
    client: reqwest::Client,
}

impl LlmClient {
    pub fn new(provider: LlmProvider) -> Self {
        Self {
            provider,
            client: reqwest::Client::new(),
        }
    }

    /// Create a client from environment variables
    pub fn from_env() -> Result<Self> {
        // Try OpenAI first, then Anthropic
        if let Ok(api_key) = std::env::var("OPENAI_API_KEY") {
            let model = std::env::var("OPENAI_MODEL").unwrap_or_else(|_| "gpt-4o".to_string());
            Ok(Self::new(LlmProvider::OpenAI { api_key, model }))
        } else if let Ok(api_key) = std::env::var("ANTHROPIC_API_KEY") {
            let model = std::env::var("ANTHROPIC_MODEL")
                .unwrap_or_else(|_| "claude-sonnet-4-20250514".to_string());
            Ok(Self::new(LlmProvider::Anthropic { api_key, model }))
        } else {
            anyhow::bail!("No LLM API key found. Set OPENAI_API_KEY or ANTHROPIC_API_KEY")
        }
    }

    /// Generate a response from the LLM
    pub async fn generate(&self, system_prompt: &str, user_prompt: &str) -> Result<String> {
        match &self.provider {
            LlmProvider::OpenAI { api_key, model } => {
                self.call_openai(api_key, model, system_prompt, user_prompt)
                    .await
            }
            LlmProvider::Anthropic { api_key, model } => {
                self.call_anthropic(api_key, model, system_prompt, user_prompt)
                    .await
            }
        }
    }

    async fn call_openai(
        &self,
        api_key: &str,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
    ) -> Result<String> {
        #[derive(Serialize)]
        struct OpenAIRequest {
            model: String,
            messages: Vec<OpenAIMessage>,
            temperature: f32,
        }

        #[derive(Serialize)]
        struct OpenAIMessage {
            role: String,
            content: String,
        }

        #[derive(Deserialize)]
        struct OpenAIResponse {
            choices: Vec<OpenAIChoice>,
        }

        #[derive(Deserialize)]
        struct OpenAIChoice {
            message: OpenAIMessageContent,
        }

        #[derive(Deserialize)]
        struct OpenAIMessageContent {
            content: String,
        }

        let request = OpenAIRequest {
            model: model.to_string(),
            messages: vec![
                OpenAIMessage {
                    role: "system".to_string(),
                    content: system_prompt.to_string(),
                },
                OpenAIMessage {
                    role: "user".to_string(),
                    content: user_prompt.to_string(),
                },
            ],
            temperature: 0.1,
        };

        debug!("Calling OpenAI API with model: {}", model);

        let response = self
            .client
            .post("https://api.openai.com/v1/chat/completions")
            .header("Authorization", format!("Bearer {}", api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await?;
            anyhow::bail!("OpenAI API error ({}): {}", status, body);
        }

        let response: OpenAIResponse = response.json().await?;
        let content = response
            .choices
            .first()
            .map(|c| c.message.content.clone())
            .ok_or_else(|| anyhow::anyhow!("No response from OpenAI"))?;

        info!("Received response from OpenAI");
        Ok(content)
    }

    async fn call_anthropic(
        &self,
        api_key: &str,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
    ) -> Result<String> {
        #[derive(Serialize)]
        struct AnthropicRequest {
            model: String,
            max_tokens: u32,
            system: String,
            messages: Vec<AnthropicMessage>,
        }

        #[derive(Serialize)]
        struct AnthropicMessage {
            role: String,
            content: String,
        }

        #[derive(Deserialize)]
        struct AnthropicResponse {
            content: Vec<AnthropicContent>,
        }

        #[derive(Deserialize)]
        struct AnthropicContent {
            text: String,
        }

        let request = AnthropicRequest {
            model: model.to_string(),
            max_tokens: 4096,
            system: system_prompt.to_string(),
            messages: vec![AnthropicMessage {
                role: "user".to_string(),
                content: user_prompt.to_string(),
            }],
        };

        debug!("Calling Anthropic API with model: {}", model);

        let response = self
            .client
            .post("https://api.anthropic.com/v1/messages")
            .header("x-api-key", api_key)
            .header("anthropic-version", "2023-06-01")
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await?;
            anyhow::bail!("Anthropic API error ({}): {}", status, body);
        }

        let response: AnthropicResponse = response.json().await?;
        let content = response
            .content
            .first()
            .map(|c| c.text.clone())
            .ok_or_else(|| anyhow::anyhow!("No response from Anthropic"))?;

        info!("Received response from Anthropic");
        Ok(content)
    }
}
