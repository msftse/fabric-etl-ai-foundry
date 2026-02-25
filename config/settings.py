"""Shared configuration loaded from environment variables."""

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class FabricConfig:
    subscription_id: str = field(
        default_factory=lambda: os.getenv("AZURE_SUBSCRIPTION_ID", "")
    )
    resource_group: str = field(
        default_factory=lambda: os.getenv("AZURE_RESOURCE_GROUP", "")
    )
    capacity_name: str = field(
        default_factory=lambda: os.getenv("FABRIC_CAPACITY_NAME", "etl-fabric-capacity")
    )
    location: str = field(
        default_factory=lambda: os.getenv("FABRIC_LOCATION", "eastus")
    )
    sku: str = field(default_factory=lambda: os.getenv("FABRIC_SKU", "F2"))
    admin_email: str = field(
        default_factory=lambda: os.getenv("FABRIC_ADMIN_EMAIL", "admin@contoso.com")
    )
    workspace_id: str = field(
        default_factory=lambda: os.getenv("FABRIC_WORKSPACE_ID", "")
    )
    lakehouse_id: str = field(
        default_factory=lambda: os.getenv("FABRIC_LAKEHOUSE_ID", "")
    )

    @property
    def is_configured(self) -> bool:
        return bool(self.subscription_id and self.resource_group)


@dataclass(frozen=True)
class OneLakeConfig:
    account_name: str = field(
        default_factory=lambda: os.getenv("ONELAKE_ACCOUNT_NAME", "onelake")
    )
    workspace_name: str = field(
        default_factory=lambda: os.getenv("ONELAKE_WORKSPACE_NAME", "")
    )
    lakehouse_name: str = field(
        default_factory=lambda: os.getenv("ONELAKE_LAKEHOUSE_NAME", "")
    )

    @property
    def is_configured(self) -> bool:
        return bool(self.workspace_name and self.lakehouse_name)

    @property
    def account_url(self) -> str:
        return f"https://{self.account_name}.dfs.fabric.microsoft.com"


@dataclass(frozen=True)
class SnowflakeConfig:
    account: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_ACCOUNT", ""))
    user: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_USER", ""))
    password: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_PASSWORD", ""))
    warehouse: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_WAREHOUSE", ""))
    database: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_DATABASE", ""))
    schema: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"))
    role: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_ROLE", ""))

    @property
    def is_configured(self) -> bool:
        return bool(self.account and self.user and self.database)


@dataclass(frozen=True)
class AIFoundryConfig:
    project_endpoint: str = field(
        default_factory=lambda: os.getenv("AZURE_AI_PROJECT_ENDPOINT", "")
    )
    model_deployment: str = field(
        default_factory=lambda: os.getenv("AZURE_AI_MODEL_DEPLOYMENT_NAME", "gpt-4o")
    )
    bing_connection_id: str | None = field(
        default_factory=lambda: os.getenv("BING_CONNECTION_ID")
    )
    # Fabric Data Agent connection (Foundry IQ -> OneLake)
    fabric_connection_id: str | None = field(
        default_factory=lambda: os.getenv("FABRIC_CONNECTION_ID")
    )
    # Foundry IQ knowledge base (OneLake files as knowledge source)
    ai_search_connection_id: str | None = field(
        default_factory=lambda: os.getenv("AI_SEARCH_CONNECTION_ID")
    )


@dataclass(frozen=True)
class ConfluenceConfig:
    url: str = field(default_factory=lambda: os.getenv("CONFLUENCE_URL", ""))
    email: str = field(default_factory=lambda: os.getenv("CONFLUENCE_EMAIL", ""))
    api_token: str = field(
        default_factory=lambda: os.getenv("CONFLUENCE_API_TOKEN", "")
    )

    @property
    def is_configured(self) -> bool:
        return bool(self.url and self.email and self.api_token)


@dataclass(frozen=True)
class AppConfig:
    fabric: FabricConfig = field(default_factory=FabricConfig)
    onelake: OneLakeConfig = field(default_factory=OneLakeConfig)
    snowflake: SnowflakeConfig = field(default_factory=SnowflakeConfig)
    ai: AIFoundryConfig = field(default_factory=AIFoundryConfig)
    confluence: ConfluenceConfig = field(default_factory=ConfluenceConfig)
