"""Shared configuration loaded from environment variables."""

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class FabricConfig:
    subscription_id: str = field(
        default_factory=lambda: os.environ["AZURE_SUBSCRIPTION_ID"]
    )
    resource_group: str = field(
        default_factory=lambda: os.environ["AZURE_RESOURCE_GROUP"]
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


@dataclass(frozen=True)
class OneLakeConfig:
    account_name: str = field(
        default_factory=lambda: os.getenv("ONELAKE_ACCOUNT_NAME", "onelake")
    )
    workspace_name: str = field(
        default_factory=lambda: os.environ["ONELAKE_WORKSPACE_NAME"]
    )
    lakehouse_name: str = field(
        default_factory=lambda: os.environ["ONELAKE_LAKEHOUSE_NAME"]
    )

    @property
    def account_url(self) -> str:
        return f"https://{self.account_name}.dfs.fabric.microsoft.com"


@dataclass(frozen=True)
class AIFoundryConfig:
    project_endpoint: str = field(
        default_factory=lambda: os.environ["AZURE_AI_PROJECT_ENDPOINT"]
    )
    model_deployment: str = field(
        default_factory=lambda: os.getenv(
            "AZURE_AI_MODEL_DEPLOYMENT_NAME", "gpt-4o-mini"
        )
    )
    bing_connection_id: str | None = field(
        default_factory=lambda: os.getenv("BING_CONNECTION_ID")
    )


@dataclass(frozen=True)
class AppConfig:
    fabric: FabricConfig = field(default_factory=FabricConfig)
    onelake: OneLakeConfig = field(default_factory=OneLakeConfig)
    ai: AIFoundryConfig = field(default_factory=AIFoundryConfig)
