"""
Fabric Capacity Infrastructure Provisioning.

Provisions and manages Azure Fabric capacities using the azure-mgmt-fabric SDK.
"""

from __future__ import annotations

import time

from azure.identity import DefaultAzureCredential
from azure.mgmt.fabric import FabricMgmtClient
from azure.mgmt.fabric.models import (
    CapacitySku,
    CheckNameAvailabilityRequest,
    FabricCapacity,
    FabricCapacityAdministration,
    FabricCapacityProperties,
    FabricCapacityUpdate,
)

from config.settings import FabricConfig
from src.utils.logging import get_logger

log = get_logger(__name__)


class FabricProvisioner:
    """Provisions and manages Azure Fabric capacities."""

    def __init__(self, config: FabricConfig) -> None:
        self._cfg = config
        self._credential = DefaultAzureCredential()
        self._client = FabricMgmtClient(
            credential=self._credential,
            subscription_id=self._cfg.subscription_id,
        )

    # ── Capacity Lifecycle ───────────────────────────────────────────

    def check_name_available(self) -> bool:
        """Return True if the configured capacity name is available."""
        result = self._client.fabric_capacities.check_name_availability(
            location=self._cfg.location,
            body=CheckNameAvailabilityRequest(
                name=self._cfg.capacity_name,
                type="Microsoft.Fabric/capacities",
            ),
        )
        if not result.name_available:
            log.warning(
                "name_unavailable", reason=result.reason, name=self._cfg.capacity_name
            )
        return result.name_available

    def provision(self) -> FabricCapacity:
        """Create or update the Fabric capacity (LRO)."""
        log.info(
            "provisioning_capacity",
            name=self._cfg.capacity_name,
            sku=self._cfg.sku,
            location=self._cfg.location,
        )

        poller = self._client.fabric_capacities.begin_create_or_update(
            resource_group_name=self._cfg.resource_group,
            capacity_name=self._cfg.capacity_name,
            resource=FabricCapacity(
                location=self._cfg.location,
                sku=CapacitySku(name=self._cfg.sku, tier="Fabric"),
                properties=FabricCapacityProperties(
                    administration=FabricCapacityAdministration(
                        members=[self._cfg.admin_email],
                    ),
                ),
            ),
        )

        # Poll with logging
        while not poller.done():
            log.info("provision_polling", status=poller.status())
            time.sleep(10)

        capacity = poller.result()
        log.info(
            "capacity_provisioned",
            name=capacity.name,
            state=capacity.properties.state,
            sku=capacity.sku.name,
        )
        return capacity

    def get_capacity(self) -> FabricCapacity:
        """Retrieve current capacity details."""
        return self._client.fabric_capacities.get(
            resource_group_name=self._cfg.resource_group,
            capacity_name=self._cfg.capacity_name,
        )

    def scale(self, target_sku: str) -> FabricCapacity:
        """Scale the capacity to a different SKU (e.g. F2 -> F4)."""
        log.info("scaling_capacity", from_sku=self._cfg.sku, to_sku=target_sku)
        return self._client.fabric_capacities.begin_update(
            resource_group_name=self._cfg.resource_group,
            capacity_name=self._cfg.capacity_name,
            properties=FabricCapacityUpdate(
                sku=CapacitySku(name=target_sku, tier="Fabric"),
            ),
        ).result()

    def suspend(self) -> None:
        """Pause the capacity to stop billing."""
        log.info("suspending_capacity", name=self._cfg.capacity_name)
        self._client.fabric_capacities.begin_suspend(
            resource_group_name=self._cfg.resource_group,
            capacity_name=self._cfg.capacity_name,
        ).result()
        log.info("capacity_suspended")

    def resume(self) -> None:
        """Resume a paused capacity."""
        log.info("resuming_capacity", name=self._cfg.capacity_name)
        self._client.fabric_capacities.begin_resume(
            resource_group_name=self._cfg.resource_group,
            capacity_name=self._cfg.capacity_name,
        ).result()
        log.info("capacity_resumed")

    def delete(self) -> None:
        """Delete the capacity permanently."""
        log.warning("deleting_capacity", name=self._cfg.capacity_name)
        self._client.fabric_capacities.begin_delete(
            resource_group_name=self._cfg.resource_group,
            capacity_name=self._cfg.capacity_name,
        ).result()
        log.info("capacity_deleted")

    def list_capacities(self) -> list[FabricCapacity]:
        """List all capacities in the resource group."""
        return list(
            self._client.fabric_capacities.list_by_resource_group(
                resource_group_name=self._cfg.resource_group,
            )
        )

    def list_available_skus(self) -> list[dict]:
        """List SKUs available for this capacity."""
        skus = self._client.fabric_capacities.list_skus(
            resource_group_name=self._cfg.resource_group,
            capacity_name=self._cfg.capacity_name,
        )
        return [{"name": s.name, "tier": s.tier} for s in skus]
