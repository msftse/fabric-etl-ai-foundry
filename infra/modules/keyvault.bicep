// ─────────────────────────────────────────────────────────────────────
// infra/modules/keyvault.bicep
// Deploys Azure Key Vault for AI Foundry Hub.
// ─────────────────────────────────────────────────────────────────────

@description('Name of the Key Vault')
param name string

@description('Location for the Key Vault')
param location string

param tags object = {}

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: name
  location: location
  tags: tags
  properties: {
    tenantId: subscription().tenantId
    sku: {
      family: 'A'
      name: 'standard'
    }
    enableRbacAuthorization: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 90
    accessPolicies: []
  }
}

output id string = keyVault.id
output name string = keyVault.name
