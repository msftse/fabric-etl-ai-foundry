// ─────────────────────────────────────────────────────────────────────
// infra/modules/storage.bicep
// Deploys a Storage Account for AI Foundry Hub.
// ─────────────────────────────────────────────────────────────────────

@description('Name of the storage account (3-24 chars, lowercase alphanumeric)')
param name string

@description('Location for the storage account')
param location string

param tags object = {}

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: name
  location: location
  tags: tags
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    supportsHttpsTrafficOnly: true
    minimumTlsVersion: 'TLS1_2'
    encryption: {
      services: {
        blob: { enabled: true }
        file: { enabled: true }
      }
      keySource: 'Microsoft.Storage'
    }
    accessTier: 'Hot'
  }
}

output id string = storageAccount.id
output name string = storageAccount.name
