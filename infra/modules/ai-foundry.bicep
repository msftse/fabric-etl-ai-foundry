// ─────────────────────────────────────────────────────────────────────
// infra/modules/ai-foundry.bicep
// Deploys AI Foundry Hub + Project (Microsoft.MachineLearningServices/workspaces).
// ─────────────────────────────────────────────────────────────────────

@description('Name of the AI Foundry Hub')
param hubName string

@description('Name of the AI Foundry Project')
param projectName string

@description('Location for the AI Foundry resources')
param location string

@description('Resource ID of the Storage Account')
param storageAccountId string

@description('Resource ID of the Key Vault')
param keyVaultId string

@description('Resource ID of the AI Services (OpenAI) account')
param aiServicesId string

@description('Endpoint of the AI Services account')
param aiServicesEndpoint string

param tags object = {}

// ── AI Foundry Hub ─────────────────────────────────────────────────

resource hub 'Microsoft.MachineLearningServices/workspaces@2024-07-01-preview' = {
  name: hubName
  location: location
  tags: tags
  kind: 'Hub'
  identity: {
    type: 'SystemAssigned'
  }
  sku: {
    name: 'Basic'
    tier: 'Basic'
  }
  properties: {
    friendlyName: hubName
    description: 'AI Foundry Hub for Confluence ETL pipeline'
    storageAccount: storageAccountId
    keyVault: keyVaultId
    systemDatastoresAuthMode: 'identity'
    managedNetwork: {
      isolationMode: 'Disabled'
    }
    publicNetworkAccess: 'Enabled'
  }
}

// ── AI Services connection on the Hub ──────────────────────────────

resource aiServicesConnection 'Microsoft.MachineLearningServices/workspaces/connections@2024-07-01-preview' = {
  parent: hub
  name: '${hubName}-aiservices'
  properties: {
    category: 'AIServices'
    authType: 'AAD'
    isSharedToAll: true
    target: aiServicesEndpoint
    metadata: {
      ApiType: 'Azure'
      ResourceId: aiServicesId
    }
  }
}

// ── AI Foundry Project ─────────────────────────────────────────────

resource project 'Microsoft.MachineLearningServices/workspaces@2024-07-01-preview' = {
  name: projectName
  location: location
  tags: tags
  kind: 'Project'
  identity: {
    type: 'SystemAssigned'
  }
  sku: {
    name: 'Basic'
    tier: 'Basic'
  }
  properties: {
    friendlyName: projectName
    description: 'AI Foundry Project for Confluence data analysis agent'
    hubResourceId: hub.id
  }
  dependsOn: [
    aiServicesConnection
  ]
}

output hubId string = hub.id
output hubName string = hub.name
output hubPrincipalId string = hub.identity.principalId
output projectId string = project.id
output projectName string = project.name
output projectEndpoint string = 'https://${hub.name}.services.ai.azure.com/api/projects/${project.name}'
