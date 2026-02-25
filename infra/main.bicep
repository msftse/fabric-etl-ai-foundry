// ─────────────────────────────────────────────────────────────────────
// infra/main.bicep
// Main orchestrator — provisions all Azure resources for the
// Confluence ETL + AI Foundry Agent pipeline.
//
// Resources deployed:
//   1. Microsoft Fabric capacity (F2)
//   2. Azure AI Search with managed identity
//   3. Azure AI Services (OpenAI) with gpt-4o deployment
//   4. Storage Account (for AI Foundry Hub)
//   5. Key Vault (for AI Foundry Hub)
//   6. AI Foundry Hub + Project
//
// Data-plane items (Fabric workspace, lakehouse, notebooks, pipeline,
// AI agent) are created by post-provision Python scripts.
// ─────────────────────────────────────────────────────────────────────

targetScope = 'resourceGroup'

// ── Parameters ─────────────────────────────────────────────────────

@description('Primary location for all resources')
param location string

@description('Unique environment name used as a suffix for resource names')
param environmentName string

@description('Fabric capacity SKU')
@allowed(['F2', 'F4', 'F8', 'F16', 'F32', 'F64'])
param fabricSku string = 'F2'

@description('Fabric capacity admin email')
param fabricAdminEmail string

@description('AI Search SKU')
@allowed(['free', 'basic', 'standard', 'standard2', 'standard3'])
param searchSku string = 'standard'

@description('OpenAI model deployment name')
param openaiModelDeploymentName string = 'gpt-4o'

@description('OpenAI model capacity (TPM in thousands)')
param openaiModelCapacity int = 10

// ── Variables ──────────────────────────────────────────────────────

var resourceToken = toLower(uniqueString(subscription().id, resourceGroup().id, environmentName))
var tags = {
  'azd-env-name': environmentName
  project: 'fabric-etl-ai-foundry'
}

// Resource names
var fabricCapacityName = 'fc${resourceToken}'
var searchServiceName = 'search${resourceToken}'
var openaiServiceName = 'ai${resourceToken}'
var storageAccountName = 'st${resourceToken}'
var keyVaultName = 'kv${resourceToken}'
var hubName = 'hub-${resourceToken}'
var projectName = 'proj-${resourceToken}'

// ── Modules ────────────────────────────────────────────────────────

module fabricCapacity 'modules/fabric-capacity.bicep' = {
  name: 'fabricCapacity'
  params: {
    name: fabricCapacityName
    location: location
    skuName: fabricSku
    adminMembers: [fabricAdminEmail]
    tags: tags
  }
}

module aiSearch 'modules/ai-search.bicep' = {
  name: 'aiSearch'
  params: {
    name: searchServiceName
    location: location
    skuName: searchSku
    tags: tags
  }
}

module openai 'modules/openai.bicep' = {
  name: 'openai'
  params: {
    name: openaiServiceName
    location: location
    modelDeploymentName: openaiModelDeploymentName
    modelCapacity: openaiModelCapacity
    tags: tags
  }
}

module storage 'modules/storage.bicep' = {
  name: 'storage'
  params: {
    name: storageAccountName
    location: location
    tags: tags
  }
}

module keyVault 'modules/keyvault.bicep' = {
  name: 'keyVault'
  params: {
    name: keyVaultName
    location: location
    tags: tags
  }
}

module aiFoundry 'modules/ai-foundry.bicep' = {
  name: 'aiFoundry'
  params: {
    hubName: hubName
    projectName: projectName
    location: location
    storageAccountId: storage.outputs.id
    keyVaultId: keyVault.outputs.id
    aiServicesId: openai.outputs.id
    aiServicesEndpoint: openai.outputs.endpoint
    tags: tags
  }
}

// ── Outputs ────────────────────────────────────────────────────────
// These are automatically exported as environment variables for
// post-provision scripts via `azd env get-values`.

output AZURE_LOCATION string = location
output AZURE_TENANT_ID string = tenant().tenantId
output AZURE_RESOURCE_GROUP string = resourceGroup().name

// Fabric
output FABRIC_CAPACITY_ID string = fabricCapacity.outputs.id
output FABRIC_CAPACITY_NAME string = fabricCapacity.outputs.name

// AI Search
output AI_SEARCH_SERVICE_NAME string = aiSearch.outputs.name
output AI_SEARCH_SERVICE_ID string = aiSearch.outputs.id
output AI_SEARCH_PRINCIPAL_ID string = aiSearch.outputs.principalId

// OpenAI / AI Services
output AZURE_OPENAI_SERVICE_NAME string = openai.outputs.name
output AZURE_OPENAI_SERVICE_ID string = openai.outputs.id
output AZURE_OPENAI_ENDPOINT string = openai.outputs.endpoint
output AZURE_OPENAI_MODEL_DEPLOYMENT string = openaiModelDeploymentName

// AI Foundry
output AI_FOUNDRY_HUB_NAME string = aiFoundry.outputs.hubName
output AI_FOUNDRY_PROJECT_NAME string = aiFoundry.outputs.projectName
output AI_FOUNDRY_PROJECT_ENDPOINT string = aiFoundry.outputs.projectEndpoint
output AI_FOUNDRY_HUB_PRINCIPAL_ID string = aiFoundry.outputs.hubPrincipalId

// Storage & Key Vault
output STORAGE_ACCOUNT_NAME string = storage.outputs.name
output KEY_VAULT_NAME string = keyVault.outputs.name
