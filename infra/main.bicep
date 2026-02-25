// ─────────────────────────────────────────────────────────────────────
// infra/main.bicep
// Main orchestrator — provisions all Azure resources for the
// Confluence ETL + AI Foundry Agent pipeline.
//
// Resources deployed:
//   1. Microsoft Fabric capacity (F2)
//   2. Azure AI Search with managed identity
//   3. Azure AI Services (OpenAI) with gpt-4o deployment,
//      Foundry-native project, and AI Search connection
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

@description('Embedding model deployment name')
param embeddingDeploymentName string = 'text-embedding-3-large'

@description('Embedding model capacity (TPM in thousands)')
param embeddingModelCapacity int = 10

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
var foundryProjectName = 'confluence-agent'
var searchConnectionName = 'confluence-search'

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
    embeddingDeploymentName: embeddingDeploymentName
    embeddingModelCapacity: embeddingModelCapacity
    projectName: foundryProjectName
    searchConnectionName: searchConnectionName
    searchServiceEndpoint: 'https://${searchServiceName}.search.windows.net/'
    searchServiceId: aiSearch.outputs.id
    searchApiKey: aiSearch.outputs.adminKey
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
output AZURE_OPENAI_PRINCIPAL_ID string = openai.outputs.principalId

// Foundry project
output AI_FOUNDRY_PROJECT_NAME string = openai.outputs.projectName
output AI_FOUNDRY_PROJECT_ENDPOINT string = openai.outputs.projectEndpoint
output AI_FOUNDRY_PROJECT_PRINCIPAL_ID string = openai.outputs.projectPrincipalId
output AI_SEARCH_CONNECTION_NAME string = openai.outputs.searchConnectionName

// Embedding model
output AZURE_OPENAI_EMBEDDING_DEPLOYMENT string = openai.outputs.embeddingDeploymentName
