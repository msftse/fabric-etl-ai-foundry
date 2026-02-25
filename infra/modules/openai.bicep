// ─────────────────────────────────────────────────────────────────────
// infra/modules/openai.bicep
// Deploys Azure AI Services (OpenAI) account with gpt-4o and
// text-embedding-3-large deployments, a Foundry-native project,
// and an AI Search connection on the project.
//
// Uses API version 2025-06-01 which supports:
//   - allowProjectManagement: true
//   - Microsoft.CognitiveServices/accounts/projects
//   - Microsoft.CognitiveServices/accounts/projects/connections
// ─────────────────────────────────────────────────────────────────────

@description('Name of the AI Services account')
param name string

@description('Location for the AI Services account')
param location string

@description('Model deployment name')
param modelDeploymentName string = 'gpt-4o'

@description('Model name')
param modelName string = 'gpt-4o'

@description('Model version')
param modelVersion string = '2024-11-20'

@description('TPM capacity for the model deployment')
param modelCapacity int = 10

@description('Embedding model deployment name')
param embeddingDeploymentName string = 'text-embedding-3-large'

@description('Embedding model name')
param embeddingModelName string = 'text-embedding-3-large'

@description('Embedding model version')
param embeddingModelVersion string = '1'

@description('TPM capacity for the embedding model deployment')
param embeddingModelCapacity int = 10

@description('Name of the Foundry project')
param projectName string

@description('Name of the AI Search connection on the project')
param searchConnectionName string = 'confluence-search'

@description('AI Search service endpoint URL (e.g. https://mysearch.search.windows.net/)')
param searchServiceEndpoint string

@description('Resource ID of the AI Search service')
param searchServiceId string

@secure()
@description('AI Search admin API key for connection authentication')
param searchApiKey string

param tags object = {}

// ── AI Services account ───────────────────────────────────────────
resource aiServices 'Microsoft.CognitiveServices/accounts@2025-06-01' = {
  name: name
  location: location
  tags: tags
  kind: 'AIServices'
  identity: {
    type: 'SystemAssigned'
  }
  sku: {
    name: 'S0'
  }
  properties: {
    customSubDomainName: name
    publicNetworkAccess: 'Enabled'
    disableLocalAuth: false
    allowProjectManagement: true
  }
}

// ── Chat model deployment ─────────────────────────────────────────
resource modelDeployment 'Microsoft.CognitiveServices/accounts/deployments@2025-06-01' = {
  parent: aiServices
  name: modelDeploymentName
  sku: {
    name: 'Standard'
    capacity: modelCapacity
  }
  properties: {
    model: {
      format: 'OpenAI'
      name: modelName
      version: modelVersion
    }
  }
}

// ── Embedding model deployment ───────────────────────────────────
// Required by the Foundry IQ knowledge source for vectorization
// during OneLake data ingestion.
resource embeddingDeployment 'Microsoft.CognitiveServices/accounts/deployments@2025-06-01' = {
  parent: aiServices
  name: embeddingDeploymentName
  sku: {
    name: 'Standard'
    capacity: embeddingModelCapacity
  }
  properties: {
    model: {
      format: 'OpenAI'
      name: embeddingModelName
      version: embeddingModelVersion
    }
  }
  dependsOn: [
    modelDeployment
  ]
}

// ── Foundry-native project ────────────────────────────────────────
// Creates a project under the AI Services account (replaces the
// legacy Hub/Project paradigm from MachineLearningServices).
resource project 'Microsoft.CognitiveServices/accounts/projects@2025-06-01' = {
  parent: aiServices
  name: projectName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {}
  dependsOn: [
    embeddingDeployment
  ]
}

// ── AI Search connection on the project ───────────────────────────
// Enables the agent to discover and query the AI Search index via
// the project's connections API. Uses ApiKey auth because the
// Foundry-native project data-plane does not resolve AAD-only
// connections for the agent runtime.
resource searchConnection 'Microsoft.CognitiveServices/accounts/projects/connections@2025-06-01' = {
  parent: project
  name: searchConnectionName
  properties: {
    category: 'CognitiveSearch'
    target: searchServiceEndpoint
    isSharedToAll: true
    authType: 'ApiKey'
    credentials: {
      key: searchApiKey
    }
    metadata: {
      ApiType: 'Azure'
      ResourceId: searchServiceId
    }
  }
}

// ── Outputs ───────────────────────────────────────────────────────
output id string = aiServices.id
output name string = aiServices.name
output endpoint string = aiServices.properties.endpoint
output principalId string = aiServices.identity.principalId

output projectName string = project.name
output projectPrincipalId string = project.identity.principalId
output projectEndpoint string = 'https://${aiServices.name}.openai.azure.com/api/projects/${project.name}'

output searchConnectionName string = searchConnection.name
output embeddingDeploymentName string = embeddingDeployment.name
