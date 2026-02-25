// ─────────────────────────────────────────────────────────────────────
// infra/modules/openai.bicep
// Deploys Azure AI Services (OpenAI) account with a gpt-4o deployment,
// a Foundry-native project, and an AI Search connection on the project.
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

@description('Name of the Foundry project')
param projectName string

@description('Name of the AI Search connection on the project')
param searchConnectionName string = 'confluence-search'

@description('AI Search service endpoint URL (e.g. https://mysearch.search.windows.net/)')
param searchServiceEndpoint string

@description('Resource ID of the AI Search service')
param searchServiceId string

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
    allowProjectManagement: true
  }
}

// ── Model deployment ──────────────────────────────────────────────
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
    modelDeployment
  ]
}

// ── AI Search connection on the project ───────────────────────────
// Enables the agent to discover and query the AI Search index via
// the project's connections API.
resource searchConnection 'Microsoft.CognitiveServices/accounts/projects/connections@2025-06-01' = {
  parent: project
  name: searchConnectionName
  properties: {
    category: 'CognitiveSearch'
    target: searchServiceEndpoint
    isSharedToAll: true
    authType: 'AAD'
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
output projectEndpoint string = 'https://${aiServices.name}.services.ai.azure.com/api/projects/${project.name}'

output searchConnectionName string = searchConnection.name
