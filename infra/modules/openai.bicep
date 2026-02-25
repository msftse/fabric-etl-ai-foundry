// ─────────────────────────────────────────────────────────────────────
// infra/modules/openai.bicep
// Deploys Azure AI Services (OpenAI) account with a gpt-4o deployment.
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

param tags object = {}

resource aiServices 'Microsoft.CognitiveServices/accounts@2024-10-01' = {
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

resource modelDeployment 'Microsoft.CognitiveServices/accounts/deployments@2024-10-01' = {
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

output id string = aiServices.id
output name string = aiServices.name
output endpoint string = aiServices.properties.endpoint
output principalId string = aiServices.identity.principalId
