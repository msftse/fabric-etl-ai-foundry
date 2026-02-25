// ─────────────────────────────────────────────────────────────────────
// infra/modules/ai-search.bicep
// Deploys Azure AI Search with SystemAssigned managed identity.
// ─────────────────────────────────────────────────────────────────────

@description('Name of the AI Search service')
param name string

@description('Location for the AI Search service')
param location string

@description('SKU for AI Search (free, basic, standard, standard2, standard3)')
@allowed([
  'free'
  'basic'
  'standard'
  'standard2'
  'standard3'
])
param skuName string = 'standard'

param tags object = {}

resource searchService 'Microsoft.Search/searchServices@2024-06-01-preview' = {
  name: name
  location: location
  tags: tags
  sku: {
    name: skuName
  }
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    replicaCount: 1
    partitionCount: 1
    hostingMode: 'default'
    publicNetworkAccess: 'enabled'
  }
}

output id string = searchService.id
output name string = searchService.name
output principalId string = searchService.identity.principalId

#disable-next-line outputs-should-not-contain-secrets
output adminKey string = searchService.listAdminKeys().primaryKey
