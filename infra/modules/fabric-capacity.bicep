// ─────────────────────────────────────────────────────────────────────
// infra/modules/fabric-capacity.bicep
// Deploys a Microsoft Fabric capacity (F2 by default).
// ─────────────────────────────────────────────────────────────────────

@description('Name of the Fabric capacity')
param name string

@description('Location for the Fabric capacity')
param location string

@description('SKU name (F2, F4, F8, F16, F32, F64, etc.)')
@allowed([
  'F2'
  'F4'
  'F8'
  'F16'
  'F32'
  'F64'
  'F128'
  'F256'
  'F512'
  'F1024'
  'F2048'
])
param skuName string = 'F2'

@description('Admin members (email addresses)')
param adminMembers array

param tags object = {}

resource fabricCapacity 'Microsoft.Fabric/capacities@2023-11-01' = {
  name: name
  location: location
  tags: tags
  sku: {
    name: skuName
    tier: 'Fabric'
  }
  properties: {
    administration: {
      members: adminMembers
    }
  }
}

output id string = fabricCapacity.id
output name string = fabricCapacity.name
