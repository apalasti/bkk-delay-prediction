variable "BKK_API_KEY" { type = string }

# User-assigned managed identity
resource "azurerm_user_assigned_identity" "aci_identity" {
  name                = "aci-identity-${local.project_name}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
}

# Give identity AcrPull on ACR
resource "azurerm_role_assignment" "aci_acr_pull" {
  scope                = azurerm_container_registry.main.id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_user_assigned_identity.aci_identity.principal_id
}

resource "azurerm_container_group" "aci" {
  name                = "aci-${local.project_name}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  os_type             = "Linux"
  restart_policy      = "Always"
  ip_address_type     = "None"

  depends_on = [
    null_resource.docker_build_and_push,
    azurerm_role_assignment.aci_acr_pull,
  ]

  container {
    name   = "scraper"
    image  = "${azurerm_container_registry.main.login_server}/${local.scraper_image}:latest"
    cpu    = "1"
    memory = "1"

    environment_variables = {
      "BKK_API_KEY"                     = var.BKK_API_KEY
      "AZURE_STORAGE_CONNECTION_STRING" = azurerm_storage_account.main.primary_connection_string
    }
  }

  diagnostics {
    log_analytics {
      workspace_id  = azurerm_log_analytics_workspace.main.workspace_id
      workspace_key = azurerm_log_analytics_workspace.main.primary_shared_key
    }
  }

  image_registry_credential {
    server                    = azurerm_container_registry.main.login_server
    user_assigned_identity_id = azurerm_user_assigned_identity.aci_identity.id
  }

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.aci_identity.id]
  }
}
