resource "azurerm_storage_container" "positions" {
  name                  = "positions"
  storage_account_name    = azurerm_storage_account.main.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "alerts" {
  name                  = "alerts"
  storage_account_name    = azurerm_storage_account.main.name
  container_access_type = "private"
}
