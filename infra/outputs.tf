output "resource_group_name" {
  description = "The name of the Azure Resource Group."
  value       = azurerm_resource_group.main.name
}

output "storage_account_name" {
  description = "The name of the Azure Storage Account."
  value       = azurerm_storage_account.main.name
}

output "acr_name" {
  description = "The name of the Azure Container Registry."
  value       = azurerm_container_registry.main.name
}

output "acr_login_server" {
  description = "The login server URL for the Azure Container Registry."
  value       = azurerm_container_registry.main.login_server
}

output "docker_image_latest" {
  description = "The full URL for the latest Docker image in the Azure Container Registry, including the repository name."
  value       = "${azurerm_container_registry.main.login_server}/${local.project_name}-scraper:latest"
}

output "vm_name" {
  value = azurerm_linux_virtual_machine.vm.name
}
