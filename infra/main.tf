data "azurerm_client_config" "current" {}

locals {
  project_name = "delayprediction"
}

resource "azurerm_resource_group" "main" {
  name     = "rg-${local.project_name}"
  location = var.location
}

resource "random_string" "acr_suffix" {
  length  = 6
  upper   = false
  lower   = true
  numeric = true
  special = false
}

# Storage Account for persistent data
resource "azurerm_storage_account" "main" {
  name                     = "strg${local.project_name}"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
}

resource "azurerm_container_registry" "main" {
  name                = "acr${local.project_name}${random_string.acr_suffix.result}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "Basic"
  admin_enabled       = true
}

# Log Analytics workspace for ACI diagnostics
resource "azurerm_log_analytics_workspace" "main" {
  name                = "law-${local.project_name}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  retention_in_days   = 30
}

locals {
  scraper_image = "${local.project_name}-scraper"
}

resource "null_resource" "docker_build_and_push" {
  depends_on = [azurerm_container_registry.main]

  triggers = {
    dockerfile_sha = filesha256("${abspath(path.module)}/../Dockerfile")
    pyproject      = filesha256("${abspath(path.module)}/../pyproject.toml")
    uv_lock        = filesha256("${abspath(path.module)}/../uv.lock")
    src_sha        = sha256(join("", [for f in fileset("${abspath(path.module)}/../src", "**") : filesha256("${abspath(path.module)}/../src/${f}")]))
  }

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-lc"]
    command     = <<-EOT
      set -euo pipefail
      ROOT_DIR="${abspath(path.module)}/.."
      IMAGE_REPO="${azurerm_container_registry.main.login_server}/${local.scraper_image}"
      GIT_SHA=$(git -C "$ROOT_DIR" rev-parse --short HEAD 2>/dev/null || echo "dev")
      docker build --platform linux/amd64 -t "$IMAGE_REPO:$GIT_SHA" -t "$IMAGE_REPO:latest" "$ROOT_DIR"
      az acr login --name "${azurerm_container_registry.main.name}"
      docker push "$IMAGE_REPO:$GIT_SHA"
      docker push "$IMAGE_REPO:latest"
    EOT
  }
}
