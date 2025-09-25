variable "BKK_API_KEY" { type = string }

resource "azurerm_virtual_network" "vm_network" {
  name                = "${local.project_name}-vm-network"
  address_space       = ["10.0.0.0/16"]
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
}

resource "azurerm_subnet" "vm_subnet" {
  name                 = "${local.project_name}-vm-subnet"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.vm_network.name
  address_prefixes     = ["10.0.2.0/24"]
}

resource "azurerm_network_interface" "vm_nic" {
  name                = "${local.project_name}-vm-nic"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.vm_subnet.id
    private_ip_address_allocation = "Dynamic"
  }
}

resource "azurerm_linux_virtual_machine" "vm" {
  depends_on = [
    azurerm_container_registry.main,
    null_resource.docker_build_and_push,
  ]

  name                = "${local.project_name}-vm"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  size                = "Standard_B1s"
  admin_username      = "azureuser"
  admin_password      = "azureuser@1234"
  network_interface_ids = [
    azurerm_network_interface.vm_nic.id,
  ]

  disable_password_authentication = false

  source_image_reference {
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-focal"
    sku       = "20_04-lts"
    version   = "latest"
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
  }

  # Cloud-init to install Docker & run container
  custom_data = base64encode(<<EOF
#cloud-config
package_update: true
packages:
  - docker.io
runcmd:
  - sudo su
  - "docker login ${azurerm_container_registry.main.login_server}
      -u '${azurerm_container_registry.main.admin_username}'
      -p '${azurerm_container_registry.main.admin_password}'"
  - docker pull ${azurerm_container_registry.main.login_server}/${local.scraper_image}:latest
  - "docker run -d --name scraper 
    -e BKK_API_KEY='${var.BKK_API_KEY}' 
    -e AZURE_STORAGE_CONNECTION_STRING='${azurerm_storage_account.main.primary_connection_string}' 
    -e ALERTS_CONTAINER='${azurerm_storage_container.alerts.name}'
    -e POSITIONS_CONTAINER='${azurerm_storage_container.positions.name}'
    ${azurerm_container_registry.main.login_server}/${local.scraper_image}:latest"
  - mkdir -p /var/log/scraper
  - docker logs -f scraper &> /var/log/scraper/output.log &
EOF
  )
}
