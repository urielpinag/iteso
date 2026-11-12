output "worker_url" {
  description = "URL pública del Worker en workers.dev"
  value       = "https://${var.worker_name}.${var.workers_dev_subdomain}.workers.dev"
}

output "worker_name" {
  description = "Nombre del Worker desplegado"
  value       = var.worker_name
}

output "d1_database_name" {
  description = "Nombre de la base de datos D1"
  value       = module.d1_database.database_name
}

output "d1_database_id" {
  description = "ID (UUID) de la base de datos D1"
  value       = module.d1_database.database_id
}
