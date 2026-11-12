variable "account_id" {
  description = "ID de la cuenta de Cloudflare"
  type        = string
}

variable "api_token" {
  description = "API Token de Cloudflare. Si se omite se usa la variable de entorno CLOUDFLARE_API_TOKEN."
  type        = string
  sensitive   = true
  default     = null
}

variable "worker_name" {
  description = "Nombre del Worker"
  type        = string
  default     = "tarea8-worker"
}

variable "d1_database_name" {
  description = "Nombre de la base de datos D1"
  type        = string
  default     = "tarea8-db"
}

variable "workers_dev_subdomain" {
  description = "Subdominio workers.dev de la cuenta (solo se usa para construir la URL de salida)"
  type        = string
  default     = "piniaops"
}
