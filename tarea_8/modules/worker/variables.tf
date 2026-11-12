variable "account_id" {
  description = "ID de la cuenta de Cloudflare"
  type        = string
}

variable "worker_name" {
  description = "Nombre del Worker"
  type        = string
}

variable "db_id" {
  description = "ID de la base de datos D1 a la que se enlaza el Worker"
  type        = string
}
