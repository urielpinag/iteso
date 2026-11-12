# Tarea 8 - Terraform Modules y HCP
# Raíz: solo orquesta los módulos locales.

module "d1_database" {
  source = "./modules/d1_database"

  account_id    = var.account_id
  database_name = var.d1_database_name
}

module "worker" {
  source = "./modules/worker"

  account_id  = var.account_id
  worker_name = var.worker_name
  db_id       = module.d1_database.database_id
}
