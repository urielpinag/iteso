# Módulo: Worker con script y subdominio workers.dev

resource "cloudflare_workers_script" "this" {
  account_id         = var.account_id
  script_name        = var.worker_name
  content            = file("${path.module}/src/index.js")
  main_module        = "index.js"
  compatibility_date = "2025-01-01"

  # Binding a la base D1 para que el Worker pueda consultarla
  bindings = [{
    name = "DB"
    type = "d1"
    id   = var.db_id
  }]
}

resource "cloudflare_workers_script_subdomain" "this" {
  account_id       = var.account_id
  script_name      = cloudflare_workers_script.this.script_name
  enabled          = true
  previews_enabled = true

  depends_on = [cloudflare_workers_script.this]
}
