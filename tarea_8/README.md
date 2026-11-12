# Tarea 8 — Terraform Modules y HCP Terraform

Práctica 11 (Cloudflare D1 + Worker) refactorizada en módulos de Terraform y
desplegada desde HCP Terraform mediante un workspace conectado a GitHub (VCS).

## Estructura

```
tarea_8/
├── main.tf            # Raíz: orquesta los módulos
├── variables.tf
├── outputs.tf
├── versions.tf
├── terraform.tfvars
└── modules/
    ├── d1_database/   # cloudflare_d1_database
    │   ├── main.tf
    │   ├── variables.tf
    │   ├── outputs.tf
    │   └── versions.tf
    └── worker/        # cloudflare_workers_script + subdominio workers.dev
        ├── main.tf
        ├── variables.tf
        ├── outputs.tf
        ├── versions.tf
        └── src/index.js
```

## Módulos

### `modules/d1_database`

Crea una base de datos Cloudflare D1 y exporta su `database_id`.

| Variable | Tipo | Descripción |
|---|---|---|
| `account_id` | string | ID de la cuenta de Cloudflare |
| `database_name` | string | Nombre de la base de datos D1 |

| Output | Descripción |
|---|---|
| `database_id` | UUID de la base D1 |
| `database_name` | Nombre de la base D1 |

### `modules/worker`

Crea el Worker, su binding a la base D1 y el subdominio `workers.dev`.

| Variable | Tipo | Descripción |
|---|---|---|
| `account_id` | string | ID de la cuenta de Cloudflare |
| `worker_name` | string | Nombre del Worker |
| `db_id` | string | ID de la base D1 para el binding `DB` |

| Output | Descripción |
|---|---|
| `worker_name` | Nombre del Worker desplegado |

## Variables del workspace (HCP Terraform)

- `account_id` — Terraform variable
- `CLOUDFLARE_API_TOKEN` — Environment variable sensible

Los valores de `worker_name`, `d1_database_name` y `workers_dev_subdomain`
provienen de `terraform.tfvars`.

## Despliegue

El workspace `tarea8-terraform` (organización `piniaops`) apunta a este
directorio (`tarea_8`) y ejecuta plan/apply en HCP Terraform con cada push a
`main`.

