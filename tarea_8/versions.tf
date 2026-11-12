terraform {
  required_version = ">= 1.5.0"

  required_providers {
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "~> 5.28"
    }
  }
}

provider "cloudflare" {
  # Si var.api_token es null, el provider toma automáticamente
  # la variable de entorno CLOUDFLARE_API_TOKEN (definida en HCP Terraform).
  api_token = var.api_token
}
