resource "google_service_account" "bucket_account" {
  account_id   = var.bucket_name
  display_name = "trabalho xml gera nota"
}

resource "google_service_account_key" "bucket_account_key" {
  service_account_id = google_service_account.bucket_account.name
  public_key_type    = "TYPE_X509_PEM_FILE"
  private_key_type   = "TYPE_GOOGLE_CREDENTIALS_FILE"
}

resource "local_file" "bucket_account_key_file" {
  content  = base64decode(google_service_account_key.bucket_account_key.private_key)
  filename = "${path.module}/${var.project_id}_${var.bucket_name}.json"
}

resource "google_storage_hmac_key" "account_hmac" {
  service_account_email = google_service_account.bucket_account.email
}

resource "local_file" "bucket_account_hmac_file" {
  content  = <<-EOT
     ${google_storage_hmac_key.account_hmac.secret}
     ${google_storage_hmac_key.account_hmac.access_id}
  EOT
  filename = "${path.module}/${var.project_id}_${var.bucket_name}.txt"
}