resource "google_storage_bucket" "storage_xml" {
  name          = var.bucket_name
  location      = var.region
  project       = var.project_id
  force_destroy = true

  storage_class            = "STANDARD"
  public_access_prevention = "enforced"

  soft_delete_policy {
    retention_duration_seconds = 0
  }
}

resource "google_storage_bucket_object" "md_folder" {
  for_each = toset(var.medalions)
  name     = "${each.value}/"
  content  = " "
  bucket   = google_storage_bucket.storage_xml.name
}

resource "google_storage_bucket_iam_member" "bucket_access" {
  bucket = google_storage_bucket.storage_xml.name
  role   = "roles/storage.admin"
  member = "serviceAccount:${google_service_account.bucket_account.email}"
}

resource "google_storage_bucket" "function_api_bronze" {
  name          = "${var.bucket_name}-function-bronze"
  location      = var.region
  project       = var.project_id
  force_destroy = true

  storage_class            = "STANDARD"
  public_access_prevention = "enforced"

  soft_delete_policy {
    retention_duration_seconds = 0
  }
}

resource "google_storage_bucket" "function_api_silver" {
  name          = "${var.bucket_name}-function-silver"
  location      = var.region
  project       = var.project_id
  force_destroy = true

  storage_class            = "STANDARD"
  public_access_prevention = "enforced"

  soft_delete_policy {
    retention_duration_seconds = 0
  }
}

resource "google_storage_bucket" "function_api_gold" {
  name          = "${var.bucket_name}-function-gold"
  location      = var.region
  project       = var.project_id
  force_destroy = true

  storage_class            = "STANDARD"
  public_access_prevention = "enforced"

  soft_delete_policy {
    retention_duration_seconds = 0
  }
}