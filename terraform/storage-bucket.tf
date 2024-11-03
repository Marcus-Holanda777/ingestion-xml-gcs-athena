resource "google_storage_bucket" "storage_xml" {
  name          = var.bucket_name
  location      = var.region
  project       = var.project_id
  force_destroy = true

  storage_class            = "STANDARD"
  public_access_prevention = "enforced"
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