data "archive_file" "source_silver" {
  type        = "zip"
  source_dir  = "../cloud_silver"
  output_path = "${path.module}/function-silver.zip"
}

resource "google_storage_bucket_object" "zip_silver" {
  source       = data.archive_file.source_silver.output_path
  content_type = "application/zip"
  name         = "src-${data.archive_file.source_silver.output_md5}.zip"
  bucket       = google_storage_bucket.function_api_silver.name
  depends_on = [
    google_storage_bucket.function_api_silver,
    data.archive_file.source_silver
  ]
}

resource "google_cloudfunctions_function" "Cloud_function_silver" {
  name                  = "${var.bucket_name}-cloud-function-silver"
  description           = "inserir dados na camada silver"
  runtime               = "python311"
  project               = var.project_id
  region                = var.region
  source_archive_bucket = google_storage_bucket.function_api_silver.name
  source_archive_object = google_storage_bucket_object.zip_silver.name
  entry_point           = "finalized_silver"
  available_memory_mb   = 8192
  timeout               = 540
  service_account_email = google_service_account.bucket_account.email
  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = var.bucket_name
  }

  environment_variables = {
    project_id  = "${var.project_id}"
    bucket_name = "${var.bucket_name}"
  }

  depends_on = [
    google_storage_bucket.function_api_silver,
    google_storage_bucket_object.zip_silver,
  ]
}

resource "google_cloudfunctions_function_iam_member" "invoker_silver" {
  project        = google_cloudfunctions_function.Cloud_function_silver.project
  region         = google_cloudfunctions_function.Cloud_function_silver.region
  cloud_function = google_cloudfunctions_function.Cloud_function_silver.name

  role   = "roles/cloudfunctions.invoker"
  member = "serviceAccount:${google_service_account.bucket_account.email}"

  depends_on = [
    google_cloudfunctions_function.Cloud_function_silver,
    google_service_account.bucket_account,
  ]
}