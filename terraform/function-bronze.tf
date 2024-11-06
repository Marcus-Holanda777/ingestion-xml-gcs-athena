data "archive_file" "source_bronze" {
  type        = "zip"
  source_dir  = "../cloud_bronze"
  output_path = "${path.module}/function-bronze.zip"
}

resource "google_storage_bucket_object" "zip_bronze" {
  source       = data.archive_file.source_bronze.output_path
  content_type = "application/zip"
  name         = "src-${data.archive_file.source_bronze.output_md5}.zip"
  bucket       = google_storage_bucket.function_api_bronze.name
  depends_on = [
    google_storage_bucket.function_api_bronze,
    data.archive_file.source_bronze
  ]
}

resource "google_cloudfunctions_function" "Cloud_function_bronze" {
  name                  = "${var.bucket_name}-cloud-function-bronze"
  description           = "inserir dados na camada bronze"
  runtime               = "python311"
  project               = var.project_id
  region                = var.region
  source_archive_bucket = google_storage_bucket.function_api_bronze.name
  source_archive_object = google_storage_bucket_object.zip_bronze.name
  entry_point           = "finalized_bronze"
  available_memory_mb   = 1024
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
    google_storage_bucket.function_api_bronze,
    google_storage_bucket_object.zip_bronze,
  ]
}

resource "google_cloudfunctions_function_iam_member" "invoker_bronze" {
  project        = google_cloudfunctions_function.Cloud_function_bronze.project
  region         = google_cloudfunctions_function.Cloud_function_bronze.region
  cloud_function = google_cloudfunctions_function.Cloud_function_bronze.name

  role   = "roles/cloudfunctions.invoker"
  member = "serviceAccount:${google_service_account.bucket_account.email}"

  depends_on = [
    google_cloudfunctions_function.Cloud_function_bronze,
    google_service_account.bucket_account,
  ]
}