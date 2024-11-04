data "archive_file" "source" {
  type        = "zip"
  source_dir  = "../cloud_bronze"
  output_path = "${path.module}/function.zip"
}

resource "google_storage_bucket_object" "zip" {
  source       = data.archive_file.source.output_path
  content_type = "application/zip"
  name         = "src-${data.archive_file.source.output_md5}.zip"
  bucket       = google_storage_bucket.function_api.name
  depends_on = [
    google_storage_bucket.function_api,
    data.archive_file.source
  ]
}

resource "google_cloudfunctions_function" "Cloud_function" {
  name                  = "${var.bucket_name}-cloud-function"
  description           = "trabalho api currency"
  runtime               = "python311"
  project               = var.project_id
  region                = var.region
  source_archive_bucket = google_storage_bucket.function_api.name
  source_archive_object = google_storage_bucket_object.zip.name
  entry_point           = "finalized_bronze"
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
    google_storage_bucket.function_api,
    google_storage_bucket_object.zip,
  ]
}

resource "google_cloudfunctions_function_iam_member" "invoker" {
  project        = google_cloudfunctions_function.Cloud_function.project
  region         = google_cloudfunctions_function.Cloud_function.region
  cloud_function = google_cloudfunctions_function.Cloud_function.name

  role   = "roles/cloudfunctions.invoker"
  member = "serviceAccount:${google_service_account.bucket_account.email}"

  depends_on = [
    google_cloudfunctions_function.Cloud_function,
    google_service_account.bucket_account,
  ]
}