data "archive_file" "source_gold" {
  type        = "zip"
  source_dir  = "../cloud_gold"
  output_path = "${path.module}/function-gold.zip"
}

resource "google_storage_bucket_object" "zip_gold" {
  source       = data.archive_file.source_gold.output_path
  content_type = "application/zip"
  name         = "src-${data.archive_file.source_gold.output_md5}.zip"
  bucket       = google_storage_bucket.function_api_gold.name
  depends_on = [
    google_storage_bucket.function_api_gold,
    data.archive_file.source_gold
  ]
}

resource "google_cloudfunctions_function" "Cloud_function_gold" {
  name                  = "${var.bucket_name}-cloud-function-gold"
  description           = "exportar delta pro athena"
  runtime               = "python311"
  project               = var.project_id
  region                = var.region
  source_archive_bucket = google_storage_bucket.function_api_gold.name
  source_archive_object = google_storage_bucket_object.zip_gold.name
  entry_point           = "finalized_gold"
  available_memory_mb   = 8192
  timeout               = 540
  service_account_email = google_service_account.bucket_account.email
  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = var.bucket_name
  }

  environment_variables = {
    project_id            = var.project_id
    bucket_name           = var.bucket_name
    aws_access_key_id     = var.aws_access_key_id
    aws_secret_access_key = var.aws_secret_access_key
    region_aws            = var.region_aws
    s3_location           = var.s3_location
    s3_location_table     = var.s3_location_table
  }

  depends_on = [
    google_storage_bucket.function_api_gold,
    google_storage_bucket_object.zip_gold,
  ]
}

resource "google_cloudfunctions_function_iam_member" "invoker" {
  project        = google_cloudfunctions_function.Cloud_function_gold.project
  region         = google_cloudfunctions_function.Cloud_function_gold.region
  cloud_function = google_cloudfunctions_function.Cloud_function_gold.name

  role   = "roles/cloudfunctions.invoker"
  member = "serviceAccount:${google_service_account.bucket_account.email}"

  depends_on = [
    google_cloudfunctions_function.Cloud_function_gold,
    google_service_account.bucket_account,
  ]
}