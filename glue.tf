terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
}

provider "aws" {
  region = "ap-south-1"
}

resource "aws_s3_bucket" "glue_script_bucket" {
  bucket = "my-glue-job-scripts-bucket-ap-south-1" # Change to a unique bucket name
}

resource "aws_s3_bucket_object" "glue_script" {
  bucket = aws_s3_bucket.glue_script_bucket.id
  key    = "glue-scripts/glue_script.py"
  source = "glue.py" # Path to your local script
  etag   = filemd5("glue.py")
}

resource "aws_iam_role" "glue_role" {
  name = "glue_admin_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "admin_policy" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}

resource "aws_glue_job" "my_glue_job" {
  name     = "my-glue-job"
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_script_bucket.id}/glue-scripts/glue_script.py"
  }

  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2

  execution_property {
    max_concurrent_runs = 1
  }
}

resource "aws_glue_catalog_database" "manju_Database" {
  name = "manju_database"
}

resource "aws_glue_catalog_table" "manju_gluetable" {
  name          = "manju_gluetable"
  database_name = aws_glue_catalog_database.manju_Database.name
  table_type    = "EXTERNAL_TABLE"

  storage_descriptor {
    columns {
      name = "fn"
      type = "string"
    }
    columns {
      name = "ln"
      type = "string"
    }
    columns {
      name = "mn"
      type = "string"
    }
    columns {
      name = "id"
      type = "string"
    }
    columns {
      name = "gndr"
      type = "string"
    }
    columns {
      name = "salary"
      type = "int"
    }

    location      = "s3://my-glue-job-scripts-bucket-ap-south-1/athena-results/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      name            = "parquet-serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
  }
}

resource "aws_athena_workgroup" "Manju_workgroup" {
  name = "Manju_workgroup"

  configuration {
    result_configuration {
      output_location = "s3://my-glue-job-scripts-bucket-ap-south-1/athena-results/"
    }
  }
}
