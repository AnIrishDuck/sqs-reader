resource "aws_sqs_queue" "fmurphy_queue" {
  name                      = "fmurphy-test-queue"
  max_message_size          = 2048
  message_retention_seconds = 86400
  receive_wait_time_seconds = 10
}

resource "aws_sqs_queue" "fmurphy_out_queue" {
  name                      = "fmurphy-test-queue-transfer"
  max_message_size          = 2048
  message_retention_seconds = 86400
  receive_wait_time_seconds = 10
}
