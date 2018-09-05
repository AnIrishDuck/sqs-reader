extern crate docopt;
extern crate rusoto_core;
extern crate rusoto_sqs;
#[macro_use]
extern crate serde_json;

use docopt::Docopt;
use rusoto_core::Region;
use rusoto_sqs::{
    DeleteMessageRequest,
    GetQueueUrlRequest,
    Message,
    ReceiveMessageRequest,
    SqsClient,
    Sqs
};
use std::collections::HashMap;

const USAGE: &'static str = "
Simple SQS queue reader. Automatically retries and deduplicates until the
desired number of messages have been read.

Usage:
    sqs-reader <queue-name> <count> [--full] [--drain]
    sqs-reader -h | --help

Options:
  -h, --help        Show this screen.
  --full            Print full response with message attributes instead of
                    message body.
  --drain           Remove messages from queue after all have been read.
";


fn main () {
    let args = Docopt::new(USAGE)
                      .and_then(|dopt| dopt.parse())
                      .unwrap_or_else(|e| e.exit());
    let region = Region::default();

    let sqs = SqsClient::new(region);
    let url = sqs.get_queue_url(GetQueueUrlRequest {
        queue_name: args.get_str("<queue-name>").to_string(),
        queue_owner_aws_account_id: None
    }).sync()
        .map(|m| m.queue_url.expect("extracting url from response"))
        .expect("fetching queue url");

    let mut all_messages = HashMap::new();
    let count: i32 = args.get_str("<count>").parse().unwrap();
    let mut attribute_names = vec!("All".to_owned());
    attribute_names.resize(1, "All".to_owned());
    while all_messages.len() < count as usize {
        let response = sqs.receive_message(ReceiveMessageRequest {
            attribute_names: Some(attribute_names.clone()),
            max_number_of_messages: Some(1),
            message_attribute_names: None,
            queue_url: url.to_string(),
            receive_request_attempt_id: None,
            visibility_timeout: Some(0),
            wait_time_seconds: None
        }).sync().expect("reading from queue");

        match response.messages {
            Some(messages) => {
                for message in messages {
                    let id = message.message_id.to_owned().expect("getting id");
                    all_messages.insert(id, message);
                }
            },
            None => (),
        }
    }

    // Wait until all messages have been received to purge them. This reduces,
    // but does not eliminate, the chance of message "loss". One of the API
    // calls below can still theoretically panic.
    if args.get_bool("--drain") {
        for (_id, message) in &all_messages {
            let handle = message.receipt_handle.to_owned();
            sqs.delete_message(DeleteMessageRequest {
                queue_url: url.to_string(),
                receipt_handle: handle.expect("getting receipt handle")
            }).sync().unwrap();
        }
    }

    for (_id, message) in all_messages {
        if args.get_bool("--full") {
            print_full_message(message);
        } else {
            println!("{}", message.body.expect("getting body"));
        }
    }
}

fn print_full_message (message: Message) {
    let attributes = message.attributes.unwrap_or(HashMap::new());
    let value = json!({
        "Body": message.body.expect("getting body"),
        "ReceiptHandle": message.receipt_handle.expect("getting receipt handle"),
        "MD5OfBody": message.md5_of_body.expect("getting md5 of body"),
        "MessageId": message.message_id.expect("getting message id"),
        "Attributes": attributes,
    });

    println!("{}", value.to_string());
}
