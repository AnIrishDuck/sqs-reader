extern crate docopt;
extern crate rusoto_core;
extern crate rusoto_sqs;
#[macro_use]
extern crate serde_json;

use docopt::Docopt;
use docopt::Value::Plain;
use rusoto_core::Region;
use rusoto_sqs::{
    DeleteMessageRequest,
    GetQueueAttributesRequest,
    GetQueueAttributesResult,
    GetQueueUrlError,
    GetQueueUrlRequest,
    Message,
    ReceiveMessageRequest,
    SendMessageRequest,
    SqsClient,
    Sqs
};
use std::collections::HashMap;

const USAGE: &'static str = "
Simple SQS queue reader. Automatically retries and deduplicates until the
desired number of messages have been read.

Can either output messages to stdout or transfer them to another queue.

NOTE: transferring message attributes is currently not supported, and thus
custom attributes will not be preserved when moving messages.

Usage:
    sqs-reader <in-queue> [--stdout] [--count=<n>] [--drain] [--full]
    sqs-reader <in-queue> <out-queue> [--stdout] [--count=<n>] [--drain] [--full]
    sqs-reader -h | --help

Options:
  -h, --help    Show this screen.
  --stdout      Dump messages to stdout.
  --count=<n>   Number of messages to read.
  --drain       Remove messages from queue after all have been read.
  --full        Print full response with message attributes instead of just
                printing the message body.
";


fn main () {
    let args = Docopt::new(USAGE)
                      .and_then(|dopt| dopt.parse())
                      .unwrap_or_else(|e| e.exit());
    let region = Region::default();
    let sqs = SqsClient::new(region);

    let in_queue = args.get_str("<in-queue>").to_string();
    let in_url = get_queue_url(&sqs, &in_queue)
        .expect(&format!("fetching input queue url for {}", &in_queue));

    let out_queue = args.find("<out-queue>").and_then(|value|
        if let Plain(Some(name)) = value { Some(name) } else { None }
    );
    let out_url : Option<String> = out_queue.map(|name|
        get_queue_url(&sqs, &name.as_str().to_string())
            .expect(&format!("fetching output queue url for {}", &name.as_str()))
    );

    let drain = args.get_bool("--drain");


    let mut all_messages = HashMap::new();
    let count: u32 = args
        .get_str("--count")
        .parse().ok()
        .unwrap_or_else(|| get_approximate_queue_size(&sqs, &in_url).expect("getting queue size"));
    let mut attribute_names = vec!("All".to_owned());
    attribute_names.resize(1, "All".to_owned());
    while all_messages.len() < count as usize {
        let response = sqs.receive_message(ReceiveMessageRequest {
            attribute_names: Some(attribute_names.clone()),
            max_number_of_messages: Some(1),
            message_attribute_names: None,
            queue_url: in_url.to_string(),
            receive_request_attempt_id: None,
            visibility_timeout: Some(if drain { 60 } else { 0 }),
            wait_time_seconds: None
        }).sync().expect("reading from queue");

        if let Some(messages) = response.messages {
            for message in messages {
                let id = message.message_id.to_owned().expect("getting id");
                all_messages.insert(id, message);
            }
        }
    }

    // Wait until all messages have been received to purge them. This reduces,
    // but does not eliminate, the chance of message "loss". One of the API
    // calls below can still theoretically panic.
    if drain {
        for (_id, message) in &all_messages {
            let handle = message.receipt_handle.to_owned();
            sqs.delete_message(DeleteMessageRequest {
                queue_url: in_url.to_string(),
                receipt_handle: handle.expect("getting receipt handle")
            }).sync().unwrap();
        }
    }

    for (_id, message) in all_messages {
        let body = message.body.to_owned().expect("getting body");

        if args.get_bool("--stdout") {
            if args.get_bool("--full") {
                print_full_message(message);
            } else {
                println!("{}", body);
            }
        }

        if let Some(url) = &out_url {
            let response = send_message(&sqs, &url, body);
            println!("{}", response);
        }
    }
}

fn get_queue_url (sqs: &SqsClient, name: &String) -> Result<String, GetQueueUrlError> {
    sqs.get_queue_url(GetQueueUrlRequest {
        queue_name: name.to_owned(),
        queue_owner_aws_account_id: None
    }).sync().map(|m| m.queue_url.expect("extracting url from response"))
}

fn get_approximate_queue_size (sqs: &SqsClient, url: &String) -> Result<u32, &'static str> {
    fn get_size (m: GetQueueAttributesResult) -> Option<u32> {
        m.attributes.and_then(|attr|
            attr.get("ApproximateNumberOfMessages")
                .and_then(|value| value.parse::<u32>().ok())
        )
    }

    sqs.get_queue_attributes(GetQueueAttributesRequest {
        queue_url: url.to_string(),
        attribute_names: Some(vec!("ApproximateNumberOfMessages".to_string()))
    }).sync().ok().and_then(get_size).ok_or("no count provided")
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

fn send_message (sqs: &SqsClient, url: &String, body: String) -> String {
    let response = sqs.send_message(SendMessageRequest {
        delay_seconds: None,
        message_attributes: None,
        message_body: body,
        message_deduplication_id: None,
        message_group_id: None,
        queue_url: url.to_string()
    }).sync().expect("sending message");

    let value = json!({
        "MD5OfMessageBody": response.md5_of_message_body.expect("getting md5 of body"),
        "MessageId": response.message_id.expect("getting message id"),
    });

    value.to_string()
}
