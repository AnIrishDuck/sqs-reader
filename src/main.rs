extern crate rusoto_core;
extern crate rusoto_sqs;

#[macro_use]
extern crate serde_json;

use std::collections::HashMap;

use rusoto_core::Region;
use rusoto_sqs::{
    DeleteMessageRequest,
    GetQueueUrlRequest,
    Message,
    ReceiveMessageRequest,
    SqsClient,
    Sqs
};

fn main () {
    let region = Region::UsWest2;

    let sqs = SqsClient::new(region);
    let url = sqs.get_queue_url(GetQueueUrlRequest {
        queue_name: "fmurphy-test-queue".to_string(),
        queue_owner_aws_account_id: None
    }).sync().unwrap().queue_url.unwrap();

    let mut all_messages = HashMap::new();
    while all_messages.len() < 4 {
        let response = sqs.receive_message(ReceiveMessageRequest {
            attribute_names: Some(vec!("All".to_string())),
            max_number_of_messages: Some(1),
            message_attribute_names: None,
            queue_url: url.to_string(),
            receive_request_attempt_id: None,
            visibility_timeout: Some(0),
            wait_time_seconds: None
        }).sync().unwrap();

        match response.messages {
            Some(messages) => {
                for message in messages {
                    let id = message.clone().message_id.unwrap();
                    all_messages.insert(id, message);
                }
            },
            None => (),
        }
    }

    // Wait until all messages have been received to purge them. This reduces,
    // but does not eliminate, the chance of message "loss". One of the API
    // calls below can still theoretically panic.
    if false {
        for (_id, message) in &all_messages {
            let copy = message.clone();
            sqs.delete_message(DeleteMessageRequest {
                queue_url: url.to_string(),
                receipt_handle: copy.receipt_handle.unwrap()
            }).sync().unwrap();
        }
    }

    for (_id, message) in all_messages {
        if true {
            print_full_message(message);
        } else {
            println!("{}", message.body.unwrap());
        }
    }
}

fn print_full_message (message: Message) {
    let attributes = message.attributes.unwrap_or(HashMap::new());
    let value = json!({
        "Body": message.body.unwrap(),
        "ReceiptHandle": message.receipt_handle.unwrap(),
        "MD5OfBody": message.md5_of_body.unwrap(),
        "MessageId": message.message_id.unwrap(),
        "Attributes": attributes,
    });

    println!("{}", value.to_string());
}
