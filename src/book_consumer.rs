use crate::{line::Line, PULSAR_ADDRESS};
use anyhow::{Context, Result};
use log::{debug, error, info};
use pulsar::{
    executor::Executor, message::proto, producer, Consumer, DeserializeMessage, Pulsar, SubType,
    TokioExecutor,
};
use std::fs::File;
use std::io::{prelude::*, BufReader};

pub struct ReconstructedBook {
    title: String,
    lines: Vec<Line>,
}

pub struct BookConsumer {
    pub reconstructed_books: Vec<ReconstructedBook>,
    pub consumer: Consumer<Line, TokioExecutor>,
}

impl BookConsumer {
    pub async fn new<S, I>(topics: I) -> Result<BookConsumer>
    where
        S: AsRef<str>,
        I: IntoIterator<Item = S>,
    {
        let consumer: Consumer<Line, _> = Pulsar::builder(PULSAR_ADDRESS, TokioExecutor)
            .build()
            .await?
            .consumer()
            .with_topics(topics)
            .with_consumer_name("line_consumer")
            .with_subscription_type(SubType::Exclusive)
            .with_subscription("book_subscription")
            .build()
            .await?;
        Ok(BookConsumer {
            reconstructed_books: Vec::new(),
            consumer,
        })
    }
}
