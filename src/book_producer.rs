use crate::{line::Line, PULSAR_ADDRESS};
use pulsar::{
    executor::Executor, message::proto, producer, Consumer, DeserializeMessage, Pulsar, SubType,
    TokioExecutor,
};
use std::fs::File;
use std::io::{BufRead, BufReader};

/// This
pub struct BookProducer<Exe: Executor> {
    pub book_title: String,
    pub producer: producer::Producer<Exe>,
}

impl<Exe: Executor> BookProducer<Exe> {
    pub async fn new(
        book_title: &str,
    ) -> Result<BookProducer<TokioExecutor>, pulsar::Error> {
        let pulsar = Pulsar::builder(PULSAR_ADDRESS, TokioExecutor)
            .build()
            .await?;

        let producer = pulsar
            .producer()
            .with_topic(book_title)
            .with_options(producer::ProducerOptions {
                schema: Some(proto::Schema {
                    r#type: proto::schema::Type::String as i32,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .build()
            .await?;

        Ok(BookProducer::<TokioExecutor> {
            book_title: book_title.to_string(),
            producer,
        })
    }

    pub async fn stream_book(self) {
        let path = format!("books/{}", self.book_title);
        let reader = BufReader::new(File::open(path).expect("Cannot not open file"));
        for line in reader.lines() {
            println!("{}", line.unwrap());
        }
    }
}
