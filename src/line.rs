use pulsar::{
    message::Payload, producer, DeserializeMessage, Error as PulsarError, SerializeMessage,
};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Serialize, Deserialize, Clone)]
pub struct Line {
    pub id: usize,
    pub book_title: String,
    pub data: String,
}

impl SerializeMessage for Line {
    fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
        let payload = serde_json::to_vec(&input).map_err(|e| PulsarError::Custom(e.to_string()))?;
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

impl DeserializeMessage for Line {
    type Output = Result<Line, serde_json::Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        serde_json::from_slice(&payload.data)
    }
}

impl fmt::Display for Line {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} #{}", self.book_title, self.id)
    }
}

// impl Clone for Line {
//     fn clone(&self) -> Self {
//         Self {
//             id: self.id.clone(),
//             book_title: self.book_title.clone(),
//             data: self.data.clone(),
//         }
//     }
// }
