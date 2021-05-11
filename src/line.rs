use pulsar::{
    message::Payload, producer, DeserializeMessage, Error as PulsarError, SerializeMessage,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Line {
    pub id: usize,
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
