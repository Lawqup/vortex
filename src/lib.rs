pub use anyhow::Context;
use serde::de::DeserializeOwned;
pub use serde::{Deserialize, Serialize};
pub use std::io::{StdoutLock, Write};

use anyhow::bail;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Message<Payload: Clone> {
    pub src: String,
    pub dest: String,
    pub body: Body<Payload>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Body<Payload: Clone> {
    pub msg_id: Option<u64>,
    pub in_reply_to: Option<u64>,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InitPayload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
}

pub struct Output {
    output: StdoutLock<'static>,
    node_id: String,
    node_ids: Vec<String>,
}

impl Output {
    pub fn reply<Payload: Serialize + Clone>(
        &mut self,
        dest: String,
        msg_id: Option<u64>,
        in_reply_to: Option<u64>,
        payload: Payload,
    ) -> anyhow::Result<()> {
        let reply = Message {
            src: self.node_id.clone(),
            dest,
            body: Body {
                msg_id,
                in_reply_to,
                payload,
            },
        };

        serde_json::to_writer(&mut self.output, &reply).context("Serialize reply")?;
        self.output.write_all(b"\n")?;
        Ok(())
    }

    pub fn send<Payload: Serialize + Clone>(
        &mut self,
        dest: String,
        body: Body<Payload>,
    ) -> anyhow::Result<()> {
        let reply = Message {
            src: self.node_id.clone(),
            dest,
            body,
        };
        serde_json::to_writer(&mut self.output, &reply).context("Serialize message")?;
        self.output.write_all(b"\n")?;
        Ok(())
    }

    pub fn broadcast<Payload: Serialize + Clone>(
        &mut self,
        body: Body<Payload>,
    ) -> anyhow::Result<()> {
        for dest in self.node_ids.clone() {
            self.send(dest, body.clone())
                .context("Send message while broadcasting")?;
        }
        Ok(())
    }
}

pub fn initialize() -> anyhow::Result<Output> {
    let stdin = std::io::stdin().lock();
    let mut input =
        serde_json::Deserializer::from_reader(stdin).into_iter::<Message<InitPayload>>();

    let init = input
        .next()
        .expect("input will block until next message")
        .context("Deserialize init message")?;

    // Initialize state
    let (node_id, node_ids) = match &init.body.payload {
        InitPayload::Init { node_id, node_ids } => (node_id.clone(), node_ids.clone()),
        _ => bail!("First message should have been an init message"),
    };

    let mut output = Output {
        output: std::io::stdout().lock(),
        node_id,
        node_ids,
    };

    output
        .reply(init.src, None, init.body.msg_id, InitPayload::InitOk)
        .context("Init reply")?;

    Ok(output)
}

pub trait Service<Payload>: Sized
where
    Payload: DeserializeOwned + Clone,
{
    fn step(&mut self, input: Message<Payload>, output: &mut Output) -> anyhow::Result<()>;

    fn run(mut self, mut output: Output) -> anyhow::Result<()> {
        let stdin = std::io::stdin().lock();
        let input = serde_json::Deserializer::from_reader(stdin).into_iter::<Message<Payload>>();

        for msg in input {
            let msg = msg.context("Deserialize messge")?;
            self.step(msg, &mut output)?;
        }

        Ok(())
    }
}
