pub use anyhow::Context;
pub use serde::{Deserialize, Serialize};
pub use std::io::{StdoutLock, Write};

use anyhow::bail;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Message<Payload> {
    pub src: String,
    pub dest: String,
    pub body: Body<Payload>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Body<Payload> {
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

#[derive(Debug)]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

pub struct Output {
    output: StdoutLock<'static>,
}

impl Output {
    pub fn reply<MP, BP: Serialize>(
        &mut self,
        msg: Message<MP>,
        body: Body<BP>,
    ) -> anyhow::Result<()> {
        let reply = Message {
            src: msg.dest,
            dest: msg.src,
            body,
        };

        serde_json::to_writer(&mut self.output, &reply).context("Serialize reply")?;
        self.output.write_all(b"\n")?;
        Ok(())
    }
}

pub fn initialize() -> anyhow::Result<Init> {
    let mut output = Output {
        output: std::io::stdout().lock(),
    };

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

    let msg_id = init.body.msg_id;
    output
        .reply(
            init,
            Body {
                msg_id: None,
                in_reply_to: msg_id,
                payload: InitPayload::InitOk,
            },
        )
        .context("Init reply")?;

    Ok(Init { node_id, node_ids })
}

pub trait Service<Payload>: Sized
where
    Payload: Deserialize<'static>,
{
    fn step(&mut self, input: Message<Payload>, output: &mut Output) -> anyhow::Result<()>;

    fn run(mut self) -> anyhow::Result<()> {
        let mut output = Output {
            output: std::io::stdout().lock(),
        };

        let stdin = std::io::stdin().lock();
        let input = serde_json::Deserializer::from_reader(stdin).into_iter::<Message<Payload>>();

        for msg in input {
            let msg = msg.context("Deserialize messge")?;
            self.step(msg, &mut output)?;
        }

        Ok(())
    }
}
