use std::io::{StdoutLock, Write};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
struct Message {
    src: String,
    dest: String,
    body: Body,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct Body {
    msg_id: Option<u64>,
    in_reply_to: Option<u64>,
    #[serde(flatten)]
    payload: Payload,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
}

struct EchoService {
    id: u64,
}

impl EchoService {
    pub fn step(&mut self, input: Message, output: &mut StdoutLock) -> anyhow::Result<()> {
        let echo = match input.body.payload {
            Payload::Echo { echo } => echo,
            _ => return Ok(()),
        };

        let reply = Message {
            src: input.dest,
            dest: input.src,
            body: Body {
                msg_id: Some(self.id),
                in_reply_to: input.body.msg_id,
                payload: Payload::EchoOk { echo },
            },
        };

        self.id += 1;

        serde_json::to_writer(&mut *output, &reply).context("Serialize echo reply")?;
        output.write_all(b"\n")?;
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    let mut input = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();

    let init = input
        .next()
        .expect("input will block until next message")
        .context("Deserialize init message")?;

    // Initialize state
    let (node_id, node_ids) = match init.body.payload {
        Payload::Init { node_id, node_ids } => (node_id, node_ids),
        _ => bail!("First message should have been an init message"),
    };

    let init_reply = Message {
        src: node_id.clone(),
        dest: init.src,
        body: Body {
            msg_id: None,
            in_reply_to: init.body.msg_id,
            payload: Payload::InitOk,
        },
    };

    serde_json::to_writer(&mut stdout, &init_reply).context("Serialize init reply")?;
    stdout.write_all(b"\n")?;

    let mut echo_state = EchoService { id: 0 };

    for msg in input {
        let msg = msg.context("Deserialize messge")?;
        echo_state.step(msg, &mut stdout)?;
    }

    Ok(())
}
