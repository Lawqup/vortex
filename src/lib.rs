pub use anyhow::Context;
use serde::de::DeserializeOwned;
pub use serde::{Deserialize, Serialize};
pub use std::io::{StdoutLock, Write};
use std::{
    sync::mpsc::{self, Sender},
    thread,
};

use anyhow::bail;

pub enum Event<Payload: Clone, Signal = ()> {
    Message(Message<Payload>),
    Signal(Signal),
    EOF,
}

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

pub struct Network {
    output: StdoutLock<'static>,
    pub node_id: String,
    pub neighbors: Vec<String>,
    pub all_nodes: Vec<String>,
}

impl Network {
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
}

pub struct IdCounter(u64);

impl IdCounter {
    pub fn new() -> Self {
        Self(0)
    }
    pub fn next(&mut self) -> Option<u64> {
        let prev = self.0;
        self.0 += 1;
        Some(prev)
    }
    pub fn peek(&mut self) -> u64 {
        self.0
    }
}

pub trait Service<Payload, Signal = ()>: Sized
where
    Payload: DeserializeOwned + Send + Clone + 'static,
    Signal: Send + 'static,
{
    fn create(network: &mut Network, sender: Sender<Event<Payload, Signal>>) -> Self;

    fn step(&mut self, input: Event<Payload, Signal>, network: &mut Network) -> anyhow::Result<()>;

    fn run() -> anyhow::Result<()> {
        let mut stdin = std::io::stdin().lock();

        let mut input =
            serde_json::Deserializer::from_reader(&mut stdin).into_iter::<Message<InitPayload>>();

        let init = input
            .next()
            .expect("input will block until next message")
            .context("Deserialize init message")?;

        // Initialize state
        let (node_id, all_nodes) = match &init.body.payload {
            InitPayload::Init { node_id, node_ids } => (node_id.clone(), node_ids.clone()),
            _ => bail!("First message should have been an init message"),
        };

        let mut network = Network {
            output: std::io::stdout().lock(),
            node_id,
            neighbors: Vec::new(),
            all_nodes,
        };

        let (sender, receiver) = mpsc::channel();

        network
            .reply(init.src, None, init.body.msg_id, InitPayload::InitOk)
            .context("Init reply")?;

        drop(stdin);
        let sender_clone = sender.clone();
        let handle = thread::spawn(move || {
            let stdin = std::io::stdin().lock();
            let input =
                serde_json::Deserializer::from_reader(stdin).into_iter::<Message<Payload>>();

            for msg in input {
                let msg = msg.context("Deserialize messge")?;

                // If connection is closed, wrap things up
                if let Err(_) = sender_clone.send(Event::Message(msg)) {
                    return Ok::<_, anyhow::Error>(());
                }
            }

            let _ = sender_clone.send(Event::EOF);

            Ok(())
        });

        let mut service = Self::create(&mut network, sender);
        for event in receiver {
            service.step(event, &mut network)?;
        }

        handle
            .join()
            .expect("Stdin reader thread panicked")
            .context("Stdin reader thread err")?;

        Ok(())
    }
}
