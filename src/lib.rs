pub mod raft;

pub use anyhow::Context;
pub use raft::*;
pub use serde::{Deserialize, Serialize};
pub use std::io::{StdoutLock, Write};

use serde::de::DeserializeOwned;
use std::{
    fmt::Debug,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Sender},
        Arc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum Event<Payload: Clone, Signal = (), LogEntry: Clone = ()> {
    Message(Message<Payload>),
    Signal(Signal),
    Raft(RaftEvent<LogEntry>),
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

    /// sqrt(n) root nodes, all with sqrt(n)-1 children
    /// Each child connects to all the root nodes
    pub fn set_sqrt_topology(&mut self) {
        if self.all_nodes.len() < 4 {
            self.set_mesh_topology();
            return;
        }

        let root_nodes = (self.all_nodes.len() as f64).sqrt() as usize;

        let idx = self
            .all_nodes
            .iter()
            .position(|n| n == &self.node_id)
            .unwrap_or_else(|| panic!("Node {} is unknown", self.node_id));

        self.neighbors = if idx % root_nodes == 0 {
            // Node is a root node
            (idx + 1..(idx + root_nodes).min(self.all_nodes.len()))
                .map(|i| self.all_nodes[i].clone())
                .collect()
        } else {
            // Node is a child node
            (0..self.all_nodes.len())
                .filter(|i| i % root_nodes == 0)
                .map(|i| self.all_nodes[i].clone())
                .collect()
        };
    }

    pub fn set_mesh_topology(&mut self) {
        self.neighbors = self.all_nodes.clone();
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

impl Default for IdCounter {
    fn default() -> Self {
        Self::new()
    }
}

pub trait Service<Payload, Signal = (), RaftEntry = ()>: Sized
where
    Payload: DeserializeOwned + Send + Clone + 'static + Debug,
    Signal: Send + 'static,
    RaftEntry: Clone + DeserializeOwned + Send + 'static + Debug,
{
    fn create(network: &mut Network, sender: Sender<Event<Payload, Signal, RaftEntry>>) -> Self;

    fn step(
        &mut self,
        input: Event<Payload, Signal, RaftEntry>,
        network: &mut Network,
    ) -> anyhow::Result<()>;

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
            let input = serde_json::Deserializer::from_reader(stdin)
                .into_iter::<Event<Payload, (), RaftEntry>>();

            for event in input {
                let event = event.context("Deserialize event")?;

                let event: Event<Payload, Signal, RaftEntry> = match event {
                    Event::Message(msg) => Event::Message(msg),
                    Event::Raft(msg) => Event::Raft(msg),
                    _ => bail!("Got local event over the network"),
                };

                if sender_clone.send(event).is_err() {
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

pub fn spawn_timer<F>(
    cb: Box<F>,
    dur: Duration,
    interrupt: Option<Arc<AtomicBool>>,
) -> JoinHandle<anyhow::Result<()>>
where
    F: Fn() -> anyhow::Result<()> + Send + 'static,
{
    thread::spawn(move || {
        let mut now = Instant::now();
        loop {
            if let Some(interrupt) = &interrupt {
                if interrupt.load(Ordering::Relaxed) {
                    now = Instant::now();
                    interrupt.store(false, Ordering::Relaxed);
                }
            }

            if now.elapsed() > dur {
                cb()?;
                now = Instant::now();
            }
        }
    })
}

pub trait SenderExt<T>: Send + Sync + 'static {
    fn send(&self, t: T) -> anyhow::Result<()>;

    fn map_input<U, F>(self, func: F) -> MapSender<Self, F>
    where
        Self: Sized,
        F: Fn(U) -> T,
    {
        MapSender { sender: self, func }
    }
}

impl<T: Send + 'static> SenderExt<T> for Sender<T> {
    fn send(&self, t: T) -> anyhow::Result<()> {
        self.send(t).map_err(|_| anyhow!("Send error"))
    }
}

#[derive(Clone)]
pub struct MapSender<S, F> {
    sender: S,
    func: F,
}

impl<S: SenderExt<U>, F, T, U> SenderExt<T> for MapSender<S, F>
where
    F: Fn(T) -> U + Clone + Send + Sync + 'static,
{
    fn send(&self, value: T) -> anyhow::Result<()> {
        self.sender.send((self.func)(value))
    }
}
