use std::{collections::HashMap, sync::mpsc};

use anyhow;
use vortex::*;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum LogPayload {
    Send {
        key: String,
        msg: u64,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        msgs: HashMap<String, Vec<(usize, u64)>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
}

struct LogService {
    msg_id: IdCounter,
    raft: RaftService<()>,
    logs: HashMap<String, Log>,
}

impl Service<LogPayload> for LogService {
    fn create(network: &mut Network, sender: mpsc::Sender<Event<LogPayload>>) -> Self {
        let raft = RaftService::create(network, sender.map_input(|s| Event::RaftSignal(s)));

        network.set_sqrt_topology();
        Self {
            msg_id: IdCounter::new(),
            logs: HashMap::new(),
            raft,
        }
    }
    fn step(&mut self, event: Event<LogPayload>, network: &mut Network) -> anyhow::Result<()> {
        match event {
            Event::Signal(_) => todo!(),
            Event::EOF => todo!(),
            Event::RaftMessage(message) => {
                self.raft.step(Event::RaftMessage(message), network)?;
            }
            Event::RaftSignal(signal) => {
                self.raft.step(Event::RaftSignal(signal), network)?;
            }
            Event::Message(message) => match message.body.payload {
                LogPayload::Send { key, msg } => {
                    let log = self.logs.entry(key).or_insert(Log::new());

                    network
                        .reply(
                            message.src,
                            self.msg_id.next(),
                            message.body.msg_id,
                            LogPayload::SendOk {
                                offset: log.push(msg),
                            },
                        )
                        .context("Send reply")?;
                }
                LogPayload::Poll { offsets } => {
                    let mut msgs = HashMap::new();

                    for (key, offset) in offsets {
                        if let Some(log) = self.logs.get(&key) {
                            msgs.insert(key, log.poll(offset));
                        }
                    }

                    network
                        .reply(
                            message.src,
                            self.msg_id.next(),
                            message.body.msg_id,
                            LogPayload::PollOk { msgs },
                        )
                        .context("Poll reply")?;
                }
                LogPayload::CommitOffsets { offsets } => {
                    for (key, offset) in offsets {
                        if let Some(log) = self.logs.get_mut(&key) {
                            log.commit(offset);
                        }
                    }

                    network
                        .reply(
                            message.src,
                            self.msg_id.next(),
                            message.body.msg_id,
                            LogPayload::CommitOffsetsOk,
                        )
                        .context("Commit reply")?;
                }
                LogPayload::ListCommittedOffsets { keys } => {
                    let mut offsets = HashMap::new();

                    for key in keys {
                        if let Some(log) = self.logs.get(&key) {
                            offsets.insert(key, log.commited());
                        }
                    }

                    network
                        .reply(
                            message.src,
                            self.msg_id.next(),
                            message.body.msg_id,
                            LogPayload::ListCommittedOffsetsOk { offsets },
                        )
                        .context("List commit reply")?;
                }
                LogPayload::SendOk { .. }
                | LogPayload::PollOk { .. }
                | LogPayload::CommitOffsetsOk
                | LogPayload::ListCommittedOffsetsOk { .. } => {}
            },
        };

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    LogService::run().context("Run generate service")
}

pub struct Log {
    first_uncommitted: usize,
    entries: Vec<Option<u64>>,
}

impl Log {
    pub fn new() -> Self {
        Self {
            first_uncommitted: 0,
            entries: Vec::new(),
        }
    }

    pub fn push(&mut self, msg: u64) -> usize {
        self.entries.push(Some(msg));
        self.entries.len() - 1
    }

    pub fn poll(&self, from: usize) -> Vec<(usize, u64)> {
        (from..)
            .zip(self.entries[from..].into_iter().copied())
            .filter_map(|(i, e)| e.map(|ofs| (i, ofs)))
            .collect()
    }

    pub fn commit(&mut self, up_to: usize) {
        if self.first_uncommitted < up_to {
            self.first_uncommitted = up_to + 1
        }
    }

    pub fn commited(&self) -> usize {
        self.first_uncommitted.saturating_sub(1)
    }
}
