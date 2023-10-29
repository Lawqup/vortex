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

struct Log {
    committed: usize,
    entries: Vec<Option<u64>>,
}

impl Log {
    fn new() -> Self {
        Self {
            // This implies 0th entry is always commited, which is a dummy value
            committed: 0,
            entries: vec![None],
        }
    }

    fn push(&mut self, msg: u64) -> usize {
        self.entries.push(Some(msg));
        self.entries.len() - 1
    }

    fn poll(&self, from: usize) -> Vec<(usize, u64)> {
        (from..)
            .zip(self.entries[from..].into_iter().copied())
            .filter_map(|(i, e)| e.map(|ofs| (i, ofs)))
            .collect()
    }

    fn commit(&mut self, up_to: usize) {
        if self.committed < up_to {
            self.committed = up_to + 1
        }
    }
}

struct LogService {
    msg_id: IdCounter,
    logs: HashMap<String, Log>,
}

impl Service<LogPayload> for LogService {
    fn create(network: &mut Network, _sender: mpsc::Sender<Event<LogPayload>>) -> Self {
        network.set_sqrt_topology();
        Self {
            msg_id: IdCounter::new(),
            logs: HashMap::new(),
        }
    }
    fn step(&mut self, event: Event<LogPayload>, network: &mut Network) -> anyhow::Result<()> {
        let Event::Message(message) = event else {
            panic!("Log should only recieve messages");
        };
        match message.body.payload {
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
                        offsets.insert(key, log.committed);
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
        };

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    LogService::run().context("Run generate service")
}
