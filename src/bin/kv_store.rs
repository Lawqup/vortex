use std::collections::HashMap;

use anyhow::bail;
use serde::de::Error;
use serde::ser::{SerializeSeq, SerializeTuple};
use serde::Deserializer;
use vortex_raft::*;

#[derive(Debug, Clone)]
enum Operation {
    Read { key: i64 },
    Write { key: i64, val: i64 },
}

#[derive(Debug, Clone, Deserialize)]
enum OperationOk {
    Read { key: i64, val: Option<i64> },
    Write { key: i64, val: i64 },
}

impl<'de> Deserialize<'de> for Operation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data: (char, i64, Option<i64>) = Deserialize::deserialize(deserializer)?;

        match data.0 {
            'r' => Ok(Operation::Read { key: data.1 }),
            'w' => Ok(Operation::Write {
                key: data.1,
                val: data
                    .2
                    .ok_or(D::Error::custom("Could not deserialize write op"))?,
            }),
            _ => Err(D::Error::custom("Could not deserialize operation")),
        }
    }
}

impl Serialize for Operation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(None)?;

        match self {
            Operation::Read { key } => {
                seq.serialize_element(&'r')?;
                seq.serialize_element(key)?;
                seq.serialize_element(&None::<i64>)?;
            }
            Operation::Write { key, val } => {
                seq.serialize_element(&'w')?;
                seq.serialize_element(key)?;
                seq.serialize_element(val)?;
            }
        }

        seq.end()
    }
}

impl Serialize for OperationOk {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut tup = serializer.serialize_tuple(3)?;

        match self {
            OperationOk::Read { key, val } => {
                tup.serialize_element(&'r')?;
                tup.serialize_element(key)?;
                tup.serialize_element(val)?;
            }
            OperationOk::Write { key, val } => {
                tup.serialize_element(&'w')?;
                tup.serialize_element(key)?;
                tup.serialize_element(val)?;
            }
        }

        tup.end()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum KVPayload {
    Txn { txn: Vec<Operation> },
    TxnOk { txn: Vec<OperationOk> },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct RaftEntry {
    client: String,
    in_reply_to: Option<u64>,
    txn: Vec<Operation>,
}

struct KVService {
    msg_id: IdCounter,
    store: HashMap<i64, i64>,
    raft: RaftService<RaftEntry>,
}

type E = Event<KVPayload, (), RaftEntry>;

impl Service<KVPayload, (), RaftEntry> for KVService {
    fn create(network: &mut Network, sender: std::sync::mpsc::Sender<E>) -> Self {
        let mut raft = RaftService::create(network, sender.map_input(E::Raft));

        if network.node_id == "n0" {
            let _ = raft.become_leader(network);
        }

        network.set_mesh_topology();
        Self {
            msg_id: IdCounter::new(),
            store: HashMap::new(),
            raft,
        }
    }

    fn step(&mut self, input: E, network: &mut Network) -> anyhow::Result<()> {
        match input {
            Event::Raft(e) => match e {
                RaftEvent::RaftMessage(msg) => {
                    self.raft.step(RaftEvent::RaftMessage(msg), network)?;
                }
                RaftEvent::RaftSignal(sig) => {
                    self.raft.step(RaftEvent::RaftSignal(sig), network)?;
                }
                RaftEvent::CommitedEntry(RaftEntry {
                    client,
                    in_reply_to,
                    txn,
                }) => {
                    let mut res = Vec::new();
                    for op in txn {
                        match op {
                            Operation::Read { key } => res.push(OperationOk::Read {
                                key,
                                val: self.store.get(&key).copied(),
                            }),
                            Operation::Write { key, val } => {
                                self.store.insert(key, val);
                                res.push(OperationOk::Write { key, val })
                            }
                        }
                    }
                    eprintln!("REPLYING");

                    network
                        .reply(
                            client,
                            self.msg_id.next(),
                            in_reply_to,
                            KVPayload::TxnOk { txn: res },
                        )
                        .context("Transaction reply")?;
                }
            },
            Event::Message(msg) => match msg.body.payload {
                KVPayload::Txn { ref txn } => {
                    if self.raft.is_leader() {
                        eprintln!("REQUESTING");
                        self.raft
                            .request(
                                RaftEntry {
                                    client: msg.src,
                                    in_reply_to: msg.body.msg_id,
                                    txn: txn.clone(),
                                },
                                network,
                            )
                            .context("Requesting raft")?;
                    } else if let Some(voted_for) = self.raft.voted_for() {
                        // Not leader, but voted for self (ie campaigning)
                        if voted_for == network.node_id {
                            return Ok(());
                        }

                        eprintln!("FORWARDING");
                        let fwd = Message {
                            dest: voted_for,
                            ..msg
                        };
                        network.send_msg(fwd).context("Forward to leader")?;
                    }
                }
                KVPayload::TxnOk { .. } => bail!("Unexpected msg variant"),
            },
            Event::EOF | Event::Signal(_) => bail!("Unexpected event"),
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    KVService::run().context("Run kv service")
}
