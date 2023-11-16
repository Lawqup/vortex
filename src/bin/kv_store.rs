use std::collections::{HashMap, HashSet};

use anyhow::bail;
use serde::de::Error;
use serde::ser::{SerializeSeq, SerializeTuple};
use serde::Deserializer;
use ulid::Ulid;
use vortex::*;

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
    Applied { applied: String },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct RaftEntry {
    client: String,
    in_reply_to: Option<u64>,
    id: String,
    txn: Vec<Operation>,
}

struct KVService {
    msg_id: IdCounter,
    store: HashMap<i64, i64>,
    pending_replies: HashMap<String, (String, Option<u64>, KVPayload)>,
    who_applied: HashMap<String, HashSet<String>>,
    raft: RaftService<RaftEntry>,
}

type E = Event<KVPayload, (), RaftEntry>;

impl Service<KVPayload, (), RaftEntry> for KVService {
    fn create(network: &mut Network, sender: std::sync::mpsc::Sender<E>) -> Self {
        let raft = RaftService::create(network, sender.map_input(E::Raft));

        network.set_mesh_topology();
        Self {
            msg_id: IdCounter::new(),
            store: HashMap::new(),
            pending_replies: HashMap::new(),
            who_applied: HashMap::new(),
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
                    id,
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
                    eprintln!("STORE {:?}", self.store);

                    self.pending_replies.insert(
                        id.clone(),
                        (client, in_reply_to, KVPayload::TxnOk { txn: res }),
                    );

                    let mut set = HashSet::new();
                    set.insert(network.node_id.clone());

                    self.who_applied.insert(id.clone(), set);

                    for other in network.all_nodes.clone() {
                        if other == network.node_id {
                            continue;
                        }

                        network
                            .send(
                                other,
                                Body {
                                    msg_id: self.msg_id.next(),
                                    in_reply_to: None,
                                    payload: KVPayload::Applied {
                                        applied: id.clone(),
                                    },
                                },
                            )
                            .context("Send applied kv")?;
                    }
                }
            },
            Event::Message(msg) => match msg.body.payload {
                KVPayload::Txn { txn } => {
                    self.raft
                        .request(
                            RaftEntry {
                                client: msg.src,
                                in_reply_to: msg.body.msg_id,
                                id: Ulid::new().to_string(),
                                txn,
                            },
                            network,
                        )
                        .context("Requesting raft")?;
                }
                KVPayload::TxnOk { .. } => bail!("Unexpected msg variant"),
                KVPayload::Applied { applied } => {
                    self.who_applied
                        .entry(applied.clone())
                        .or_insert(HashSet::new())
                        .insert(msg.src);

                    if self.who_applied[&applied].len() == network.all_nodes.len() {
                        let (client, in_reply_to, payload) = self
                            .pending_replies
                            .remove(&applied)
                            .expect("All nodes, including this, have pending");

                        self.who_applied.remove(&applied);

                        eprintln!("Responding: {payload:?}");

                        network
                            .reply(client, self.msg_id.next(), in_reply_to, payload)
                            .context("Transaction reply")?;
                    }
                }
            },
            Event::EOF | Event::Signal(_) => bail!("Unexpected event"),
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    KVService::run().context("Run kv service")
}
