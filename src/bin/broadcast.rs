use std::{
    collections::{HashMap, HashSet},
    sync::mpsc,
};

use anyhow;
use vortex::*;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum BroadcastPayload {
    Broadcast {
        message: u64,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<u64>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}

enum BroadcastSignal {
    Gossip,
}

struct BroadcastService {
    msg_id: IdCounter,
    messages: HashSet<u64>,
    known: HashMap<String, HashSet<u64>>,
    messages_sent: HashMap<u64, u64>,
}

impl Service<BroadcastPayload, BroadcastSignal> for BroadcastService {
    fn create(sender: mpsc::Sender<Event<BroadcastPayload, BroadcastSignal>>) -> Self {
        Self {
            msg_id: IdCounter::new(),
            messages: HashSet::new(),
            known: HashMap::new(),
            messages_sent: HashMap::new(),
        }
    }
    fn step(
        &mut self,
        event: Event<BroadcastPayload, BroadcastSignal>,
        network: &mut Network,
    ) -> anyhow::Result<()> {
        match event {
            Event::Signal(_) => todo!(),
            Event::EOF => todo!(),
            Event::Message(msg) => {
                match msg.body.payload {
                    BroadcastPayload::Broadcast { message } => {
                        self.messages.insert(message);

                        network
                            .reply(
                                msg.src,
                                self.msg_id.next(),
                                msg.body.msg_id,
                                BroadcastPayload::BroadcastOk,
                            )
                            .context("Broadcast reply")?;

                        for neighbor in network.neighbors.clone() {
                            if neighbor != network.node_id
                                && !self
                                    .known
                                    .get(&neighbor)
                                    .is_some_and(|known| known.contains(&message))
                            {
                                let forward = Body {
                                    msg_id: self.msg_id.next(),
                                    in_reply_to: None,
                                    payload: BroadcastPayload::Broadcast { message },
                                };

                                self.messages_sent.insert(self.msg_id.peek(), message);

                                network
                                    .send(neighbor, forward)
                                    .context("Broadcast forward")?;
                            }
                        }
                    }
                    BroadcastPayload::Read => {
                        network
                            .reply(
                                msg.src,
                                self.msg_id.next(),
                                msg.body.msg_id,
                                BroadcastPayload::ReadOk {
                                    messages: self.messages.clone(),
                                },
                            )
                            .context("Read reply")?;
                    }
                    BroadcastPayload::Topology { mut topology } => {
                        network.neighbors = topology
                            .remove(&network.node_id)
                            .expect("Topology should contain information for all nodes");

                        network
                            .reply(
                                msg.src,
                                self.msg_id.next(),
                                msg.body.msg_id,
                                BroadcastPayload::TopologyOk,
                            )
                            .context("Read reply")?;
                    }
                    BroadcastPayload::BroadcastOk => {
                        let ent = self.known.entry(msg.src).or_insert(HashSet::new());

                        let in_reply_to = &msg
                            .body
                            .msg_id
                            .expect("BroadcastOk should have an in_reply_to");

                        if let Some(sent) = self.messages_sent.get(&in_reply_to) {
                            ent.insert(*sent);

                            // Space optimization
                            self.messages_sent.remove(&in_reply_to);
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    BroadcastService::run().context("Run generate service")
}
