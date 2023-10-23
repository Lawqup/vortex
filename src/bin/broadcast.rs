use std::{
    collections::{HashMap, HashSet},
    sync::mpsc,
    thread,
    time::Duration,
};

use anyhow;
use rand::Rng;
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
    Gossip {
        known: HashSet<u64>,
    },
}

enum BroadcastSignal {
    Gossip,
}

struct BroadcastService {
    msg_id: IdCounter,
    messages: HashSet<u64>,
    known: HashMap<String, HashSet<u64>>,
}

impl Service<BroadcastPayload, BroadcastSignal> for BroadcastService {
    fn create(
        network: &mut Network,
        sender: mpsc::Sender<Event<BroadcastPayload, BroadcastSignal>>,
    ) -> Self {
        thread::spawn(move || loop {
            // TODO: try smaller
            thread::sleep(Duration::from_millis(150));
            if let Err(_) = sender.send(Event::Signal(BroadcastSignal::Gossip)) {
                return Ok::<_, anyhow::Error>(());
            }
        });

        // sqrt(n) root nodes, all with sqrt(n)-1 children
        // Each child connects to all the root nodes

        let root_nodes = (network.all_nodes.len() as f64).sqrt() as usize;

        let idx = network
            .all_nodes
            .iter()
            .position(|n| n == &network.node_id)
            .unwrap_or_else(|| panic!("Node {} is unknown", network.node_id));

        network.neighbors = if idx % root_nodes == 0 {
            // Node is a root node
            (idx + 1..(idx + root_nodes).min(network.all_nodes.len()))
                .map(|i| network.all_nodes[i].clone())
                .collect()
        } else {
            // Node is a child node
            (0..network.all_nodes.len())
                .filter(|i| i % root_nodes == 0)
                .map(|i| network.all_nodes[i].clone())
                .collect()
        };

        eprintln!("{}:{:?}", network.node_id, network.neighbors);

        Self {
            msg_id: IdCounter::new(),
            messages: HashSet::new(),
            known: network
                .all_nodes
                .clone()
                .into_iter()
                .map(|id| (id, HashSet::new()))
                .collect(),
        }
    }
    fn step(
        &mut self,
        event: Event<BroadcastPayload, BroadcastSignal>,
        network: &mut Network,
    ) -> anyhow::Result<()> {
        match event {
            Event::EOF => todo!(),
            Event::Signal(signal) => {
                match signal {
                    BroadcastSignal::Gossip => {
                        for neighbor in network.neighbors.clone() {
                            let known_to_neighbor = &self.known[&neighbor];
                            let (known, mut to_send): (HashSet<_>, HashSet<_>) = self
                                .messages
                                .iter()
                                .copied()
                                .partition(|msg| known_to_neighbor.contains(msg));

                            // A tells B it knows 1,2,3
                            // B now knows A knows 1,2,3
                            // Thus, B never tells A it knows 1,2,3
                            //
                            // So, send a random fixed-size subset elements of what is
                            // already known to let A know B knows

                            let mut rng = rand::thread_rng();
                            to_send.extend(known.iter().copied().filter(|_| {
                                rng.gen_ratio(10.min(known.len() as u32), known.len() as u32)
                            }));
                            network
                                .send(
                                    neighbor.clone(),
                                    Body {
                                        msg_id: None,
                                        in_reply_to: None,
                                        payload: BroadcastPayload::Gossip { known: to_send },
                                    },
                                )
                                .context("Sending gossip message")?;
                        }
                    }
                }
            }
            Event::Message(msg) => match msg.body.payload {
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
                BroadcastPayload::Topology { topology: _ } => {
                    // Implement our own topology

                    // network.neighbors = topology
                    //     .remove(&network.node_id)
                    //     .expect("Topology should contain information for all nodes");

                    network
                        .reply(
                            msg.src,
                            self.msg_id.next(),
                            msg.body.msg_id,
                            BroadcastPayload::TopologyOk,
                        )
                        .context("Read reply")?;
                }
                BroadcastPayload::Gossip { known } => {
                    self.known
                        .get_mut(&msg.src)
                        .expect("Initialized all nodes in the map at creation")
                        .extend(known.clone());

                    self.messages.extend(known);
                }
                _ => {}
            },
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    BroadcastService::run().context("Run generate service")
}
