use std::{
    collections::{HashMap, HashSet},
    sync::mpsc,
    time::Duration,
};

use anyhow::{self, bail};
use rand::Rng;
use ulid::Ulid;
use vortex_raft::*;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum CounterPayload {
    Add { delta: u64 },
    AddOk,
    Read,
    ReadOk { value: u64 },
    Gossip { deltas: HashMap<String, u64> },
}

#[derive(Debug, Clone, Copy)]
enum BroadcastSignal {
    Gossip,
}

struct CounterService {
    msg_id: IdCounter,
    /// Ulid to all the deltas added
    deltas: HashMap<String, u64>,
    known: HashMap<String, HashSet<String>>,
}

impl Service<CounterPayload, BroadcastSignal> for CounterService {
    fn create(
        network: &mut Network,
        sender: mpsc::Sender<Event<CounterPayload, BroadcastSignal>>,
    ) -> Self {
        spawn_timer(
            Box::new(move || {
                let _ = sender.send(Event::Signal(BroadcastSignal::Gossip));
                Ok(())
            }),
            Duration::from_millis(150),
            None,
        );

        network.set_sqrt_topology();
        Self {
            msg_id: IdCounter::new(),
            deltas: HashMap::new(),
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
        event: Event<CounterPayload, BroadcastSignal>,
        network: &mut Network,
    ) -> anyhow::Result<()> {
        match event {
            Event::Raft(_) | Event::EOF => {
                bail!("Unexpected event recieved: {event:?}")
            }
            Event::Signal(signal) => match signal {
                BroadcastSignal::Gossip => {
                    for neighbor in network.neighbors.clone() {
                        let known_to_neighbor = &self.known[&neighbor];
                        let (known, mut to_send): (HashSet<_>, HashSet<_>) = self
                            .deltas
                            .keys()
                            .cloned()
                            .partition(|msg| known_to_neighbor.contains(msg));

                        // A tells B it knows 1,2,3
                        // B now knows A knows 1,2,3
                        // Thus, B never tells A it knows 1,2,3
                        //
                        // So, send a random fixed-size subset elements of what is
                        // already known to let A know B knows

                        let mut rng = rand::thread_rng();
                        to_send.extend(
                            known
                                .iter()
                                .filter(|_| {
                                    rng.gen_ratio(10.min(known.len() as u32), known.len() as u32)
                                })
                                .cloned(),
                        );

                        let deltas = to_send
                            .into_iter()
                            .map(|ulid| {
                                let delta = self.deltas[&ulid];
                                (ulid, delta)
                            })
                            .collect();

                        network
                            .send(
                                neighbor.clone(),
                                Body {
                                    msg_id: None,
                                    in_reply_to: None,
                                    payload: CounterPayload::Gossip { deltas },
                                },
                            )
                            .context("Sending gossip message")?;
                    }
                }
            },
            Event::Message(msg) => match msg.body.payload {
                CounterPayload::Add { delta } => {
                    self.deltas.insert(Ulid::new().to_string(), delta);

                    network
                        .reply(
                            msg.src,
                            self.msg_id.next(),
                            msg.body.msg_id,
                            CounterPayload::AddOk,
                        )
                        .context("Broadcast reply")?;
                }
                CounterPayload::Read => {
                    network
                        .reply(
                            msg.src,
                            self.msg_id.next(),
                            msg.body.msg_id,
                            CounterPayload::ReadOk {
                                value: self.deltas.values().sum(),
                            },
                        )
                        .context("Read reply")?;
                }
                CounterPayload::Gossip { deltas } => {
                    self.known
                        .get_mut(&msg.src)
                        .expect("Initialized all nodes in the map at creation")
                        .extend(deltas.keys().cloned().collect::<HashSet<_>>());

                    self.deltas.extend(deltas)
                }
                _ => {}
            },
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    CounterService::run().context("Run generate service")
}
