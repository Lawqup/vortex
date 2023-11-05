use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use rand::Rng;

use crate::*;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum RaftPayload<Entry = ()> {
    RequestVote {
        /// candidate’s term
        term: u64,
        /// index of candidate’s last log entry
        last_log_index: usize,
        /// term of candidate’s last log entry
        last_log_term: u64,
    },
    RequestVoteOk {
        /// currentTerm, for candidate to update itself
        term: u64,
        /// true means candidate received vote
        vote_granted: bool,
    },
    /// Invoked by leader to replicate log entries; also used as heartbeat.
    AppendEntries {
        /// leader’s term
        term: u64,
        /// index of log entry immediately preceding new ones
        prev_log_index: usize,
        /// term of prev_log_index entry
        prev_log_term: u64,
        entries: Vec<(Entry, u64)>,
    },
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum RaftSignal {
    Heartbeat,
    Campaign,
}

pub struct RaftService<Entry> {
    msg_id: IdCounter,
    /// latest term server has seen
    current_term: u64,
    /// log entries; each entry contains command for state machine, and term when entry was received by leader
    log: Vec<Option<(Entry, u64)>>,
    /// index of highest log entry known to be committed
    commit_index: usize,
    /// index of highest log entry applied to state machine
    last_applied: usize,
    /// Node that received our vote in current term
    voted_for: Option<String>,
    /// for each server, index of the next log entry to send to that server
    next_index: HashMap<String, usize>,
    /// for each server, index of highest log entry known to be replicated on server
    match_index: HashMap<String, usize>,
    /// Votes recieved from servers
    votes: HashSet<String>,
    /// True iff self is leader
    is_leader: bool,
    /// Whether or not to reset the election timeout
    /// (set to true only when leader sends an AppendEntries)
    reset_election_timer: Arc<AtomicBool>,
}

impl<Entry: Clone + Serialize> RaftService<Entry> {
    pub fn create(network: &mut Network, sender: impl SenderExt<RaftSignal>) -> Self {
        // Between 150 and 300 as per Raft spec
        let election_timeout = Duration::from_millis(rand::thread_rng().gen_range(150..=300));
        let heartbeat_timeout = Duration::from_millis(150);

        let reset_election_timer = Arc::new(AtomicBool::new(false));
        let sender_clone = sender.clone();
        spawn_timer(
            Box::new(move || {
                let _ = sender_clone.send(RaftSignal::Campaign);
                Ok(())
            }),
            election_timeout,
            Some(reset_election_timer.clone()),
        );

        spawn_timer(
            Box::new(move || {
                let _ = sender.send(RaftSignal::Heartbeat);
                Ok(())
            }),
            heartbeat_timeout,
            None,
        );

        Self {
            msg_id: IdCounter::new(),
            log: vec![None],
            commit_index: 0,
            last_applied: 0,
            current_term: 0,
            voted_for: None,
            next_index: network.all_nodes.iter().cloned().map(|n| (n, 1)).collect(),
            match_index: network.all_nodes.iter().cloned().map(|n| (n, 0)).collect(),
            votes: HashSet::new(),
            is_leader: false,
            reset_election_timer,
        }
    }

    fn last_log_index(&self) -> usize {
        self.log.len() - 1
    }

    fn last_log_term(&self) -> u64 {
        self.log[self.last_log_index()]
            .as_ref()
            .map(|(_, term)| *term)
            .unwrap_or(0)
    }

    fn delay_election(&self) {
        self.reset_election_timer.store(true, Ordering::Relaxed);
    }

    fn update_term(&mut self, term: u64) {
        self.current_term = term;
        self.is_leader = false;
        self.votes.clear();
        // Voted for no one in this new term
        self.voted_for = None;
    }

    /// Request to append entry to the log
    pub fn request(&mut self, entry: Entry) -> anyhow::Result<()> {
        todo!()
    }

    pub fn step(&mut self, event: Event<()>, network: &mut Network) -> anyhow::Result<()> {
        match event {
            Event::EOF => todo!(),
            Event::RaftSignal(signal) => match signal {
                RaftSignal::Heartbeat => {
                    // If leader, send append entries
                    if !self.is_leader {
                        return Ok(());
                    }

                    eprintln!("{} sending heartbeats", network.node_id);
                    for (follower, &next_index) in self.next_index.iter() {
                        if follower == &network.node_id {
                            continue;
                        }

                        let entries = self.log[next_index..]
                            .iter()
                            .map(|e| e.clone().expect("Only the 0th element should be None"))
                            .collect();

                        network
                            .send(
                                follower.to_string(),
                                Body {
                                    msg_id: self.msg_id.next(),
                                    in_reply_to: None,
                                    payload: RaftPayload::AppendEntries {
                                        term: self.current_term,
                                        prev_log_index: next_index - 1,
                                        prev_log_term: self.log[next_index - 1]
                                            .as_ref()
                                            .map(|(_, term)| *term)
                                            .unwrap_or(0),
                                        entries,
                                    },
                                },
                            )
                            .context("Send append entries")?;
                    }
                }
                RaftSignal::Campaign => {
                    if self.is_leader {
                        return Ok(());
                    }

                    self.update_term(self.current_term + 1);

                    eprintln!(
                        "{} campaigning in new term {}",
                        network.node_id, self.current_term
                    );

                    // Broadcast RequestVote, vote for self
                    self.voted_for = Some(network.node_id.clone());
                    self.votes.insert(network.node_id.clone());
                    self.delay_election();

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
                                    payload: RaftPayload::<Entry>::RequestVote {
                                        term: self.current_term,
                                        last_log_index: self.last_log_index(),
                                        last_log_term: self.last_log_term(),
                                    },
                                },
                            )
                            .context("Send request vote")?;
                    }
                }
            },
            Event::RaftMessage(msg) => match msg.body.payload {
                RaftPayload::RequestVote {
                    term,
                    last_log_index,
                    last_log_term,
                } => {
                    //  Grant vote iff term >= currentTerm,
                    //  votedFor is null or candidateId, and
                    //  candidate’s log is at least as up-to-date as
                    //  receiver’s log
                    if term > self.current_term {
                        self.update_term(term);
                    }

                    let granted = term >= self.current_term
                        && !self.voted_for.as_ref().is_some_and(|v| v != &msg.src)
                        && (last_log_term, last_log_index)
                            >= (self.last_log_term(), self.last_log_index());

                    if granted {
                        self.update_term(term);

                        self.voted_for = Some(msg.src.clone());
                        self.delay_election();

                        eprintln!("{} voted for {} in term {term}", network.node_id, msg.src);
                    }

                    network
                        .reply(
                            msg.src,
                            self.msg_id.next(),
                            msg.body.msg_id,
                            RaftPayload::<Entry>::RequestVoteOk {
                                term: self.current_term,
                                vote_granted: granted,
                            },
                        )
                        .context("Request vote OK reply")?;
                }
                RaftPayload::RequestVoteOk { term, vote_granted } => {
                    if term > self.current_term {
                        self.update_term(term);
                        return Ok(());
                    }

                    if vote_granted && term == self.current_term {
                        self.votes.insert(msg.src);
                    }

                    if self.votes.len() > network.all_nodes.len() / 2 && !self.is_leader {
                        eprintln!(
                            "{} has been elected as leader in term {term}",
                            network.node_id
                        );

                        // Send inital heartbeat as soon as elected
                        self.step(Event::RaftSignal(RaftSignal::Heartbeat), network)
                            .context("Heartbeat on elected")?;

                        self.is_leader = true;
                    }
                }
                RaftPayload::AppendEntries {
                    term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                } => {
                    if term >= self.current_term {
                        // Must be from a new leader
                        self.update_term(term);
                        self.voted_for = Some(msg.src.clone());
                    }

                    if term < self.current_term
                        || !self.voted_for.as_ref().is_some_and(|v| v == &msg.src)
                    {
                        return Ok(());
                    }

                    self.delay_election();
                    eprintln!("got heartbeat from {}", msg.src);
                }
            },

            Event::Message(_) | Event::Signal(_) => bail!("Unexpected event recieved: {event:?}"),
        }
        Ok(())
    }
}
