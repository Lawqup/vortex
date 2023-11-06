use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
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
        leader_commit: usize,
    },
    AppendEntriesOk {
        term: u64,
        success: bool,
        /// Last index that was applied in the AppendEntries
        applied_up_to: usize,
    },
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum RaftSignal {
    Heartbeat,
    Campaign,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RaftEvent<Entry: Clone> {
    RaftMessage(Message<RaftPayload<Entry>>),
    RaftSignal(RaftSignal),
    CommitedEntry(Entry),
}

pub struct RaftService<Entry: Clone> {
    msg_id: IdCounter,
    /// latest term server has seen
    current_term: u64,
    /// log entries; each entry contains command for state machine, and term when entry was received by leader
    log: Vec<Option<(Entry, u64)>>,
    /// index of highest log entry known to be committed
    commit_index: usize,
    /// Node that received our vote in current term
    voted_for: Option<String>,
    /// for each server, index of the next log entry to send to that server
    next_index: HashMap<String, usize>,
    /// for each server, index of highest log entry known to be replicated on server
    match_index: HashMap<String, usize>,
    /// Votes recieved from servers
    votes: HashSet<String>,
    /// Whether or not to reset the election timeout
    /// (set to true only when leader sends an AppendEntries)
    reset_election_timer: Arc<AtomicBool>,
    /// Sends commited log entries to the client node
    log_sender: Box<dyn SenderExt<Entry>>,
}

impl<Entry: Clone + Serialize + fmt::Debug + 'static> RaftService<Entry> {
    pub fn create(sender: impl SenderExt<RaftEvent<Entry>> + Clone) -> Self {
        // Between 150 and 300 as per Raft spec
        let election_timeout = Duration::from_millis(rand::thread_rng().gen_range(150..=300));
        let heartbeat_timeout = Duration::from_millis(150);

        let reset_election_timer = Arc::new(AtomicBool::new(false));
        let sender_clone = sender.clone();
        spawn_timer(
            Box::new(move || {
                let _ = sender_clone.send(RaftEvent::RaftSignal(RaftSignal::Campaign));
                Ok(())
            }),
            election_timeout,
            Some(reset_election_timer.clone()),
        );

        let sender_clone = sender.clone();
        spawn_timer(
            Box::new(move || {
                let _ = sender_clone.send(RaftEvent::RaftSignal(RaftSignal::Heartbeat));
                Ok(())
            }),
            heartbeat_timeout,
            None,
        );

        Self {
            msg_id: IdCounter::new(),
            log: vec![None],
            commit_index: 0,
            current_term: 0,
            voted_for: None,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            votes: HashSet::new(),
            reset_election_timer,
            log_sender: Box::new(sender.map_input(RaftEvent::CommitedEntry)),
        }
    }

    fn is_leader(&self) -> bool {
        !self.next_index.is_empty()
    }

    fn last_log_index(&self) -> usize {
        self.log.len() - 1
    }

    fn log_term_at(&self, idx: usize) -> u64 {
        self.log[idx].as_ref().map(|(_, term)| *term).unwrap_or(0)
    }

    fn delay_election(&self) {
        self.reset_election_timer.store(true, Ordering::Relaxed);
    }

    fn update_term(&mut self, term: u64) {
        self.current_term = term;
        self.votes.clear();
        self.next_index.clear();
        self.match_index.clear();
        // Voted for no one in this new term
        self.voted_for = None;
    }

    fn send_append_entries(&mut self, dest: String, network: &mut Network) -> anyhow::Result<()> {
        let next_index = self.next_index[&dest];
        let entries = self.log[next_index..]
            .iter()
            .map(|e| e.clone().expect("Only the 0th element should be None"))
            .collect();

        network
            .send(
                dest,
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
                        leader_commit: self.commit_index,
                    },
                },
            )
            .context("Send append entries")
    }

    /// Request to append entry to the log
    pub fn request(&mut self, entry: Entry, node_id: &str) -> anyhow::Result<()> {
        if !self.is_leader() {
            return Ok(());
        }

        self.log.push(Some((entry, self.current_term)));
        *self
            .match_index
            .get_mut(node_id)
            .expect("Leader should have itself in match_index") += 1;

        Ok(())
    }

    fn commit(&mut self, new_idx: usize) {
        let prev = self.commit_index;
        self.commit_index = new_idx;
        for i in prev + 1..=self.commit_index {
            if let Some((e, _)) = self.log[i].clone() {
                let _ = self.log_sender.send(e);
            }
        }

        eprintln!("Commited entries {}-{}", prev + 1, self.commit_index);
    }

    pub fn step(&mut self, event: RaftEvent<Entry>, network: &mut Network) -> anyhow::Result<()> {
        match event {
            RaftEvent::RaftSignal(signal) => match signal {
                RaftSignal::Heartbeat => {
                    // If leader, send append entries
                    if !self.is_leader() {
                        return Ok(());
                    }

                    eprintln!("{} sending heartbeats", network.node_id);
                    for follower in network.all_nodes.clone() {
                        // TODO: singelton case
                        if follower == network.node_id {
                            continue;
                        }

                        self.send_append_entries(follower, network)
                            .context("Heartbeat send append entries")?;
                    }
                }
                RaftSignal::Campaign => {
                    if self.is_leader() {
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

                    // TODO: singelton case
                    // if network.all_nodes.len() == 1 {
                    //     self.next_index = network
                    //         .all_nodes
                    //         .iter()
                    //         .cloned()
                    //         .map(|n| (n, self.last_log_index() + 1))
                    //         .collect();

                    //     self.match_index =
                    //         network.all_nodes.iter().cloned().map(|n| (n, 0)).collect();

                    //     return Ok(());
                    // }

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
                                        last_log_term: self.log_term_at(self.last_log_index()),
                                    },
                                },
                            )
                            .context("Send request vote")?;
                    }
                }
            },
            RaftEvent::RaftMessage(msg) => match msg.body.payload {
                RaftPayload::RequestVote {
                    term,
                    last_log_index,
                    last_log_term,
                } => {
                    eprintln!("Got vote request from {} at term {}", msg.src, term);
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
                            >= (
                                self.log_term_at(self.last_log_index()),
                                self.last_log_index(),
                            );

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

                    if self.votes.len() > network.all_nodes.len() / 2 && !self.is_leader() {
                        eprintln!(
                            "{} has been elected as leader in term {term}",
                            network.node_id
                        );

                        self.next_index = network
                            .all_nodes
                            .iter()
                            .cloned()
                            .map(|n| (n, self.last_log_index() + 1))
                            .collect();

                        self.match_index =
                            network.all_nodes.iter().cloned().map(|n| (n, 0)).collect();

                        // Send inital heartbeat as soon as elected
                        self.step(RaftEvent::RaftSignal(RaftSignal::Heartbeat), network)
                            .context("Heartbeat on elected")?;
                    }
                }
                RaftPayload::AppendEntries {
                    term,
                    prev_log_index,
                    prev_log_term,
                    mut entries,
                    leader_commit,
                } => {
                    if term >= self.current_term {
                        // Must be from a new leader
                        self.update_term(term);
                        self.voted_for = Some(msg.src.clone());
                        self.delay_election();
                    }

                    if term < self.current_term
                        || self.last_log_index() < prev_log_index
                        || self.log_term_at(prev_log_index) != prev_log_term
                    {
                        network
                            .reply(
                                msg.src,
                                self.msg_id.next(),
                                msg.body.msg_id,
                                RaftPayload::<Entry>::AppendEntriesOk {
                                    term: self.current_term,
                                    success: false,
                                    applied_up_to: self.last_log_index(),
                                },
                            )
                            .context("Append entries rejection")?;
                        return Ok(());
                    }

                    eprintln!("got appendEntries from {}:{:?}", msg.src, &entries);

                    for i in prev_log_index + 1
                        ..std::cmp::min(prev_log_index + 1 + entries.len(), self.log.len())
                    {
                        if self.log_term_at(i) != entries[0].1 {
                            // Conflicting entries
                            // so remove everything from this point on
                            self.log.drain(i..);
                            break;
                        }
                        entries.remove(0);
                    }

                    self.log.extend(entries.into_iter().map(Some));

                    let new_idx = std::cmp::max(
                        self.commit_index,
                        std::cmp::min(leader_commit, self.last_log_index()),
                    );

                    self.commit(new_idx);

                    network
                        .reply(
                            msg.src,
                            self.msg_id.next(),
                            msg.body.msg_id,
                            RaftPayload::<Entry>::AppendEntriesOk {
                                term: self.current_term,
                                success: true,
                                applied_up_to: self.last_log_index(),
                            },
                        )
                        .context("Append entries acceptance")?;
                }
                RaftPayload::AppendEntriesOk {
                    term,
                    success,
                    applied_up_to,
                } => {
                    if term > self.current_term {
                        self.update_term(term);
                        return Ok(());
                    }

                    // Should never have term < current_term
                    // Since follower updated their term to leader's
                    // On AppendEntries, thus this message is too delayed
                    if term < self.current_term {
                        return Ok(());
                    }

                    *self
                        .next_index
                        .get_mut(&msg.src)
                        .expect("Leader should have all nodes in next_index") = applied_up_to + 1;

                    *self
                        .match_index
                        .get_mut(&msg.src)
                        .expect("Leader should have all nodes in match_index") = applied_up_to;

                    if success {
                        let mut counts = HashMap::new();

                        for &e in self.match_index.values() {
                            *counts.entry(e).or_insert(0) += 1
                        }

                        let half_nodes = network.all_nodes.len() / 2;

                        let new_idx = counts
                            .into_iter()
                            .filter(|(_, c)| c > &half_nodes)
                            .map(|(v, _)| v)
                            .max()
                            .unwrap_or(0);

                        self.commit(new_idx);
                    } else {
                        self.send_append_entries(msg.src, network)
                            .context("Append entries retry")?;
                    }
                }
            },
            RaftEvent::CommitedEntry(_) => bail!("Commited entry should be handled by Raft client"),
        }
        Ok(())
    }
}
