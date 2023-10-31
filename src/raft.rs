use std::{collections::HashMap, sync::Arc, time::Duration};

use rand::Rng;

use crate::*;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum RaftPayload<Entry> {
    RequestVote {
        /// candidate’s term
        term: u64,
        /// index of candidate’s last log entry
        last_log_index: usize,
        /// term of candidate’s last log entry
        last_log_term: usize,
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
        /// term of prevLogIndex entry
        prev_log_term: usize,
        entries: Vec<(usize, Entry)>,
    },
    /// Leader tells followers to commit up to and including `index`
    Commit { index: usize },
    /// Follower tells leader
    CommitOk { index: usize },
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum RaftSignal {
    Heartbeat,
    BecomeCandidate,
}

pub struct RaftService<Entry> {
    msg_id: IdCounter,
    /// latest term server has seen
    current_term: u64,
    /// log entries; each entry contains command for state machine, and term when entry was received by leader
    log: Vec<(Entry, u64)>,
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
    votes: Vec<String>,
    /// Whether or not to reset the election timeout
    /// (set to true only when leader sends an AppendEntries)
    reset_election_timeout: Arc<AtomicBool>,
}

impl<Entry> RaftService<Entry> {
    pub fn create(network: &mut Network, sender: impl SenderExt<RaftSignal>) -> Self {
        let mut rng = rand::thread_rng();

        // Between 150 and 300 as per Raft spec
        let election_timeout = Duration::from_millis(rng.gen_range(150..=300));
        let heartbeat_timeout = Duration::from_millis(150);

        let reset_election_timeout = Arc::new(AtomicBool::new(false));
        spawn_timer(
            RaftSignal::BecomeCandidate,
            sender.clone(),
            election_timeout,
            Some(reset_election_timeout.clone()),
        );

        spawn_timer(RaftSignal::Heartbeat, sender, heartbeat_timeout, None);

        Self {
            msg_id: IdCounter::new(),
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            current_term: 0,
            voted_for: None,
            next_index: network.all_nodes.iter().cloned().map(|n| (n, 1)).collect(),
            match_index: network.all_nodes.iter().cloned().map(|n| (n, 0)).collect(),
            votes: Vec::new(),
            reset_election_timeout,
        }
    }

    fn is_leader(&self) -> bool {
        // self.votes.len() > (self.n_peers / 2)
        todo!()
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
                }
                RaftSignal::BecomeCandidate => {
                    // Broadcast RequestVote
                }
            },
            Event::RaftMessage(msg) => match msg.body.payload {
                RaftPayload::AppendEntries {
                    term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                } => {}
                RaftPayload::RequestVote {
                    term,
                    last_log_index,
                    last_log_term,
                } => todo!(),
                RaftPayload::RequestVoteOk { term, vote_granted } => todo!(),
                RaftPayload::Commit { index } => todo!(),
                RaftPayload::CommitOk { index } => todo!(),
            },

            Event::Message(_) | Event::Signal(_) => bail!("Unexpected event recieved: {event:?}"),
        }
        Ok(())
    }
}
