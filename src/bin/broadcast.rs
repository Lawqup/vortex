use anyhow;
use vortex::*;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum BroadcastPayload {
    Broadcast { message: u64 },
    BroadcastOk,
    Read,
    ReadOk { messages: Vec<u64> },
}
struct BroadcastService {
    msg_id: u64,
    store: Vec<u64>,
}

impl Service<BroadcastPayload> for BroadcastService {
    fn step(
        &mut self,
        input: Message<BroadcastPayload>,
        output: &mut Output,
    ) -> anyhow::Result<()> {
        match &input.body.payload {
            BroadcastPayload::Broadcast { message } => {
                self.store.push(*message);

                output
                    .reply(
                        input.src,
                        Some(self.msg_id),
                        input.body.msg_id,
                        BroadcastPayload::BroadcastOk,
                    )
                    .context("Broadcast reply")?;

                self.msg_id += 1;
            }
            BroadcastPayload::Read => {
                output
                    .reply(
                        input.src,
                        Some(self.msg_id),
                        input.body.msg_id,
                        BroadcastPayload::ReadOk {
                            messages: self.store.clone(),
                        },
                    )
                    .context("Read reply")?;

                self.msg_id += 1;
            }
            _ => {}
        }

        Ok(())
    }
}

/*
{
  "src": "c1",
  "dest": "n1",
  "body": {
    "type":     "init",
    "msg_id":   1,
    "node_id":  "n3",
    "node_ids": ["n1", "n2", "n3"]
  }
}



 */

fn main() -> anyhow::Result<()> {
    let output = initialize().context("Initialize node")?;

    let broadcast = BroadcastService {
        msg_id: 0,
        store: Vec::new(),
    };

    broadcast.run(output).context("Run generate service")
}
