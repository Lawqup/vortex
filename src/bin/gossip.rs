use anyhow;
use vortex::*;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum BroadcastPayload {
    Generate,
    GenerateOk {
        #[serde(rename = "id")]
        uuid: String,
    },
}
struct BroadcastService {
    msg_id: u64,
}

impl Service<BroadcastPayload> for BroadcastService {
    fn step(
        &mut self,
        input: Message<BroadcastPayload>,
        output: &mut Output,
    ) -> anyhow::Result<()> {
        todo!()
    }
}

fn main() -> anyhow::Result<()> {
    let _ = initialize().context("Initialize node")?;

    let generate = BroadcastService { msg_id: 0 };

    generate.run().context("Run generate service")
}
