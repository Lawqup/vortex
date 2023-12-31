use ulid::Ulid;
use vortex_raft::*;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum GeneratePayload {
    Generate,
    GenerateOk {
        #[serde(rename = "id")]
        uuid: String,
    },
}

struct GenerateService {
    msg_id: IdCounter,
}

impl Service<GeneratePayload> for GenerateService {
    fn create(
        _network: &mut Network,
        _sender: std::sync::mpsc::Sender<Event<GeneratePayload>>,
    ) -> Self {
        Self {
            msg_id: IdCounter::new(),
        }
    }
    fn step(&mut self, event: Event<GeneratePayload>, network: &mut Network) -> anyhow::Result<()> {
        let Event::Message(input) = event else {
            panic!("Echo should only recieve messages");
        };

        match &input.body.payload {
            GeneratePayload::Generate => {}
            _ => return Ok(()),
        };

        network
            .reply(
                input.src,
                self.msg_id.next(),
                input.body.msg_id,
                GeneratePayload::GenerateOk {
                    uuid: Ulid::new().to_string(),
                },
            )
            .context("Generate reply")?;
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    GenerateService::run().context("Run generate service")
}
