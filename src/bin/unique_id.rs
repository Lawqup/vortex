use ulid::Ulid;
use vortex::*;

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
    msg_id: u64,
}

impl Service<GeneratePayload> for GenerateService {
    fn step(&mut self, input: Message<GeneratePayload>, output: &mut Output) -> anyhow::Result<()> {
        match &input.body.payload {
            GeneratePayload::Generate => {}
            _ => return Ok(()),
        };

        output
            .reply(
                input.src,
                Some(&mut self.msg_id),
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
    let init = initialize().context("Initialize node")?;

    let generate = GenerateService { msg_id: 0 };

    generate.run(init).context("Run generate service")
}
