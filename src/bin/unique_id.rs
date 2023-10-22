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

        let body = Body {
            msg_id: Some(self.msg_id),
            in_reply_to: input.body.msg_id,
            payload: GeneratePayload::GenerateOk {
                uuid: Ulid::new().to_string(),
            },
        };

        self.msg_id += 1;

        output.reply(input, body).context("Generate reply")?;
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let _ = initialize().context("Initialize node")?;

    let generate = GenerateService { msg_id: 0 };

    generate.run().context("Run generate service")
}
