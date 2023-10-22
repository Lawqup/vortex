use vortex::*;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum EchoPayload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct EchoService {
    id: u64,
}

impl Service<EchoPayload> for EchoService {
    fn step(&mut self, input: Message<EchoPayload>, output: &mut Output) -> anyhow::Result<()> {
        let echo = match &input.body.payload {
            EchoPayload::Echo { echo } => echo,
            _ => return Ok(()),
        };

        let body = Body {
            msg_id: Some(self.id),
            in_reply_to: input.body.msg_id,
            payload: EchoPayload::EchoOk { echo: echo.clone() },
        };

        self.id += 1;

        output.reply(input, body).context("Echo reply")?;
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let _ = initialize().context("Initialize node")?;

    let echo = EchoService { id: 0 };

    echo.run().context("Run echo service")
}
