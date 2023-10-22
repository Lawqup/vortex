use vortex::*;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum EchoPayload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct EchoService {
    msg_id: u64,
}

impl Service<EchoPayload> for EchoService {
    fn step(&mut self, input: Message<EchoPayload>, output: &mut Output) -> anyhow::Result<()> {
        let echo = match &input.body.payload {
            EchoPayload::Echo { echo } => echo,
            _ => return Ok(()),
        };

        output
            .reply(
                input.src,
                Some(&mut self.msg_id),
                input.body.msg_id,
                EchoPayload::EchoOk { echo: echo.clone() },
            )
            .context("Echo reply")?;
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let init = initialize().context("Initialize node")?;

    let echo = EchoService { msg_id: 0 };

    echo.run(init).context("Run echo service")
}
