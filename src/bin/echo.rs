use vortex::*;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum EchoPayload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct EchoService {
    msg_id: IdCounter,
}

impl Service<EchoPayload> for EchoService {
    fn create(_sender: std::sync::mpsc::Sender<Event<EchoPayload>>) -> Self {
        Self {
            msg_id: IdCounter::new(),
        }
    }
    fn step(&mut self, event: Event<EchoPayload>, network: &mut Network) -> anyhow::Result<()> {
        let Event::Message(input) = event else {
            panic!("Echo should only recieve messages");
        };

        let echo = match &input.body.payload {
            EchoPayload::Echo { echo } => echo,
            _ => return Ok(()),
        };

        network
            .reply(
                input.src,
                self.msg_id.next(),
                input.body.msg_id,
                EchoPayload::EchoOk { echo: echo.clone() },
            )
            .context("Echo reply")?;
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    EchoService::run().context("Run echo service")
}
