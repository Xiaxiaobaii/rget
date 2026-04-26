use encoding::DecoderTrap::Strict;
use std::io::{BufRead, BufReader};
use tokio::{fs::File, io::AsyncReadExt};
use utils::{
    client::ControlClient,
    config::{CONFIG, LABELS, utf16_decode},
    task::{self},
};

use crate::utils::task::Task;

pub mod utils;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("version: 2.0.5\nBy: Xiaxiaobai");
    let client: ControlClient = ControlClient::new()?;
    if CONFIG.debug {
        println!("已启用debug");
    }
    if let Some(file) = CONFIG.file.clone() {
        let mut file = File::open(file).await?;
        let mut buf = vec![];
        file.read_to_end(&mut buf).await?;
        let text_buf = String::from_utf8(buf.clone())
            .ok()
            .or_else(|| utf16_decode(&buf, Strict).ok())
            .or_else(|| {
                LABELS.iter().find_map(|x| {
                    encoding::label::encoding_from_whatwg_label(x)
                        .unwrap()
                        .decode(&buf, Strict)
                        .ok()
                })
            })
            .expect("file encoding is Unsupport");
        let reader = BufReader::new(text_buf.as_bytes());
        let mut task = Task::create_task();

        for line in reader.lines() {
            if CONFIG.debug {
                println!("{:?}", line);
            }
            let line = line?;
            if line.is_empty() {
                continue;
            }
            task.reset_url(line);
            if let Err(err) = task.download(&client).await {
                eprintln!("download err: {err}")
            }
        }
    } else {
        let mut tas = task::Task::create_task();
        if CONFIG.debug {
            println!("{}", tas.url);
        }
        if let Err(err) = tas.download(&client).await {
            eprintln!("task download err: {err}")
        };
    }
    Ok(())
}
