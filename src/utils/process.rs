use crossfire::MTx;
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use rquest::{
    Response,
    header::{HeaderMap, RANGE},
};
use std::fs::File;
use std::{io::Write, path::PathBuf};
use std::{process, sync::atomic::Ordering::Relaxed};

#[cfg(unix)]
use std::os::unix::fs::FileExt;

#[cfg(windows)]
use std::os::windows::fs::FileExt;

use crate::utils::{
    config::MP,
    task::{ATO, ETO},
};

use super::{client, task::HttpMode};

pub struct Process {
    pub file_path: PathBuf,
    pub max_retry: u8,
    pub retry: u8,
    pub mode: HttpMode,
    pub url: String,
}

pub fn create_bar(size: u64) -> ProgressBar {
    if let Ok(mut style) = ProgressStyle::with_template(
        "{spinner:.green} [{elapsed_precise}] [{bar:50.cyan/blue}] {msg:<13} {decimal_bytes_per_sec} {bytes:>8}/{total_bytes} ({eta})",
    ) {
        style = style.progress_chars("==>");
        let bar = ProgressBar::new(size);
        bar.set_style(style);
        bar
    } else {
        process::exit(888)
    }
}

impl Process {
    pub async fn download(
        &mut self,
        id: u64,
        header: &HeaderMap,
        down_tx: &MTx<u64>,
        file: &mut File,
    ) -> anyhow::Result<()> {
        //start_pos: 线程创建时被分配的初始下载位置
        //end_pos: 被分配的结束下载位置
        //next_pos: 线程在整个文件线上的进度
        //函数id在执行前均硬性保证插入到ATO/ETO中.
        let next_ato = ATO.get(&id).unwrap();
        let end_eto = ETO.get(&id).unwrap();
        let client = client::ControlClient::no_self_create_client(header.clone())?;

        loop {
            let mut err = false;
            let mut back_eto = end_eto.load(Relaxed);
            let range = format!("bytes={}-{}", next_ato.load(Relaxed), back_eto);
            let resq: Result<Response, rquest::Error> = if self.mode == HttpMode::GET {
                client.get(&self.url).header(RANGE, &range).send().await
            } else {
                client.post(&self.url).header(RANGE, &range).send().await
            };

            match resq {
                Ok(resq) => {
                    
                    let mut next_pos = next_ato.load(Relaxed);
                    let bar = MP.add(create_bar(back_eto - next_pos));
                    let mut stream = resq.bytes_stream();
                    while let Some(block) = stream.next().await {
                        match block {
                            Ok(block) => {
                                #[cfg(unix)]
                                file.write_at(&block, next_pos)?;
                                #[cfg(windows)]
                                file.seek_write(&block, next_pos)?;
                                let end_pos = end_eto.load(Relaxed);
                                if end_pos == 0 {
                                    return Ok(());
                                } else if end_pos != back_eto {
                                    bar.set_length(end_pos - next_pos);
                                    back_eto = end_eto.load(Relaxed);
                                }
                                next_pos += block.len() as u64;
                                next_ato.store(next_pos, Relaxed);
                                if next_pos >= end_pos {
                                    break;
                                }
                                bar.inc(block.len() as u64);
                            }
                            Err(_) => {
                                err = true;
                                break;
                            }
                        }
                    }
                    file.flush()?;
                    bar.finish_and_clear();
                    if err {
                        continue;
                    }
                    down_tx.send(id)?;
                    break;
                }

                Err(err) => {
                    self.retry += 1;
                    if self.retry >= self.max_retry {
                        panic!("process download timer faild: {err}");
                    }
                }
            }
        }
        Ok(())
    }
}
