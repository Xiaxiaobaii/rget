use anyhow::Error;
use clap::Parser;
use crossfire::{AsyncRx, MTx};
use dashmap::DashMap;
use futures::StreamExt;
use indicatif::HumanBytes;
use rquest::{
    Client, StatusCode,
    header::{ACCEPT_RANGES, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_RANGE, HeaderMap},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    env::temp_dir,
    path::{Path, PathBuf},
    process::{self},
    sync::{Arc, atomic::AtomicU64},
    time::{Duration, Instant},
};
use std::{
    sync::{LazyLock, atomic::Ordering::Relaxed},
    thread,
};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
    task::JoinHandle,
};

use sha256::try_digest;
#[cfg(target_family = "unix")]
use tokio::signal::unix::SignalKind;

use crate::utils::{
    client::ControlClient,
    config::{CONFIG, RE_FILENAME},
    process::create_bar,
};

use super::{client, config::Config, process::Process};

pub static ATO: LazyLock<DashMap<u64, AtomicU64>> = LazyLock::new(DashMap::new);

pub static ETO: LazyLock<DashMap<u64, AtomicU64>> = LazyLock::new(DashMap::new);

pub static XTO: LazyLock<DashMap<u64, (AtomicU64, AtomicU64)>> = LazyLock::new(DashMap::new);

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Task {
    pub url: String,

    pub output: Option<String>,

    pub thread_count: u32,

    pub retry: u8,

    pub mode: HttpMode,

    pub file_name: Option<String>,

    pub delete_stat: bool,

    pub config_retry: bool,

    pub extend: Extend,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Seiki {
    server: HashMap<String, (u64, u64)>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Extend {
    seiki_enable: bool,
    seiki_server_list: HashMap<String, (u64, u64)>,
}

impl Extend {
    pub fn new() -> Extend {
        Extend {
            seiki_enable: false,
            seiki_server_list: HashMap::new(),
        }
    }
}

impl Default for Extend {
    fn default() -> Self {
        Self::new()
    }
}

pub enum DownloadState {
    Over(u64),
}

//id: (end, start)
pub type ProcessStatus = Arc<DashMap<u64, (u64, u64)>>;

#[derive(Serialize, Deserialize, PartialEq, Clone, Copy, Debug)]
pub enum HttpMode {
    GET,
    POST,
}

impl Task {
    pub fn filename(&self) -> anyhow::Result<String> {
        if let Some(name) = &self.file_name {
            return Ok(name.clone());
        }

        let urls = &self.url;
        if let Some(last) = urls
            .split('/')
            .next_back()
            .and_then(|i| i.split('?').next())
        {
            Ok(percent_encoding::percent_decode_str(last)
                .decode_utf8()?
                .to_string())
        } else {
            Err(Error::msg("url is bad"))
        }
    }

    pub fn save_path(&self) -> anyhow::Result<PathBuf> {
        match &self.output {
            Some(out) => {
                let output = Path::new(out);
                if output.is_dir() {
                    Ok(output.join(self.filename()?))
                } else {
                    Ok(output.to_path_buf())
                }
            }
            None => Ok(Path::new(&self.filename()?).to_path_buf()),
        }
    }

    pub async fn get_header(
        &mut self,
        headers: &HeaderMap,
    ) -> Result<(bool, Option<u64>), anyhow::Error> {
        let is_seiki = headers.contains_key("seiki-enable");
        if is_seiki {
            self.extend.seiki_enable = true;
        }
        let is_resumed = headers.contains_key(CONTENT_RANGE) || headers.contains_key(ACCEPT_RANGES);
        if let Some(length) = headers.get(CONTENT_DISPOSITION) {
            let length = String::from_utf8_lossy(length.as_bytes());

            RE_FILENAME.captures(&length).and_then(|stack| {
                if let Some(k) = stack.get(1) {
                    println!("链接服务器提供下载文件名: {}", k.as_str());
                    self.file_name = Some(k.as_str().to_string());
                };
                stack.get(1)
            });
        }

        let mut length: Option<u64> = None;
        if let Some(lengt) = headers.get(CONTENT_LENGTH) {
            let lengt: u64 = lengt.to_str()?.parse()?;
            length = Some(lengt);
        }
        Ok((is_resumed, length))
    }

    pub async fn location(
        &mut self,
        conclient: &ControlClient,
    ) -> Result<(bool, Option<u64>), anyhow::Error> {
        let mut header = conclient.header.clone();
        header.append("range", "bytes=0-".parse()?);
        let client = ControlClient::no_self_create_client(header)?;
        let mut con_try = self.retry;
        loop {
            let resq = client.get(&self.url).send().await?;
            let headers: &HeaderMap = resq.headers();
            client::debug_print(format!("{:?}", headers));
            let mut error = "";
            let message = format!("url code is {}", resq.status().as_str());
            match resq.status() {
                StatusCode::FOUND => {
                    if let Some(local) = resq.headers().get("Location") {
                        self.url = local.to_str()?.to_string();
                    } else if let Some(local) = resq.headers().get("location") {
                        self.url = local.to_str()?.to_string();
                    } else {
                        error = "url code is 302, but not found Location!";
                    }
                    println!("location redirect to {}", self.url);
                }
                StatusCode::OK => return self.get_header(headers).await,
                StatusCode::PARTIAL_CONTENT => {
                    return self.get_header(headers).await;
                }
                StatusCode::FORBIDDEN => error = "url code is 403!, please check your header!",
                StatusCode::NOT_FOUND => error = "url code is 404!, please check your url!",
                StatusCode::TOO_MANY_REQUESTS => {
                    error = "url code is 429!, please reduce requests!"
                }
                StatusCode::INTERNAL_SERVER_ERROR => error = "url code is 500?",
                _ => error = &message,
            }
            if !error.is_empty() {
                eprintln!("header err: {}", error);
                client::debug_print(format!("{:?}", CONFIG.generate_header()));
                if !self.config_retry || con_try == 0 {
                    process::exit(1)
                } else {
                    con_try -= 1;
                }
            } else if self.extend.seiki_enable {
                self.extend.seiki_server_list =
                    serde_json::from_slice::<Seiki>(&resq.bytes().await?)?.server;
            }
        }
    }

    pub async fn thread_download(
        &mut self,
        file_size: Option<u64>,
        mut header: HeaderMap,
    ) -> anyhow::Result<()> {
        if let Ok(file) =
            tokio::fs::File::open(temp_dir().join(json_fix_name(self.filename()?))).await
        {
            if self.delete_stat {
                println!("该文件允许断点续传，但您使用参数禁止了.");
            } else {
                match self.downloaded(&mut header, file).await {
                    Ok(_) => return Ok(()),
                    Err(err) => {
                        println!("断点续传错误! {err}\n转向正常下载....")
                    }
                }
            }
        }
        if CONFIG.debug {
            println!("文件大小: {:?}", file_size)
        }
        if let Some(file_size) = file_size {
            let block_size = file_size / self.thread_count as u64;

            for i in 0..self.thread_count {
                let start = if i == 0 { 0 } else { i as u64 * block_size + 1 };
                let end = if i == self.thread_count - 1 {
                    file_size - 1
                } else {
                    (i + 1) as u64 * block_size
                };
                ATO.insert(i as u64, AtomicU64::new(start));
                ETO.insert(i as u64, AtomicU64::new(end));
            }
            self.downloads(&mut header).await?;
        } else if CONFIG.force_thread {
            return Err(Error::msg(
                "强制使用多线程，但文件并未提供大小，无法进行多线程下载",
            ));
        } else {
            return Err(Error::msg("该地址支持多线程但不提供大小, 请指定单线程下载"));
        }
        Ok(())
    }

    pub async fn one_download(
        &self,
        file_size: Option<u64>,
        client: &Client,
    ) -> Result<u64, anyhow::Error> {
        let resq = client.get(&self.url).send().await?;
        let mut file = File::create(self.save_path()?).await?;
        if let Some(size) = file_size {
            let bar = create_bar(size);
            let mut stream = resq.bytes_stream();
            while let Some(block) = stream.next().await {
                let block = block?;
                let chunk_length = block.len() as u64;
                match file.write_all(&block).await {
                    Ok(_) => {
                        break;
                    }
                    Err(err) => {
                        println!("request timing err: {}", err);
                        thread::sleep(Duration::from_millis(500));
                    }
                }
                bar.inc(chunk_length);
            }
            file.flush().await?;
            bar.finish_with_message("download success!");
            Ok(0)
        } else {
            let bar = create_bar(107374182400);
            let mut stream = resq.bytes_stream();
            let mut content_length: u64 = 0;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                bar.inc(chunk.len() as u64);
                content_length += chunk.len() as u64;
                file.write_all(&chunk).await?;
            }
            Ok(content_length)
        }
    }

    pub fn create_process(&self) -> anyhow::Result<Process> {
        Ok(Process {
            file_path: self.save_path()?,
            max_retry: 15,
            retry: 0,
            mode: self.mode,
            url: self.url.to_string(),
        })
    }

    pub async fn downloaded(
        &self,
        header: &mut HeaderMap,
        mut file: tokio::fs::File,
    ) -> anyhow::Result<()> {
        println!("断点续传连接中....");
        let mut s = String::new();
        file.read_to_string(&mut s).await?;
        let sj: Sjson = serde_json::from_str(&s)?;
        let ato: DashMap<u64, u64> = serde_json::from_str(&sj.ato)?;
        let eto: DashMap<u64, u64> = serde_json::from_str(&sj.eto)?;
        if ato.is_empty() || ato.len() != eto.len() {
            return Err(anyhow::Error::msg("断点文件参数为0或不一致！"));
        }
        for a in ato.into_iter() {
            ATO.insert(a.0, AtomicU64::new(a.1));
        }
        for e in eto.into_iter() {
            ETO.insert(e.0, AtomicU64::new(e.1));
        }
        self.downloads(header).await?;

        Ok(())
    }

    pub async fn download(&mut self, client: &ControlClient) -> anyhow::Result<()> {
        let if_thread;
        let mut file_size;
        match self.location(client).await {
            Ok((thread, size)) => {
                if_thread = thread;
                file_size = size;
            }
            Err(e) => return Err(Error::msg(format!("location: {e}"))),
        }

        if let Some(name) = &CONFIG.output {
            self.file_name = Some(name.clone())
        }
        if let Some(file_name) = &self.file_name {
            self.file_name = Some(
                percent_encoding::percent_decode_str(file_name)
                    .decode_utf8()?
                    .to_string(),
            );
        }
        println!("文件名: {}", self.filename()?);

        if let Some(size) = file_size {
            println!("文件大小: {}", HumanBytes(size));
            client::debug_print(format!("file_size: {size}"));
        } else {
            println!("无法确定文件大小!");
        }

        let timer = Instant::now();
        if (if_thread && !CONFIG.one_download) || CONFIG.force_thread || self.extend.seiki_enable {
            println!("使用多线程下载");
            self.thread_download(file_size, client.header.clone())
                .await?;
        } else {
            println!("使用单线程下载");
            let temp_size = self.one_download(file_size, &client.client).await?;
            if file_size.is_none() {
                file_size = Some(temp_size)
            }
        }

        let _ = tokio::fs::remove_file(temp_dir().join(json_fix_name(self.filename()?))).await;
        let cost = timer.elapsed().as_millis() as f64;
        let cost = cost / 1000.0 + ((cost % 1000.0) / 1000.0);
        if let Some(size) = file_size {
            println!(
                "下载完成!\n总用时: {:.2}s\n均速: {:.2}Mb/s",
                cost,
                size as f64 / 1024.0 / 1024.0 / cost
            );
        } else {
            println!("下载完成!\n总用时: {:.2}s", cost);
        }
        if CONFIG.sha256 {
            let sha = try_digest(Path::new(&self.filename()?))?;
            println!("sha256校验值: {}", sha);
        }

        Ok(())
    }

    pub fn create_task() -> Task {
        let config = Config::parse();
        let mut mode = HttpMode::GET;
        if CONFIG.post {
            mode = HttpMode::POST;
        }
        Task {
            url: config.url.clone(),
            output: config.output.clone(),
            mode,
            thread_count: config.thread_size,
            retry: config.retry,
            file_name: None,
            delete_stat: config.delete_stat,
            config_retry: true,
            extend: Extend::default(),
        }
    }

    pub fn reset_url(&mut self, url: String) {
        self.url = url;
        self.output = None;
        self.retry = 5;
        self.file_name = None;
    }

    pub async fn downloads(&self, header: &mut HeaderMap) -> anyhow::Result<()> {
        let mut joins: Vec<tokio::task::JoinHandle<()>> = vec![];

        let (down_tx, down_rx) = crossfire::mpsc::unbounded_async::<u64>();
        let one_bytes =
            ETO.iter().next().expect("断点备份文件有问题或链接返回数据出错").load(Relaxed) - ATO.iter().next().unwrap().load(Relaxed);
        let file = std::fs::OpenOptions::new()
            .truncate(false)
            .write(true)
            .create(true)
            .open(self.save_path()?)?;

        for call in ATO.iter() {
            let (k, _) = call.pair();
            let k = *k;
            let mut block = self.create_process()?;
            let header = header.clone();
            let down_tx = down_tx.clone();

            let mut file = file.try_clone()?;
            joins.push(tokio::spawn(async move {
                while block
                    .download(k, &header, &down_tx, &mut file)
                    .await
                    .is_err()
                {}
                drop(down_tx);
            }));
        }
        let mut _file = file.try_clone()?;
        self.downed_for(_file)?;
        if one_bytes < CONFIG.resumd_min_body * 1024 {
            for i in joins {
                let _ = i.await;
            }
            return Ok(());
        }
        let basic_id = self.thread_count as u64 + 1;

        let bound = ResumdBound {
            basic_id,
            one_bytes,
            header: header.clone(),
            joins,
            down_rx,
            down_tx,
            file,
        };
        let joins = self.resumd_thread(bound).await?;
        for i in joins {
            let _ = i.await;
        }
        Ok(())
    }

    async fn resumd_thread(
        &self,
        mut bound: ResumdBound,
    ) -> Result<Vec<JoinHandle<()>>, anyhow::Error> {
        loop {
            if let Ok(id) = bound.down_rx.recv().await {
                ATO.remove(&id);
                ETO.remove(&id);
                if ATO.is_empty() {
                    break;
                }
                //statu中进度最慢的线程内end与next值
                let mut max_pos = (0, 0);
                //进度最慢线程的id
                let mut max_id = 0;
                for i in ATO.iter() {
                    let (id, next) = i.pair();
                    let next = next.load(Relaxed);
                    let end = ETO.get(id).unwrap().load(Relaxed);
                    if next >= end {
                        continue;
                    }
                    if max_pos.0 - max_pos.1 < end - next {
                        max_id = id.to_owned();
                        max_pos = (end, next).to_owned();
                    }
                }
                if max_pos.0 == 0 {
                    continue;
                }
                //最慢线程理想剩余数据大小/2
                let space_over = (max_pos.0 - max_pos.1) / 2;
                if (space_over * 2) as f32 <= bound.one_bytes as f32 * CONFIG.resumd_point {
                    continue;
                }
                //重新分配给旧线程的end_pos
                let old_pos = space_over + max_pos.1;

                let new_id = bound.basic_id + max_id;
                ATO.insert(new_id, AtomicU64::new(old_pos + 1));
                ETO.insert(new_id, AtomicU64::new(max_pos.0));

                let mut block = self.create_process()?;

                let header = bound.header.clone();
                let down_tx = bound.down_tx.clone();
                let mut file = bound.file.try_clone()?;

                bound.joins.push(tokio::spawn(async move {
                    while block
                        .download(new_id, &header, &down_tx, &mut file)
                        .await
                        .is_err()
                    {}
                    drop(down_tx);
                }));
            }
        }
        Ok(bound.joins)
    }

    fn downed_for(&self, file: std::fs::File) -> anyhow::Result<()> {
        let file_name = self.filename()?;

        tokio::spawn(async move {
            stop_signal().await;
            let _ = fs::remove_file(temp_dir().join(json_fix_name(file_name.clone()))).await;
            let over = OpenOptions::new()
                .truncate(false)
                .write(true)
                .create(true)
                .open(temp_dir().join(json_fix_name(file_name)))
                .await;
            let ato: DashMap<u64, u64> = DashMap::new();
            let eto = DashMap::new();
            for i in ATO.iter().enumerate() {
                let i = Box::leak(Box::new(i));
                ato.insert(*i.1.key(), i.1.load(Relaxed));
            }
            for i in ETO.iter().enumerate() {
                let i = Box::leak(Box::new(i));
                eto.insert(i.1.key(), i.1.load(Relaxed));
            }
            let ff = Sjson {
                ato: serde_json::to_string(&ato).unwrap(),
                eto: serde_json::to_string(&eto).unwrap(),
            };
            if let Ok(f) = over {
                if let Err(err) = ff.serialize(&mut serde_json::Serializer::new(f.into_std().await))
                {
                    println!("save err: {err}");
                } else {
                    println!("已保存断点文件数据， 再次下载会自动尝试续传!");
                    process::exit(0);
                }
            }
            println!(
                "请保存下面输出的json文件, 这将可帮助手动复原断点文件数据:\n{:?}",
                serde_json::to_string(&ff).unwrap()
            );
            ETO.iter().for_each(|id| {
                id.store(0, std::sync::atomic::Ordering::SeqCst);
            });
            file.sync_all().unwrap();
            process::exit(1);
        });
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Sjson {
    ato: String,
    eto: String,
}

#[cfg(target_family = "unix")]
pub async fn stop_signal() {
    let mut sigint = tokio::signal::unix::signal(SignalKind::interrupt()).unwrap();
    let mut sighup = tokio::signal::unix::signal(SignalKind::hangup()).unwrap();
    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate()).unwrap();
    tokio::select! {
        _ = sigint.recv() => {}
        _ = sighup.recv() => {}
        _ = sigterm.recv() => {}
    }
}

#[cfg(target_family = "windows")]
pub async fn stop_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("core error, this is not we error.");
}

pub fn json_fix_name(file_name: String) -> String {
    let split_coll: Vec<&str> = file_name.split(".").collect();
    let q_name = split_coll.join("");

    q_name + ".json"
}

struct ResumdBound {
    basic_id: u64,
    one_bytes: u64,
    header: HeaderMap,
    joins: Vec<JoinHandle<()>>,
    down_tx: MTx<u64>,
    down_rx: AsyncRx<u64>,
    file: std::fs::File,
}
