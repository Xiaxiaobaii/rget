use std::{borrow::Cow, collections::HashMap, sync::LazyLock};

use clap::Parser;
use encoding::{
    all::{UTF_16BE, UTF_16LE},
    DecoderTrap, Encoding,
};
use indicatif::MultiProgress;
use regex::Regex;
use rquest::{
    header::{HeaderMap, HeaderName, ACCEPT, ACCEPT_ENCODING, ACCEPT_LANGUAGE, COOKIE, USER_AGENT},
};
use rquest_util::Emulation;
use serde::{Deserialize, Serialize};

pub static MP: LazyLock<MultiProgress> = LazyLock::new(MultiProgress::new);

pub static CONFIG: LazyLock<Config> = LazyLock::new(Config::parse);

pub static RE_FILENAME: LazyLock<Regex> = LazyLock::new(|| {
    regex::Regex::new("filename=\"?([^;\"]*)\"?;?").unwrap()
});

pub static EMULATION: LazyLock<HashMap<String, Emulation>> = LazyLock::new(|| {
    let mut map = HashMap::with_capacity(10);
    map.insert(String::from("edge131"), Emulation::Edge131);
    map.insert(String::from("edge101"), Emulation::Edge101);
    map.insert(String::from("edge122"), Emulation::Edge122);
    map.insert(String::from("edge127"), Emulation::Edge127);
    map.insert(String::from("chrome126"), Emulation::Chrome126);
    map.insert(String::from("chrome127"), Emulation::Chrome127);
    map.insert(String::from("chrome128"), Emulation::Chrome128);
    map.insert(String::from("chrome129"), Emulation::Chrome129);
    map.insert(String::from("chrome130"), Emulation::Chrome130);
    map.insert(String::from("chrome131"), Emulation::Chrome131);
    map
});

#[derive(Serialize, Deserialize, Parser, Clone, Debug)]
pub struct Config {
    ///下载链接，指定-f时必填但不使用
    pub url: String,

    ///指定输出文件名
    #[arg(short, long)]
    pub output: Option<String>,

    ///请求头，示例：“-H "Host: baidu.com"”
    #[arg(short('H'), long)]
    pub header: Option<String>,

    ///文件批量下载，将链接分行自动下载
    #[arg(short, long)]
    pub file: Option<String>,

    ///代理url
    #[arg(long)]
    pub proxy_url: Option<String>,

    /// 设置伪装的浏览器指纹，默认使用Edge131
    #[arg(short('I'), long)]
    pub emulation: Option<String>,

    /// 参数为none时，禁用代理，其他值则为自定义代理（将proxy_url的参数设置为代理地址，支持socks4(a)，socks5(h)，http，https）
    #[arg(short, long)]
    pub proxy_type: Option<String>,

    /// 代理使用用户名
    #[arg(long)]
    pub user_name: Option<String>,

    /// 代理使用密码
    #[arg(long)]
    pub password: Option<String>,

    /// Debug模式
    #[arg(long, default_value_t = false)]
    pub debug: bool,

    ///设置UA
    #[arg(short('A'), long)]
    pub user_agent: Option<String>,

    ///设置下载重试次数
    #[arg(short, long, default_value_t = 3)]
    pub retry: u8,

    ///设置多线程下载使用线程数
    #[arg(short, long, default_value_t = 8)]
    pub thread_size: u32,

    ///禁用断点续传
    #[arg(short, long, default_value_t = false)]
    pub delete_stat: bool,

    ///指定单线程下载
    #[arg(short('O'), long, default_value_t = false)]
    pub one_download: bool,

    #[arg(short('C'), long)]
    pub cookie: Option<String>,

    ///强制使用多线程（适用于部分可用多线程但返回中未指示的文件）
    #[arg(long, default_value_t = false)]
    pub force_thread: bool,

    ///（仅使用文件批量下载时有效）在文件解码时决定UTF16使用大小端，默认小端，使用选项后改为大端
    #[arg(long, default_value_t = false)]
    pub utf_be: bool,

    /// （仅使用复用时有效）下载完后触发复用新进程的要求临界点，默认：0.4
    #[arg(long, default_value_t = 0.4)]
    pub resumd_point: f32,

    /// 触发复用的最小文件大小，默认200，单位KB
    #[arg(long, default_value_t = 200)]
    pub resumd_min_body: u64,

    /// 使用POST请求数据，而非GET
    #[arg(long, default_value_t = false)]
    pub post: bool,

    /// 下载完后输出sha256校验值
    #[arg(long, default_value_t = false)]
    pub sha256: bool,
}

pub const LABELS: [&str; 15] = [
    "gbk",
    "big5",
    "gb18030",
    "866",
    "euc-jp",
    "euc-kr",
    "koi",
    "iso88592",
    "iso88593",
    "iso88594",
    "iso88595",
    "iso88596",
    "iso88597",
    "iso88598",
    "iso885910",
];

pub fn utf16_decode(input: &[u8], trap: DecoderTrap) -> Result<String, Cow<'static, str>> {
    if CONFIG.utf_be {
        UTF_16BE.decode(input, trap)
    } else {
        UTF_16LE.decode(input, trap)
    }
}

impl Config {
    pub fn generate_header(&self) -> anyhow::Result<HeaderMap> {
        let mut header = HeaderMap::new();
        header.insert("priority", "u=0, i".parse()?);
        header.insert(ACCEPT, "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7".parse()?);
        header.insert(ACCEPT_ENCODING, "gzip, deflate, br, zstd".parse()?);
        header.insert(
            ACCEPT_LANGUAGE,
            "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6".parse()?,
        );
        //header.insert("upgrade-insecure-requests", "1".parse()?);
        if let Some(agent) = &self.user_agent {
            header.insert(USER_AGENT, agent.parse()?);
        } else {
            header.insert(USER_AGENT, "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0".parse()?);
        }
        if let Some(agent) = &self.cookie {
            header.insert(COOKIE, agent.parse()?);
        }

        if let Some(text) = self.header.as_ref() {
            for line in text.lines() {
                let parts: Vec<&str> = line.split(":").collect();
                if parts.len() >= 2 {
                    let key = parts[0].trim().as_bytes();
                    let key = HeaderName::from_bytes(key)?;
                    let value = parts[1..].join(":").trim().to_owned();
                    header.insert(key, value.parse()?);
                }
            }
        }
        Ok(header)
    }
}