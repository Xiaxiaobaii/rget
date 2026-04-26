use std::time::Duration;

use crate::utils::config::EMULATION;

use super::config::CONFIG;
use anyhow::{Error, Ok};
use rquest::{Client, ClientBuilder, Proxy, header::HeaderMap, redirect::Policy};
use rquest_util::Emulation;

pub struct ControlClient {
    pub header: HeaderMap,
    pub client: Client,
}

pub fn debug_print(msg: String) {
    if CONFIG.debug {
        println!("[debug] msg: {msg}");
    }
}

impl ControlClient {
    pub fn new() -> anyhow::Result<ControlClient> {
        let header = CONFIG.generate_header()?;
        Ok(ControlClient {
            header: header.clone(),
            client: ControlClient::no_self_create_client(header)?,
        })
    }

    pub fn self_create_client(&self) -> anyhow::Result<Client> {
        Ok(ControlClient::create_client()?
            .default_headers(self.header.clone())
            .build()?)
    }

    pub fn create_client() -> anyhow::Result<ClientBuilder> {
        let mut client = Client::builder()
            .redirect(Policy::limited(10))
            .emulation(Emulation::Edge131)
            .connect_timeout(Duration::from_secs(30));
        if let Some(emulation_str) = &CONFIG.emulation {
            if emulation_str == "None" {
                println!("已禁用指纹伪装");
            } else if let Some(emulation) = EMULATION.get(emulation_str) {
                println!("使用指纹: {}", emulation_str);
                client = client.emulation(*emulation);
            } else {
                println!("伪装指纹不存在, 默认使用Edge131");
                client = client.emulation(Emulation::Edge131);
            }
        } else {
            client = client.emulation(Emulation::Edge131);
        }
        if let Some(ptype) = &CONFIG.proxy_type {
            debug_print(format!("proxy: {ptype}"));
            if ptype == "none" {
                client = client.no_proxy();
            } else if let Some(url) = &CONFIG.proxy_url {
                let mut proxy = Proxy::all(url)?;
                if let Some(name) = &CONFIG.user_name {
                    if let Some(password) = &CONFIG.password {
                        proxy = proxy.basic_auth(name, password);
                    } else {
                        eprintln!("have username but no password")
                    }
                }
                client = client.proxy(proxy);
            } else {
                return Err(Error::msg("no set proxy_url"));
            }
        }
        Ok(client)
    }

    pub fn no_self_create_client(header: HeaderMap) -> anyhow::Result<Client> {
        Ok(ControlClient::create_client()?
            .default_headers(header)
            .timeout(Duration::from_hours(48))
            .build()?)
    }
}
