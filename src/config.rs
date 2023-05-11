use std::path::{Path};

use anyhow::{Result, anyhow};
use serde::Deserialize;

pub const CONFIG_PATH: &str = "/etc/edgebit/cluster-agent-config.yaml";

const DEFAULT_LOG_LEVEL: &str = "info";

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Inner {
    edgebit_id: Option<String>,

    edgebit_url: Option<String>,

    log_level: Option<String>,

    hostname: Option<String>,
}

// TODO: probably worth using Figment or similar to unify yaml and env vars
pub struct Config {
    inner: Inner,
}

impl Config {
    pub fn load<P: AsRef<Path>>(path: P, hostname: Option<String>) -> Result<Self> {
        let mut inner: Inner = match std::fs::File::open(path.as_ref()) {
            Ok(file) => serde_yaml::from_reader(file)?,
            Err(err) => {
                if err.kind() != std::io::ErrorKind::NotFound {
                    // Don't bail since the config can also be provided via env vars.
                    // Do print a warning.
                    eprintln!("Could not open config file at {}, {err}", path.as_ref().display());
                }
                Inner::default()
            }
        };

        inner.hostname = hostname;

        let me = Self{
            inner,
        };

        // check that the config items are there
        me.try_edgebit_id()?;
        me.try_edgebit_url()?;

        Ok(me)
    }

    pub fn edgebit_id(&self) -> String {
        self.try_edgebit_id().unwrap()
    }

    fn try_edgebit_id(&self) -> Result<String> {
        if let Ok(id) = std::env::var("EDGEBIT_ID") {
            Ok(id)
        } else {
            self.inner
                .edgebit_id
                .clone()
                .ok_or(anyhow!("$EDGEBIT_ID not set and .edgebit_id missing in config file"))
        }
    }

    pub fn edgebit_url(&self) -> String {
        self.try_edgebit_url().unwrap()
    }

    fn try_edgebit_url(&self) -> Result<String> {
        if let Ok(id) = std::env::var("EDGEBIT_URL") {
            Ok(id)
        } else {
            self.inner
                .edgebit_url
                .clone()
                .ok_or(anyhow!("$EDGEBIT_URL not set and .edgebit_url missing in config file"))
        }
    }

    pub fn log_level(&self) -> String {
        if let Ok(level) = std::env::var("EDGEBIT_LOG_LEVEL") {
            level
        } else {
            self.inner.log_level
                .clone()
                .unwrap_or_else(|| DEFAULT_LOG_LEVEL.to_string())
        }
    }

    pub fn hostname(&self) -> String {
        self.inner.hostname
            .clone()
            .or_else(|| std::env::var("EDGEBIT_HOSTNAME").ok())
            .unwrap_or_else(|| {
                gethostname::gethostname()
                    .to_string_lossy()
                    .into_owned()
            })
    }
}