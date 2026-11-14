//! The node's TOML configuration file (gap O1). Every key is optional;
//! unknown keys are errors, so typos fail loudly.
//!
//! ```toml
//! ingress = "0.0.0.0:7000"
//! feed = "239.1.1.1:7001"
//! interface = "10.0.0.5"
//! recovery = "0.0.0.0:7002"
//! stream_id = 1
//! spin = true
//! cores = [2, 3, 4, 5]          # ingress, core, egress, recovery
//!
//! [limits]
//! mtu = 1472
//! max_payload = 1024
//! history = 65536               # messages kept for recovery (power of two)
//! max_clients = 4096
//! outcome_ring = 64             # clients' windows must not exceed this
//! max_keys = 65536
//! evict_idle_ms = 60000
//!
//! [mold]                        # optional MoldUDP64 adapter
//! session = "KC20261114"
//! feed = "239.1.1.2:7004"
//! rerequest = "0.0.0.0:7003"
//! ```
use crate::{Idle, MoldConfig, NodeConfig};
use serde::Deserialize;
use std::net::{Ipv4Addr, SocketAddrV4};

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    pub ingress: Option<SocketAddrV4>,
    pub feed: Option<SocketAddrV4>,
    pub recovery: Option<SocketAddrV4>,
    pub interface: Option<Ipv4Addr>,
    pub stream_id: Option<u64>,
    pub spin: Option<bool>,
    pub cores: Option<Vec<usize>>,
    /// Shared-memory metrics file (default /dev/shm/krusty-<stream_id>.stats).
    pub metrics: Option<std::path::PathBuf>,
    pub limits: Option<Limits>,
    pub mold: Option<Mold>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub mtu: Option<usize>,
    pub max_payload: Option<usize>,
    pub history: Option<usize>,
    pub max_clients: Option<usize>,
    pub outcome_ring: Option<usize>,
    pub max_keys: Option<usize>,
    pub evict_idle_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mold {
    pub session: String,
    pub feed: SocketAddrV4,
    pub rerequest: Option<SocketAddrV4>,
}

impl FileConfig {
    pub fn parse(text: &str) -> Result<FileConfig, String> {
        toml::from_str(text).map_err(|e| e.to_string())
    }

    /// Applies the file on top of `cfg`; checks values the node relies on.
    pub fn apply(&self, cfg: &mut NodeConfig) -> Result<(), String> {
        if let Some(a) = self.ingress {
            cfg.ingress = a;
        }
        if let Some(a) = self.feed {
            cfg.feed = a;
        }
        if let Some(a) = self.recovery {
            cfg.recovery = a;
        }
        if let Some(i) = self.interface {
            cfg.interface = i;
        }
        if let Some(s) = self.stream_id {
            cfg.stream_id = s;
        }
        if let Some(p) = &self.metrics {
            cfg.metrics_path = Some(p.clone());
        }
        if self.spin == Some(true) {
            cfg.idle = Idle::Spin;
        }
        if let Some(cores) = &self.cores {
            if cores.len() > 4 {
                return Err("cores: at most 4 (ingress, core, egress, recovery)".into());
            }
            for (i, c) in cores.iter().enumerate() {
                cfg.cores[i] = Some(*c);
            }
        }
        if let Some(l) = &self.limits {
            if let Some(v) = l.mtu {
                cfg.mtu = v;
            }
            if let Some(v) = l.max_payload {
                cfg.max_payload = v;
            }
            if let Some(v) = l.history {
                if !v.is_power_of_two() {
                    return Err("limits.history must be a power of two".into());
                }
                cfg.history = v;
            }
            if let Some(v) = l.max_clients {
                cfg.core.max_clients = v;
            }
            if let Some(v) = l.outcome_ring {
                cfg.core.outcome_ring = v;
            }
            if let Some(v) = l.max_keys {
                cfg.core.max_keys = v;
            }
            if let Some(v) = l.evict_idle_ms {
                cfg.core.evict_idle_ns = v.saturating_mul(1_000_000);
            }
        }
        if let Some(m) = &self.mold {
            let session = krusty_mold::Session::new(&m.session)
                .ok_or("mold.session: 1-10 printable ASCII characters")?;
            cfg.mold = Some(MoldConfig {
                session,
                feed: m.feed,
                rerequest: m
                    .rerequest
                    .unwrap_or_else(|| "0.0.0.0:7003".parse().unwrap()),
            });
        }
        cfg.validate()
    }
}

impl NodeConfig {
    /// Checks the constraints `start` relies on.
    pub fn validate(&self) -> Result<(), String> {
        use krusty_wire::stream::{DataFrameWriter, HEADER_LEN};
        if HEADER_LEN + DataFrameWriter::block_len(self.max_payload) > self.mtu {
            return Err(format!(
                "max_payload {} does not fit one message per {}-byte frame",
                self.max_payload, self.mtu
            ));
        }
        if self.feed.ip().is_multicast() && self.interface.is_unspecified() {
            return Err("a multicast feed needs `interface` (the default route may be a slow link; docs/BASELINE.md)".into());
        }
        if let Some(m) = self.mold
            && m.feed.ip().is_multicast()
            && self.interface.is_unspecified()
        {
            return Err("a multicast Mold feed needs `interface`".into());
        }
        if self.core.outcome_ring == 0 || self.core.max_clients == 0 {
            return Err("limits.outcome_ring and limits.max_clients must be at least 1".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> NodeConfig {
        NodeConfig::new(
            "0.0.0.0:7000".parse().unwrap(),
            "127.0.0.1:7001".parse().unwrap(),
            "0.0.0.0:7002".parse().unwrap(),
        )
    }

    #[test]
    fn example_parses_and_applies() {
        let text = include_str!("../../../config/node.example.toml");
        let f = FileConfig::parse(text).unwrap();
        let mut cfg = base();
        f.apply(&mut cfg).unwrap();
        assert_eq!(cfg.feed, "239.1.1.1:7001".parse().unwrap());
        assert_eq!(cfg.cores, [Some(2), Some(3), Some(4), Some(5)]);
        assert_eq!(cfg.core.evict_idle_ns, 60_000_000_000);
        assert_eq!(
            cfg.mold.unwrap().session,
            krusty_mold::Session::new("KC20261114").unwrap()
        );
        assert_eq!(cfg.idle, Idle::Spin);
    }

    #[test]
    fn typos_and_bad_values_are_errors() {
        assert!(FileConfig::parse("ingres = \"0.0.0.0:1\"").is_err());
        assert!(
            FileConfig::parse("[limits]\nhistory = 1000")
                .unwrap()
                .apply(&mut base())
                .is_err()
        );
        assert!(
            FileConfig::parse("feed = \"239.1.1.1:7001\"")
                .unwrap()
                .apply(&mut base())
                .is_err()
        );
        assert!(
            FileConfig::parse("[limits]\nmax_payload = 4000")
                .unwrap()
                .apply(&mut base())
                .is_err()
        );
    }
}
