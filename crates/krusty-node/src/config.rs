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
//! [storage]                     # optional durable log (M2)
//! dir = "/var/lib/krusty/stream-1"
//! durability = "a"              # "a": fsync off the latency path; "b": publish after fsync
//! snapshot_every = 1000000      # log entries between core snapshots
//! segment_mib = 256
//! retain_gib = 64               # whole segments beyond this are deleted (below a snapshot)
//! core = 7                      # storage stage
//!
//! [mold]                        # optional MoldUDP64 adapter
//! session = "KC20261114"
//! feed = "239.1.1.2:7004"
//! rerequest = "0.0.0.0:7003"
//!
//! [cluster]                     # optional replication (M3); needs [storage]
//! id = 1
//! members = ["1@10.0.0.5:7100", "2@10.0.0.6:7100", "3@10.0.0.7:7100"]
//! election_min_ms = 50
//! election_max_ms = 100
//! heartbeat_ms = 10
//! core = 6                      # replication I/O stage
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
    pub storage: Option<Storage>,
    pub mold: Option<Mold>,
    pub cluster: Option<Cluster>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cluster {
    pub id: u32,
    /// `"<id>@<ip>:<port>"`, one per member.
    pub members: Vec<String>,
    pub election_min_ms: Option<u64>,
    pub election_max_ms: Option<u64>,
    pub heartbeat_ms: Option<u64>,
    pub max_append_bytes: Option<usize>,
    pub retain_entries: Option<u64>,
    pub core: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Storage {
    pub dir: std::path::PathBuf,
    pub durability: Option<String>,
    pub snapshot_every: Option<u64>,
    pub segment_mib: Option<u64>,
    pub retain_gib: Option<u64>,
    pub core: Option<usize>,
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
        if let Some(st) = &self.storage {
            let durability = match st.durability.as_deref().unwrap_or("a") {
                "a" => crate::Durability::Async,
                "b" => crate::Durability::Sync,
                other => return Err(format!("storage.durability: \"a\" or \"b\", not {other:?}")),
            };
            let mut log = krusty_log::LogConfig::new(&st.dir);
            if let Some(m) = st.segment_mib {
                log.segment_bytes = m << 20;
            }
            if let Some(g) = st.retain_gib {
                log.retain_bytes = g << 30;
            }
            cfg.storage = Some(crate::StorageConfig {
                log,
                durability,
                snapshot_every: st.snapshot_every.unwrap_or(1_000_000),
                core: st.core,
            });
        }
        if let Some(c) = &self.cluster {
            let mut members = Vec::new();
            for m in &c.members {
                let (id, addr) = m
                    .split_once('@')
                    .ok_or_else(|| format!("cluster.members: {m:?} is not \"<id>@<ip>:<port>\""))?;
                let id: u32 = id
                    .parse()
                    .map_err(|_| format!("cluster.members: bad id in {m:?}"))?;
                let addr: SocketAddrV4 = addr
                    .parse()
                    .map_err(|_| format!("cluster.members: bad address in {m:?}"))?;
                members.push((id, addr));
            }
            let mut cl = crate::ClusterConfig::new(c.id, members);
            if let Some(v) = c.election_min_ms {
                cl.election_min_us = v * 1000;
            }
            if let Some(v) = c.election_max_ms {
                cl.election_max_us = v * 1000;
            }
            if let Some(v) = c.heartbeat_ms {
                cl.heartbeat_us = v * 1000;
            }
            if let Some(v) = c.max_append_bytes {
                cl.max_append_bytes = v;
            }
            if let Some(v) = c.retain_entries {
                cl.retain_entries = v;
            }
            cl.core = c.core;
            cfg.cluster = Some(cl);
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
        if let Some(c) = &self.cluster {
            if self.storage.is_none() {
                return Err("[cluster] needs [storage] (the Raft log)".into());
            }
            let mut ids: Vec<u32> = c.members.iter().map(|m| m.0).collect();
            ids.sort_unstable();
            ids.dedup();
            if ids.len() != c.members.len() || ids.contains(&0) {
                return Err("cluster.members: ids must be distinct and at least 1".into());
            }
            if !ids.contains(&c.id) {
                return Err(format!("cluster.id {} is not among the members", c.id));
            }
            if !(c.heartbeat_us < c.election_min_us && c.election_min_us < c.election_max_us) {
                return Err("cluster: need heartbeat < election_min < election_max".into());
            }
            if c.max_append_bytes < krusty_raft::wire::entry_len(8 + self.max_payload + 64)
                || c.max_append_bytes > 65_000
            {
                return Err(
                    "cluster.max_append_bytes: must hold one entry and fit a datagram".into(),
                );
            }
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
        let st = cfg.storage.unwrap();
        assert_eq!(st.durability, crate::Durability::Async);
        assert_eq!(st.log.retain_bytes, 64 << 30);
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

    #[test]
    fn cluster_section_parses_and_is_checked() {
        let text = r#"
            [storage]
            dir = "/tmp/x"
            [cluster]
            id = 2
            members = ["1@10.0.0.5:7100", "2@10.0.0.6:7100", "3@10.0.0.7:7100"]
            election_min_ms = 80
            election_max_ms = 160
            core = 6
        "#;
        let mut cfg = base();
        FileConfig::parse(text).unwrap().apply(&mut cfg).unwrap();
        let c = cfg.cluster.as_ref().unwrap();
        assert_eq!(
            (c.id, c.members.len(), c.election_min_us, c.core),
            (2, 3, 80_000, Some(6))
        );
        assert_eq!(c.addr_of(3), Some("10.0.0.7:7100".parse().unwrap()));
        let bad = |t: &str| {
            FileConfig::parse(t)
                .unwrap()
                .apply(&mut base())
                .unwrap_err()
        };
        assert!(
            bad("[cluster]\nid = 1\nmembers = [\"1@10.0.0.5:7100\"]").contains("needs [storage]")
        );
        // Mode (b) replicates too.
        let mut b = base();
        FileConfig::parse("[storage]\ndir = \"/x\"\ndurability = \"b\"\n[cluster]\nid = 1\nmembers = [\"1@10.0.0.5:7100\"]")
            .unwrap()
            .apply(&mut b)
            .unwrap();
        assert_eq!(b.storage.unwrap().durability, crate::Durability::Sync);
        assert!(
            bad("[storage]\ndir = \"/x\"\n[cluster]\nid = 4\nmembers = [\"1@10.0.0.5:7100\"]")
                .contains("not among")
        );
        assert!(bad("[storage]\ndir = \"/x\"\n[cluster]\nid = 1\nmembers = [\"1@10.0.0.5:7100\", \"1@10.0.0.6:7100\"]").contains("distinct"));
        assert!(
            bad("[storage]\ndir = \"/x\"\n[cluster]\nid = 1\nmembers = [\"10.0.0.5:7100\"]")
                .contains("<id>@")
        );
    }
}
