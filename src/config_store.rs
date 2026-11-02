// Copyright (c) 2026 Henrik Widlund
// SPDX-License-Identifier: MPL-2.0

//! Configured Apple TV devices and per-device cache files.
//!
//! Files are written to a temporary file first and then renamed, so a crash never leaves a broken
//! file. The configuration holds credentials and is only readable by the owner.

use log::{error, info};
use ratv::{Credentials, ExposeSecret};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fs;
use std::io::{self, Write};
use std::net::IpAddr;
use std::path::{Path, PathBuf};

const CONFIG_FILE: &str = "config.json";
/// Length of the device key used in entity ids.
pub const KEY_LEN: usize = 12;

/// One configured Apple TV.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceConfig {
    /// Identifier at pairing time. It never changes.
    pub id: String,
    /// Key for the entity ids, see [`device_key`].
    pub key: String,
    /// Device name.
    pub name: String,
    /// Latest AirPlay, MRP and Companion identifiers. They can change after tvOS updates.
    #[serde(default)]
    pub known_ids: Vec<String>,
    /// Last known IP address.
    #[serde(default)]
    pub address: Option<IpAddr>,
    /// The address was entered by the user. Multicast discovery is only used as a fallback.
    #[serde(default)]
    pub manual_address: bool,
    /// Pairing credentials.
    #[serde(
        serialize_with = "ser_credentials",
        deserialize_with = "de_credentials"
    )]
    pub credentials: Credentials,
    /// Change the volume of all AirPlay outputs together.
    #[serde(default = "default_true")]
    pub global_volume: bool,
}

impl DeviceConfig {
    /// Returns `true` if one of the identifiers belongs to this device.
    pub fn matches<'a>(&self, mut ids: impl Iterator<Item = &'a str>) -> bool {
        ids.any(|id| id == self.id || self.known_ids.iter().any(|k| k == id))
    }
}

const fn default_true() -> bool {
    true
}

fn ser_credentials<S: Serializer>(credentials: &Credentials, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(credentials.to_secret_string().expose_secret())
}

fn de_credentials<'de, D: Deserializer<'de>>(d: D) -> Result<Credentials, D::Error> {
    let value = String::deserialize(d)?;
    value.parse().map_err(serde::de::Error::custom)
}

/// Derive the entity id key from the device identifier.
///
/// - An AirPlay `deviceid` (`AA:BB:CC:DD:EE:FF`) gives its 12 hex digits.
/// - Other identifiers (MRP `UniqueIdentifier`, Companion `rpMRtID`) give the first 12 hex digits
///   of a 64-bit FNV-1a hash. FNV-1a is used because the output must never change. The std
///   `DefaultHasher` can change between Rust versions.
pub fn device_key(id: &str) -> String {
    let hex: String = id.chars().filter(|c| *c != ':').collect();
    if hex.len() == KEY_LEN && id.len() == 17 && hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return hex.to_ascii_uppercase();
    }
    let hash = fnv1a(id.as_bytes());
    let mut key = format!("{hash:016X}");
    key.truncate(KEY_LEN);
    key
}

/// 64-bit FNV-1a hash. The output never changes between Rust versions.
pub fn fnv1a(data: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0100_0000_01b3;

    data.iter()
        .fold(OFFSET, |h, b| (h ^ u64::from(*b)).wrapping_mul(PRIME))
}

/// Reads and writes the configuration files.
#[derive(Debug, Clone)]
pub struct ConfigStore {
    dir: PathBuf,
}

impl ConfigStore {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    /// Load the configured devices. A missing file gives an empty list.
    pub fn load_devices(&self) -> Vec<DeviceConfig> {
        let path = self.dir.join(CONFIG_FILE);
        match read_json(&path) {
            Ok(Some(devices)) => devices,
            Ok(None) => Vec::new(),
            Err(e) => {
                error!("Error reading {}: {e}", path.display());
                Vec::new()
            }
        }
    }

    /// Save the configured devices.
    pub fn save_devices(&self, devices: &[DeviceConfig]) -> io::Result<()> {
        write_json(&self.dir.join(CONFIG_FILE), &devices, true)?;
        info!("Saved configuration with {} device(s)", devices.len());
        Ok(())
    }

    /// Load the cache of a device. A missing or broken file gives `None`.
    pub fn load_cache<T: DeserializeOwned>(&self, key: &str) -> Option<T> {
        let path = self.cache_path(key);
        read_json(&path).unwrap_or_else(|e| {
            error!("Error reading {}: {e}", path.display());
            None
        })
    }

    /// Save the cache of a device.
    pub fn save_cache<T: Serialize>(&self, key: &str, cache: &T) -> io::Result<()> {
        write_json(&self.cache_path(key), cache, false)
    }

    /// Delete the cache of a device.
    pub fn remove_cache(&self, key: &str) {
        let path = self.cache_path(key);
        if let Err(e) = fs::remove_file(&path)
            && e.kind() != io::ErrorKind::NotFound
        {
            error!("Error deleting {}: {e}", path.display());
        }
    }

    fn cache_path(&self, key: &str) -> PathBuf {
        self.dir.join(format!("cache_{key}.json"))
    }
}

fn read_json<T: DeserializeOwned>(path: &Path) -> io::Result<Option<T>> {
    match fs::read(path) {
        Ok(data) => serde_json::from_slice(&data)
            .map(Some)
            .map_err(io::Error::other),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn write_json<T: Serialize + ?Sized>(path: &Path, value: &T, private: bool) -> io::Result<()> {
    let data = serde_json::to_vec_pretty(value).map_err(io::Error::other)?;
    let tmp = path.with_extension("json.tmp");
    {
        let mut options = fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        if private {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        #[cfg(not(unix))]
        let _ = private;
        let mut file = options.open(&tmp)?;
        file.write_all(&data)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CREDS: &str = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1:\
        b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2:\
        4141:4242";

    fn device() -> DeviceConfig {
        DeviceConfig {
            id: "AA:BB:CC:DD:EE:FF".into(),
            key: device_key("AA:BB:CC:DD:EE:FF"),
            name: "Living Room".into(),
            known_ids: vec!["AA:BB:CC:DD:EE:00".into(), "UUID-1".into()],
            address: Some("10.0.30.6".parse().expect("ip")),
            manual_address: false,
            credentials: CREDS.parse().expect("credentials"),
            global_volume: true,
        }
    }

    #[test]
    fn key_from_airplay_id() {
        assert_eq!("AABBCCDDEEFF", device_key("aa:bb:cc:dd:ee:ff"));
    }

    #[test]
    fn key_from_uuid_is_stable_and_short() {
        let id = "2F4C3D10-7A2B-4D8E-9F01-23456789ABCD";
        let key = device_key(id);
        assert_eq!(KEY_LEN, key.len());
        assert_eq!(key, device_key(id));
        assert!(key.chars().all(|c| c.is_ascii_hexdigit()));
        // Known FNV-1a value, so the key never changes between builds.
        assert_eq!("CBF29CE48422", device_key(""));
    }

    #[test]
    fn matches_any_known_id() {
        let d = device();
        assert!(d.matches(["X", "AA:BB:CC:DD:EE:FF"].into_iter()));
        assert!(d.matches(std::iter::once("UUID-1")));
        assert!(!d.matches(["UUID-2", "AA:BB:CC:DD:EE:11"].into_iter()));
    }

    #[test]
    fn config_round_trip() {
        let dir = std::env::temp_dir().join(format!("uc-intg-appletv-test-{}", std::process::id()));
        fs::create_dir_all(&dir).expect("dir");
        let store = ConfigStore::new(&dir);
        assert_eq!(Vec::<DeviceConfig>::new(), store.load_devices());

        let devices = vec![device()];
        store.save_devices(&devices).expect("save");
        assert_eq!(devices, store.load_devices());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(dir.join(CONFIG_FILE))
                .expect("meta")
                .permissions()
                .mode();
            assert_eq!(0o600, mode & 0o777);
        }

        store
            .save_cache("KEY", &vec!["a".to_string()])
            .expect("cache");
        assert_eq!(
            Some(vec!["a".to_string()]),
            store.load_cache::<Vec<String>>("KEY")
        );
        store.remove_cache("KEY");
        assert_eq!(None, store.load_cache::<Vec<String>>("KEY"));

        fs::remove_dir_all(&dir).expect("cleanup");
    }
}
