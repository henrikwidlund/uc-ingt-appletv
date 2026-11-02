// Copyright (c) 2026 Henrik Widlund
// SPDX-License-Identifier: MPL-2.0

//! Find Apple TV devices and AirPlay outputs on the network.

use crate::config_store::DeviceConfig;
use log::{debug, info};
use ratv::discovery::{Device, MDNS_PORT, ScanOptions, ServiceKind, scan};
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

/// Time for a multicast scan.
const SCAN_TIMEOUT: Duration = Duration::from_secs(3);
/// Time for a unicast scan to a known address.
const UNICAST_TIMEOUT: Duration = Duration::from_secs(2);

/// The identifiers of a discovered device: AirPlay, MRP and Companion.
pub fn device_ids(device: &Device) -> impl Iterator<Item = &str> {
    [
        device
            .service(ServiceKind::AirPlay)
            .and_then(|s| s.txt.get("deviceid")),
        device
            .service(ServiceKind::MediaRemote)
            .and_then(|s| s.txt.get("UniqueIdentifier")),
        device
            .service(ServiceKind::Companion)
            .and_then(|s| s.txt.get("rpMRtID")),
    ]
    .into_iter()
    .flatten()
}

/// Returns `true` for an Apple TV. HomePod devices also report tvOS, so the model is checked.
pub fn is_apple_tv(device: &Device) -> bool {
    device.model().is_some_and(is_supported_model)
}

/// Apple TV HD (`AppleTV5,3`) and newer. Older models have no tvOS.
fn is_supported_model(model: &str) -> bool {
    model
        .strip_prefix("AppleTV")
        .and_then(|v| v.split(',').next())
        .and_then(|major| major.parse::<u32>().ok())
        .is_some_and(|major| major >= 5)
}

/// Scan for Apple TV devices. With an address, only that host is asked.
pub async fn scan_apple_tvs(address: Option<IpAddr>) -> Result<Vec<Device>, ratv::Error> {
    let mut options = ScanOptions::default();
    options.timeout = SCAN_TIMEOUT;
    if let Some(ip) = address {
        options.targets = vec![SocketAddr::new(ip, MDNS_PORT)];
    }
    let mut devices = scan(&options).await?;
    devices.retain(is_apple_tv);
    Ok(devices)
}

/// Find a configured Apple TV.
///
/// Asks the last known address first, then the whole network unless the address was entered
/// by the user and the device answered there.
pub async fn find(cfg: &DeviceConfig) -> Result<Option<Device>, ratv::Error> {
    if let Some(ip) = cfg.address {
        let mut options = ScanOptions::default();
        options.timeout = UNICAST_TIMEOUT;
        options.targets = vec![SocketAddr::new(ip, MDNS_PORT)];
        if let Some(device) = scan(&options)
            .await?
            .into_iter()
            .find(|d| cfg.matches(device_ids(d)))
        {
            debug!("[{}] found at {ip}", cfg.name);
            return Ok(Some(device));
        }
        info!("[{}] not found at {ip}, scanning the network", cfg.name);
    }

    let mut options = ScanOptions::default();
    options.timeout = SCAN_TIMEOUT;
    Ok(scan(&options)
        .await?
        .into_iter()
        .find(|d| cfg.matches(device_ids(d))))
}

/// Update the identifiers, name and address of a configuration from a discovered device.
///
/// Returns `true` if something changed.
pub fn update_config(cfg: &mut DeviceConfig, device: &Device) -> bool {
    let mut changed = false;
    let ids: Vec<String> = device_ids(device).map(str::to_string).collect();
    if !ids.is_empty() && ids != cfg.known_ids {
        info!(
            "[{}] identifiers changed: {:?} -> {ids:?}",
            cfg.name, cfg.known_ids
        );
        cfg.known_ids = ids;
        changed = true;
    }
    let name = device.name();
    if !name.is_empty() && name != cfg.name {
        info!("[{}] name changed to {name}", cfg.name);
        cfg.name = name.to_string();
        changed = true;
    }
    if let Some(ip) = device.addresses.first().copied()
        && cfg.address != Some(ip)
        && !cfg.manual_address
    {
        info!("[{}] address changed to {ip}", cfg.name);
        cfg.address = Some(ip);
        changed = true;
    }
    changed
}

/// Scan for AirPlay devices that can join the group of an Apple TV: (output uid, name).
///
/// `own_uid` is the output uid of the Apple TV. It is left out.
pub async fn scan_airplay_outputs(
    own_uid: Option<&str>,
) -> Result<Vec<(String, String)>, ratv::Error> {
    let mut options = ScanOptions::default();
    options.timeout = SCAN_TIMEOUT;
    options.services = vec![ServiceKind::AirPlay];
    let devices = scan(&options).await?;
    Ok(devices
        .iter()
        .filter_map(|d| {
            let uid = d.output_uid()?;
            if own_uid.is_some_and(|o| o.eq_ignore_ascii_case(uid)) {
                return None;
            }
            Some((uid.to_string(), d.name().to_string()))
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supported_models() {
        for model in ["AppleTV5,3", "AppleTV6,2", "AppleTV11,1", "AppleTV14,1"] {
            assert!(is_supported_model(model), "{model}");
        }
        for model in [
            "AppleTV3,2",
            "AppleTV2,1",
            "AudioAccessory5,1",
            "AppleTV",
            "AppleTVx,1",
        ] {
            assert!(!is_supported_model(model), "{model}");
        }
    }
}
