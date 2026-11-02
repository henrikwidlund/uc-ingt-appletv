// Copyright (c) 2023 Unfolded Circle ApS, Markus Zehnder <markus.z@unfoldedcircle.com>
// Copyright (c) 2026 Henrik Widlund
// SPDX-License-Identifier: MPL-2.0

//! mDNS advertisement with the mdns-sd Rust crate.

use crate::errors::ServiceError;
use log::info;
use mdns_sd::{ServiceDaemon, ServiceInfo, TxtProperty};
use std::sync::OnceLock;

/// The daemon runs in its own thread for as long as the driver runs.
static DAEMON: OnceLock<ServiceDaemon> = OnceLock::new();

/// Publish a service on all network interfaces with the default hostname.
///
/// * `instance_name`: Instance name
/// * `service_name`: The service name (e.g. `http`).
/// * `protocol`: The protocol of the service (e.g. `tcp`).
/// * `port`: The port on which the service accepts connections.
/// * `txt`: TXT record data with format: `key=value`.
pub fn publish_service(
    instance_name: impl AsRef<str>,
    service_name: impl AsRef<str>,
    protocol: impl AsRef<str>,
    port: u16,
    txt: Vec<String>,
) -> Result<(), ServiceError> {
    let daemon = if let Some(d) = DAEMON.get() {
        d
    } else {
        let d = ServiceDaemon::new()
            .map_err(|e| ServiceError::ServiceUnavailable(format!("mDNS daemon failed: {e}")))?;
        DAEMON.get_or_init(|| d)
    };

    let service_type = format!("_{}._{}.local.", service_name.as_ref(), protocol.as_ref());
    let host = hostname::get()?;
    let host = host.to_string_lossy();
    // macOS host names can end with `.local` already.
    let host_name = format!("{}.local.", host.trim_end_matches(".local"));
    let properties: Vec<TxtProperty> = txt
        .iter()
        .map(|v| match v.split_once('=') {
            Some((k, v)) => TxtProperty::from((k, v)),
            None => TxtProperty::from((v.as_str(), "")),
        })
        .collect();

    // The addresses of all interfaces are added and kept up to date by the daemon.
    let service = ServiceInfo::new(
        &service_type,
        instance_name.as_ref(),
        &host_name,
        (),
        port,
        properties,
    )
    .map_err(|e| ServiceError::InternalServerError(format!("Invalid mDNS service: {e}")))?
    .enable_addr_auto();
    let fullname = service.get_fullname().to_string();
    daemon.register(service).map_err(|e| {
        ServiceError::InternalServerError(format!("Failed to register {service_type}: {e}"))
    })?;
    info!("Registered mDNS service: {fullname}");
    Ok(())
}
