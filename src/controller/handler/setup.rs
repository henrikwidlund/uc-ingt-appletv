// Copyright (c) 2023 Unfolded Circle ApS, Markus Zehnder <markus.z@unfoldedcircle.com>
// Copyright (c) 2026 Henrik Widlund
// SPDX-License-Identifier: MPL-2.0

//! Driver setup flow: discovery, pairing and reconfiguration.

use crate::config_store::{DeviceConfig, device_key};
use crate::controller::handler::AbortDriverSetup;
use crate::controller::{Controller, OperationModeInput, OperationModeState};
use crate::device::{UpdateConfig, discovery};
use crate::entities::EntityKind;
use crate::errors::ServiceError;
use actix::{ActorFutureExt, AsyncContext, Context, Handler, Message, WrapFuture};
use log::{debug, info, warn};
use ratv::discovery::Device;
use ratv::{Endpoints, PairOptions, PinPrompt};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::net::IpAddr;
use std::time::Duration;
use uc_api::intg::{DriverSetupChange, IntegrationSetup, SetupDriver};
use uc_api::model::intg::{IntegrationSetupError, IntegrationSetupState, SetupChangeEventType};
use uc_api::ws::{EventCategory, WsMessage};

/// Delay so that the request is acknowledged before the next setup event.
const ACK_DELAY: Duration = Duration::from_millis(100);

/// Start the setup flow.
#[derive(Debug, Message)]
#[rtype(result = "Result<(), ServiceError>")]
pub struct SetupDriverMsg {
    pub ws_id: String,
    pub data: SetupDriver,
}

/// User input of a setup page.
#[derive(Debug, Message)]
#[rtype(result = "Result<(), ServiceError>")]
pub struct SetDriverUserDataMsg {
    pub ws_id: String,
    pub data: IntegrationSetup,
}

/// Apple TV found in the setup flow.
#[derive(Debug, Clone)]
pub struct Found {
    id: String,
    name: String,
    model: String,
    address: Option<IpAddr>,
    endpoints: Endpoints,
    known_ids: Vec<String>,
}

impl Found {
    fn new(device: &Device) -> Option<Self> {
        Some(Self {
            id: device.identifier()?.to_string(),
            name: device.name().to_string(),
            model: device.model().unwrap_or_default().to_string(),
            address: device.addresses.first().copied(),
            endpoints: Endpoints::from(device),
            known_ids: discovery::device_ids(device).map(str::to_string).collect(),
        })
    }
}

/// Page the setup flow waits for.
pub enum SetupState {
    /// Optional IP address for the discovery.
    Discover,
    /// Reconfigure: device and action.
    ConfigMode,
    /// List of found devices. `manual_address`: the user entered the IP address.
    ChooseDevice {
        found: Vec<Found>,
        manual_address: bool,
    },
    /// PIN shown on the Apple TV.
    Pin {
        prompt: Box<PinPrompt>,
        found: Found,
        global_volume: bool,
        manual_address: bool,
        /// Key of the device that is paired again.
        replace: Option<String>,
    },
    /// Options of a configured device, and the Apple TVs on the network it can be linked to.
    Configure { key: String, found: Vec<Found> },
}

impl Handler<SetupDriverMsg> for Controller {
    type Result = Result<(), ServiceError>;

    fn handle(&mut self, msg: SetupDriverMsg, ctx: &mut Self::Context) -> Self::Result {
        debug!("[{}] {:?}", msg.ws_id, msg.data);
        if self
            .sm_consume(&msg.ws_id, &OperationModeInput::SetupDriverRequest, ctx)
            .is_err()
        {
            return Err(ServiceError::BadRequest(
                "Cannot start driver setup. Please abort setup first.".into(),
            ));
        }
        let reconfigure = msg.data.reconfigure == Some(true);
        if let Some(session) = self.sessions.get_mut(&msg.ws_id) {
            session.reconfiguring = msg.data.reconfigure;
        }
        self.setup = None;
        let ws_id = msg.ws_id;
        if reconfigure {
            ctx.run_later(ACK_DELAY, move |act, ctx| {
                let page = act.config_mode_page();
                act.setup = Some(SetupState::ConfigMode);
                act.request_input(&ws_id, page, ctx);
            });
        } else {
            // A first setup starts with a clean configuration.
            self.reset_devices()?;
            ctx.run_later(ACK_DELAY, move |act, ctx| act.request_discover(&ws_id, ctx));
        }
        Ok(())
    }
}

impl Handler<SetDriverUserDataMsg> for Controller {
    type Result = Result<(), ServiceError>;

    fn handle(&mut self, msg: SetDriverUserDataMsg, ctx: &mut Self::Context) -> Self::Result {
        debug!("[{}] setup user data", msg.ws_id);
        if self
            .sm_consume(&msg.ws_id, &OperationModeInput::SetupUserData, ctx)
            .is_err()
        {
            return Err(ServiceError::BadRequest(
                "Not waiting for driver user data. Please restart setup.".into(),
            ));
        }
        let IntegrationSetup::InputValues(values) = msg.data else {
            return Err(ServiceError::BadRequest(
                "Invalid response: require input_values".into(),
            ));
        };
        let ws_id = msg.ws_id;
        let state = self.setup.take();
        match state {
            Some(SetupState::Discover) => {
                let address = parse_address(values.get("address").map(String::as_str))?;
                ctx.run_later(ACK_DELAY, move |act, ctx| act.discover(ws_id, address, ctx));
            }
            Some(SetupState::ConfigMode) => self.config_mode(ws_id, &values, ctx)?,
            Some(SetupState::ChooseDevice {
                found,
                manual_address,
            }) => {
                let choice = values.get("choice").map(String::as_str).unwrap_or_default();
                let found = found
                    .into_iter()
                    .find(|f| f.id == choice)
                    .ok_or_else(|| ServiceError::BadRequest("Unknown device".into()))?;
                let global_volume = parse_bool(&values, "global_volume", true);
                ctx.run_later(ACK_DELAY, move |act, ctx| {
                    act.pair(ws_id, found, global_volume, manual_address, None, ctx);
                });
            }
            Some(SetupState::Pin {
                prompt,
                found,
                global_volume,
                manual_address,
                replace,
            }) => {
                let pin = values
                    .get("pin")
                    .map(|p| p.trim().to_string())
                    .unwrap_or_default();
                if pin.is_empty() {
                    return Err(ServiceError::BadRequest("Missing PIN".into()));
                }
                self.finish_pairing(
                    ws_id,
                    prompt,
                    pin,
                    found,
                    global_volume,
                    manual_address,
                    replace,
                    ctx,
                );
            }
            Some(SetupState::Configure { key, found }) => {
                self.configure(&key, &found, &values)?;
                ctx.run_later(ACK_DELAY, move |act, ctx| {
                    act.finish_setup(&ws_id, None, ctx);
                });
            }
            None => return Err(ServiceError::BadRequest("No active setup step".into())),
        }
        Ok(())
    }
}

impl Handler<AbortDriverSetup> for Controller {
    type Result = ();

    fn handle(&mut self, msg: AbortDriverSetup, ctx: &mut Self::Context) {
        debug!(
            "[{}] abort driver setup, timeout: {}",
            msg.ws_id, msg.timeout
        );
        self.setup = None;
        if msg.timeout {
            self.finish_setup(&msg.ws_id, Some(IntegrationSetupError::Timeout), ctx);
            return;
        }
        if self
            .sm_consume(&msg.ws_id, &OperationModeInput::AbortSetup, ctx)
            .is_err()
        {
            return;
        }
        // A reconfiguration that was aborted keeps the configured devices.
        if let Some(session) = self.sessions.get_mut(&msg.ws_id) {
            session.reconfiguring = None;
        }
        if !self.devices.is_empty() {
            let _ = self.sm_consume(&msg.ws_id, &OperationModeInput::ConfigurationAvailable, ctx);
            self.connect_devices();
        }
    }
}

impl Controller {
    /// Ask for an optional IP address, then discover.
    fn request_discover(&mut self, ws_id: &str, ctx: &mut Context<Self>) {
        self.setup = Some(SetupState::Discover);
        self.request_input(ws_id, discover_page(), ctx);
    }

    /// Scan for Apple TVs and show the ones that are not configured.
    fn discover(&self, ws_id: String, address: Option<IpAddr>, ctx: &mut Context<Self>) {
        info!("[{ws_id}] discovering Apple TV devices, address: {address:?}");
        ctx.spawn(
            async move { discovery::scan_apple_tvs(address).await }
                .into_actor(self)
                .map(move |result, act, ctx| {
                    let devices = match result {
                        Ok(d) => d,
                        Err(e) => {
                            warn!("[{ws_id}] discovery failed: {e}");
                            act.finish_setup(&ws_id, Some(IntegrationSetupError::Other), ctx);
                            return;
                        }
                    };
                    let found: Vec<Found> = devices
                        .iter()
                        .filter(|d| {
                            !act.devices
                                .values()
                                .any(|c| c.cfg.matches(discovery::device_ids(d)))
                        })
                        .filter_map(Found::new)
                        .collect();
                    if found.is_empty() {
                        info!("[{ws_id}] no new Apple TV found");
                        act.finish_setup(&ws_id, Some(IntegrationSetupError::NotFound), ctx);
                        return;
                    }
                    let page = choose_device_page(&found);
                    act.setup = Some(SetupState::ChooseDevice {
                        found,
                        manual_address: address.is_some(),
                    });
                    act.request_input(&ws_id, page, ctx);
                }),
        );
    }

    fn config_mode(
        &mut self,
        ws_id: String,
        values: &HashMap<String, String>,
        ctx: &mut Context<Self>,
    ) -> Result<(), ServiceError> {
        let action = values.get("action").map(String::as_str).unwrap_or_default();
        let key = values.get("choice").cloned().unwrap_or_default();
        match action {
            "add" => {
                ctx.run_later(ACK_DELAY, move |act, ctx| act.request_discover(&ws_id, ctx));
            }
            "remove" => {
                let cfg = self
                    .remove_device(&key)
                    .ok_or_else(|| ServiceError::BadRequest("Unknown device".into()))?;
                info!("[{ws_id}] removed {}", cfg.name);
                self.save_devices()?;
                for session in self.sessions.values_mut() {
                    session
                        .subscribed_entities
                        .retain(|id| EntityKind::parse(id).is_none_or(|(k, _)| k != key));
                }
                ctx.run_later(ACK_DELAY, move |act, ctx| {
                    act.finish_setup(&ws_id, None, ctx);
                });
            }
            "configure" => {
                let cfg = self
                    .devices
                    .get(key.as_str())
                    .ok_or_else(|| ServiceError::BadRequest("Unknown device".into()))?
                    .cfg
                    .clone();
                self.request_configure(ws_id, cfg, ctx);
            }
            "pair" => {
                let device = self
                    .devices
                    .get(key.as_str())
                    .ok_or_else(|| ServiceError::BadRequest("Unknown device".into()))?;
                let cfg = device.cfg.clone();
                ctx.run_later(ACK_DELAY, move |act, ctx| act.pair_again(ws_id, cfg, ctx));
            }
            "reset" => {
                self.reset_devices()?;
                info!("[{ws_id}] configuration reset");
                ctx.run_later(ACK_DELAY, move |act, ctx| act.request_discover(&ws_id, ctx));
            }
            _ => {
                return Err(ServiceError::BadRequest(format!(
                    "Unknown action: {action}"
                )));
            }
        }
        Ok(())
    }

    /// Remove all devices and their entities.
    fn reset_devices(&mut self) -> Result<(), ServiceError> {
        if self.devices.is_empty() {
            return Ok(());
        }
        let keys: Vec<String> = self.devices.keys().map(ToString::to_string).collect();
        for key in &keys {
            self.remove_device(key);
        }
        self.entities.clear();
        for session in self.sessions.values_mut() {
            session.subscribed_entities.clear();
        }
        self.save_devices()
    }

    /// Scan the network, then show the options of a configured device.
    fn request_configure(&self, ws_id: String, cfg: DeviceConfig, ctx: &mut Context<Self>) {
        ctx.spawn(
            async move { discovery::scan_apple_tvs(None).await }
                .into_actor(self)
                .map(move |result, act, ctx| {
                    let devices = result.unwrap_or_else(|e| {
                        warn!("[{ws_id}] discovery failed: {e}");
                        Vec::new()
                    });
                    // The device itself, and devices that are not configured.
                    let found: Vec<Found> = devices
                        .iter()
                        .filter(|d| {
                            cfg.matches(discovery::device_ids(d))
                                || !act
                                    .devices
                                    .values()
                                    .any(|c| c.cfg.matches(discovery::device_ids(d)))
                        })
                        .filter_map(Found::new)
                        .collect();
                    let page = configure_page(&cfg, &found);
                    act.setup = Some(SetupState::Configure {
                        key: cfg.key,
                        found,
                    });
                    act.request_input(&ws_id, page, ctx);
                }),
        );
    }

    /// Find a configured device and pair it again.
    fn pair_again(&self, ws_id: String, cfg: DeviceConfig, ctx: &mut Context<Self>) {
        ctx.spawn(
            async move {
                let found = discovery::find(&cfg).await;
                (cfg, found)
            }
            .into_actor(self)
            .map(move |(cfg, result), act, ctx| match result {
                Ok(Some(device)) => {
                    let Some(mut found) = Found::new(&device) else {
                        act.finish_setup(&ws_id, Some(IntegrationSetupError::NotFound), ctx);
                        return;
                    };
                    // Keep the id from the first pairing.
                    found.id.clone_from(&cfg.id);
                    found.name.clone_from(&cfg.name);
                    if cfg.manual_address {
                        found.address = cfg.address;
                    }
                    act.pair(
                        ws_id,
                        found,
                        cfg.global_volume,
                        cfg.manual_address,
                        Some(cfg.key),
                        ctx,
                    );
                }
                Ok(None) => act.finish_setup(&ws_id, Some(IntegrationSetupError::NotFound), ctx),
                Err(e) => {
                    warn!("[{ws_id}] discovery failed: {e}");
                    act.finish_setup(&ws_id, Some(IntegrationSetupError::Other), ctx);
                }
            }),
        );
    }

    /// Start pairing. The Apple TV shows a PIN.
    fn pair(
        &self,
        ws_id: String,
        found: Found,
        global_volume: bool,
        manual_address: bool,
        replace: Option<String>,
        ctx: &mut Context<Self>,
    ) {
        info!("[{ws_id}] pairing with {} ({})", found.name, found.model);
        let mut options = PairOptions::default();
        options.name.clone_from(&self.device_ctx.client_name);
        let endpoints = found.endpoints;
        ctx.spawn(
            async move { ratv::pair(&endpoints, &options).await }
                .into_actor(self)
                .map(move |result, act, ctx| match result {
                    Ok(prompt) => {
                        let page = pin_page(&found.name);
                        act.setup = Some(SetupState::Pin {
                            prompt: Box::new(prompt),
                            found,
                            global_volume,
                            manual_address,
                            replace,
                        });
                        act.request_input(&ws_id, page, ctx);
                    }
                    Err(e) => {
                        warn!("[{ws_id}] pairing failed: {e}");
                        act.finish_setup(&ws_id, Some(setup_error(&e)), ctx);
                    }
                }),
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_pairing(
        &self,
        ws_id: String,
        prompt: Box<PinPrompt>,
        pin: String,
        found: Found,
        global_volume: bool,
        manual_address: bool,
        replace: Option<String>,
        ctx: &mut Context<Self>,
    ) {
        ctx.spawn(
            async move { prompt.finish(&pin).await }
                .into_actor(self)
                .map(move |result, act, ctx| {
                    let credentials = match result {
                        Ok(c) => c,
                        Err(e) => {
                            warn!("[{ws_id}] pairing failed: {e}");
                            act.finish_setup(&ws_id, Some(setup_error(&e)), ctx);
                            return;
                        }
                    };
                    info!("[{ws_id}] paired with {}", found.name);
                    let existing = replace
                        .as_deref()
                        .and_then(|k| act.devices.get(k))
                        .map(|d| d.cfg.clone());
                    let cfg = match existing {
                        Some(mut cfg) => {
                            cfg.credentials = credentials;
                            cfg.known_ids = found.known_ids;
                            cfg.address = found.address;
                            cfg
                        }
                        None => DeviceConfig {
                            key: device_key(&found.id),
                            id: found.id,
                            name: found.name,
                            known_ids: found.known_ids,
                            address: found.address,
                            manual_address,
                            credentials,
                            global_volume,
                        },
                    };
                    if let Some(device) = act.devices.get_mut(cfg.key.as_str()) {
                        device.cfg = cfg.clone();
                        device.addr.do_send(UpdateConfig(cfg));
                    } else {
                        act.add_device(cfg, ctx);
                    }
                    let error = act.save_devices().err().map(|e| {
                        warn!("[{ws_id}] saving the configuration failed: {e}");
                        IntegrationSetupError::Other
                    });
                    act.finish_setup(&ws_id, error, ctx);
                }),
        );
    }

    /// Apply the options page of a configured device.
    fn configure(
        &mut self,
        key: &str,
        found: &[Found],
        values: &HashMap<String, String>,
    ) -> Result<(), ServiceError> {
        let address = parse_address(values.get("address").map(String::as_str))?;
        let device = self
            .devices
            .get_mut(key)
            .ok_or_else(|| ServiceError::BadRequest("Unknown device".into()))?;
        let cfg = &mut device.cfg;
        // The identifiers can all change, for example after a tvOS update. Link to the chosen device.
        let choice = values.get("device").map(String::as_str).unwrap_or_default();
        if let Some(f) = found.iter().find(|f| f.id == choice)
            && !cfg.matches(f.known_ids.iter().map(String::as_str))
        {
            info!("[{}] linked to {} ({:?})", cfg.name, f.name, f.known_ids);
            cfg.known_ids.clone_from(&f.known_ids);
            if !cfg.manual_address {
                cfg.address = f.address;
            }
        }
        cfg.global_volume = parse_bool(values, "global_volume", cfg.global_volume);
        cfg.manual_address = address.is_some();
        if address.is_some() {
            cfg.address = address;
        }
        device.addr.do_send(UpdateConfig(cfg.clone()));
        self.save_devices()
    }

    /// Show a setup page.
    fn request_input(&mut self, ws_id: &str, page: Value, ctx: &mut Context<Self>) {
        if self
            .sm_consume(ws_id, &OperationModeInput::RequestUserInput, ctx)
            .is_err()
        {
            return;
        }
        let event = json!({
            "event_type": SetupChangeEventType::Setup,
            "state": IntegrationSetupState::WaitUserAction,
            "require_user_action": { "input": page },
        });
        self.send_r2_msg(
            WsMessage::event("driver_setup_change", EventCategory::Device, event),
            ws_id,
        );
    }

    /// End the setup flow.
    fn finish_setup(
        &mut self,
        ws_id: &str,
        error: Option<IntegrationSetupError>,
        ctx: &mut Context<Self>,
    ) {
        self.setup = None;
        let input = if error.is_none() {
            OperationModeInput::Successful
        } else {
            OperationModeInput::SetupError
        };
        if self.sm_consume(ws_id, &input, ctx).is_err() {
            return;
        }
        if let Some(session) = self.sessions.get_mut(ws_id) {
            session.reconfiguring = None;
        }
        if matches!(self.machine.state(), OperationModeState::Running) {
            self.connect_devices();
        }
        let state = if error.is_none() {
            IntegrationSetupState::Ok
        } else {
            IntegrationSetupState::Error
        };
        info!("[{ws_id}] setup flow finished: {state:?}");
        match serde_json::to_value(DriverSetupChange {
            event_type: SetupChangeEventType::Stop,
            state,
            error,
            error_message: None,
            require_user_action: None,
        }) {
            Ok(event) => self.send_r2_msg(
                WsMessage::event("driver_setup_change", EventCategory::Device, event),
                ws_id,
            ),
            Err(e) => warn!("driver_setup_change serialize error: {e}"),
        }
    }

    fn config_mode_page(&self) -> Value {
        let mut devices: Vec<_> = self.devices.values().map(|d| &d.cfg).collect();
        devices.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        let mut items: Vec<Value> = devices
            .iter()
            .map(|c| json!({ "id": c.key, "label": { "en": format!("{} ({})", c.name, c.id) } }))
            .collect();
        let mut actions = vec![json!({ "id": "add", "label": { "en": "Add a new device" } })];
        if items.is_empty() {
            items.push(json!({ "id": "", "label": { "en": "---" } }));
        } else {
            actions.extend([
                json!({ "id": "remove", "label": { "en": "Delete selected device" } }),
                json!({ "id": "configure", "label": { "en": "Configure selected device" } }),
                json!({ "id": "pair", "label": { "en": "Pair selected device again" } }),
                json!({ "id": "reset", "label": { "en": "Reset configuration and reconfigure" } }),
            ]);
        }
        let first = devices.first().map(|c| c.key.as_str()).unwrap_or_default();
        json!({
            "title": { "en": "Configuration mode" },
            "settings": [
                {
                    "id": "choice",
                    "label": { "en": "Configured devices" },
                    "field": { "dropdown": { "value": first, "items": items } }
                },
                {
                    "id": "action",
                    "label": { "en": "Action" },
                    "field": { "dropdown": { "value": "add", "items": actions } }
                }
            ]
        })
    }
}

fn discover_page() -> Value {
    json!({
        "title": { "en": "Setup mode" },
        "settings": [
            {
                "id": "info",
                "label": { "en": "Discover or connect to Apple TV device" },
                "field": { "label": { "value": { "en": "Leave blank to use auto-discovery and click _Next_.\n\nThe device must be on the same network as the remote." } } }
            },
            {
                "id": "address",
                "label": { "en": "IP address (same network only)" },
                "field": { "text": { "value": "" } }
            }
        ]
    })
}

fn global_volume_field(value: bool) -> Value {
    json!({
        "id": "global_volume",
        "label": { "en": "Change volume on all connected devices" },
        "field": { "checkbox": { "value": value } }
    })
}

fn device_label(f: &Found) -> String {
    f.address
        .map_or_else(|| f.name.clone(), |a| format!("{} ({a})", f.name))
}

fn choose_device_page(found: &[Found]) -> Value {
    let items: Vec<Value> = found
        .iter()
        .map(|f| json!({ "id": f.id, "label": { "en": device_label(f) } }))
        .collect();
    json!({
        "title": { "en": "Please choose your Apple TV" },
        "settings": [
            {
                "id": "choice",
                "label": { "en": "Choose your Apple TV" },
                "field": { "dropdown": { "value": found.first().map(|f| f.id.as_str()).unwrap_or_default(), "items": items } }
            },
            global_volume_field(true)
        ]
    })
}

fn pin_page(name: &str) -> Value {
    json!({
        "title": { "en": "Enter the PIN" },
        "settings": [
            {
                "id": "pin",
                "label": { "en": format!("Enter the PIN shown on {name}") },
                "field": { "text": { "value": "", "regex": "^[0-9]{4}$" } }
            }
        ]
    })
}

fn configure_page(cfg: &DeviceConfig, found: &[Found]) -> Value {
    let address = if cfg.manual_address {
        cfg.address.map(|a| a.to_string()).unwrap_or_default()
    } else {
        String::new()
    };
    let current = found
        .iter()
        .find(|f| cfg.matches(f.known_ids.iter().map(String::as_str)))
        .map_or("", |f| f.id.as_str());
    let mut items: Vec<Value> = found
        .iter()
        .map(
            |f| json!({ "id": f.id, "label": { "en": format!("{} ({})", device_label(f), f.id) } }),
        )
        .collect();
    items.push(json!({ "id": "", "label": { "en": "Not found: keep the current identifiers" } }));
    json!({
        "title": { "en": "Configure your Apple TV" },
        "settings": [
            {
                "id": "device",
                "label": { "en": "Apple TV on the network" },
                "field": { "dropdown": { "value": current, "items": items } }
            },
            {
                "id": "address",
                "label": { "en": "IP address (optional)" },
                "field": { "text": { "value": address } }
            },
            global_volume_field(cfg.global_volume)
        ]
    })
}

fn parse_address(value: Option<&str>) -> Result<Option<IpAddr>, ServiceError> {
    match value.map(str::trim) {
        None | Some("") => Ok(None),
        Some(v) => v
            .parse()
            .map(Some)
            .map_err(|_| ServiceError::BadRequest(format!("Invalid IP address: {v}"))),
    }
}

fn parse_bool(values: &HashMap<String, String>, key: &str, default: bool) -> bool {
    values.get(key).map_or(default, |v| v == "true")
}

const fn setup_error(e: &ratv::Error) -> IntegrationSetupError {
    match e {
        e if e.is_auth() => IntegrationSetupError::AuthorizationError,
        ratv::Error::Hap(_) => IntegrationSetupError::AuthorizationError,
        ratv::Error::Timeout | ratv::Error::NoAnswer(_) => IntegrationSetupError::Timeout,
        ratv::Error::Io(_) | ratv::Error::NoEndpoints => IntegrationSetupError::ConnectionRefused,
        _ => IntegrationSetupError::Other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses() {
        assert_eq!(None, parse_address(None).expect("ok"));
        assert_eq!(None, parse_address(Some(" ")).expect("ok"));
        assert_eq!(
            Some(IpAddr::from([10, 0, 0, 2])),
            parse_address(Some("10.0.0.2")).expect("ok")
        );
        assert!(parse_address(Some("tv.local")).is_err());
    }
}
