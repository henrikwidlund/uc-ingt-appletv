// Copyright (c) 2023 Unfolded Circle ApS, Markus Zehnder <markus.z@unfoldedcircle.com>
// Copyright (c) 2026 Henrik Widlund
// SPDX-License-Identifier: MPL-2.0

//! Handler for [`R2RequestMsg`].

use crate::controller::handler::setup::{SetDriverUserDataMsg, SetupDriverMsg};
use crate::controller::{Controller, R2RequestMsg, operation_mode};
use crate::errors::ServiceError;
use crate::util::{DeserializeMsgData, return_fut_err, return_fut_ok};
use crate::{APP_VERSION, built_info};
use actix::{AsyncContext, Handler, ResponseFuture};
use log::debug;
use serde_json::Value;
use strum::EnumMessage;
use uc_api::intg::ws::{AvailableEntitiesMsgData, DriverVersionMsgData, R2Request};
use uc_api::intg::{IntegrationVersion, SubscribeEvents};
use uc_api::ws::WsMessage;

/// Integration-API version from the `uc_api` dependency.
fn api_version() -> &'static str {
    built_info::DEPENDENCIES
        .iter()
        .find(|p| p.0 == "uc_api")
        .map_or("?", |v| v.1)
}

impl Handler<R2RequestMsg> for Controller {
    type Result = ResponseFuture<Result<Option<WsMessage>, ServiceError>>;

    #[allow(clippy::too_many_lines)]
    fn handle(&mut self, msg: R2RequestMsg, ctx: &mut Self::Context) -> Self::Result {
        debug!("R2RequestMsg: {:?}", msg.request);
        // A Remote that sends a request is not in standby.
        let Some(session) = self.sessions.get_mut(&msg.ws_id) else {
            return_fut_err!(ServiceError::NotFound("No session found".into()));
        };
        session.standby = false;

        let req_id = msg.req_id;
        // Answered with an event, so it has no response message.
        if msg.request == R2Request::GetDeviceState {
            return_fut_ok!(Some(super::super::device_state_event(self.device_state)));
        }
        let Some(resp_msg) = msg.request.get_message() else {
            return_fut_err!(ServiceError::InternalServerError(format!(
                "No response message for {}",
                msg.request
            )));
        };

        // Requests that are allowed in setup flow and running mode.
        match msg.request {
            R2Request::GetDriverVersion => {
                return_fut_ok!(Some(WsMessage::response(
                    req_id,
                    resp_msg,
                    DriverVersionMsgData {
                        name: self
                            .drv_metadata
                            .name
                            .as_ref()
                            .and_then(|n| n.get("en").cloned()),
                        version: Some(IntegrationVersion {
                            api: Some(api_version().into()),
                            driver: Some(APP_VERSION.into()),
                        }),
                    },
                )));
            }
            R2Request::GetDriverMetadata => {
                return_fut_ok!(Some(WsMessage::response(
                    req_id,
                    resp_msg,
                    &self.drv_metadata
                )));
            }
            R2Request::SetupDriver => {
                let controller = ctx.address();
                let ws_id = msg.ws_id.clone();
                return Box::pin(async move {
                    let data = msg.deserialize()?;
                    controller.send(SetupDriverMsg { ws_id, data }).await??;
                    Ok(Some(WsMessage::response(req_id, resp_msg, Value::Null)))
                });
            }
            R2Request::SetDriverUserData => {
                let controller = ctx.address();
                let ws_id = msg.ws_id.clone();
                return Box::pin(async move {
                    let data = msg.deserialize()?;
                    controller
                        .send(SetDriverUserDataMsg { ws_id, data })
                        .await??;
                    Ok(Some(WsMessage::response(req_id, resp_msg, Value::Null)))
                });
            }
            _ => {}
        }

        // The other requests need the running mode.
        if self
            .sm_consume(&msg.ws_id, &operation_mode::Input::R2Request, ctx)
            .is_err()
        {
            return_fut_err!(ServiceError::ServiceUnavailable(
                "Request cannot be handled: setup required".into()
            ));
        }

        match msg.request {
            R2Request::GetAvailableEntities => {
                let msg_data = AvailableEntitiesMsgData {
                    filter: None,
                    available_entities: self.available_entities(),
                };
                return_fut_ok!(Some(WsMessage::response(req_id, resp_msg, msg_data)));
            }
            R2Request::GetEntityStates => {
                let states = self.entity_states(&msg.ws_id);
                return_fut_ok!(Some(WsMessage::response(req_id, resp_msg, states)));
            }
            R2Request::SubscribeEvents => {
                let ws_id = msg.ws_id.clone();
                match msg.deserialize::<SubscribeEvents>() {
                    Ok(subscribe) => self.subscribe(&ws_id, subscribe.entity_ids),
                    Err(e) => return_fut_err!(e.into()),
                }
                return_fut_ok!(Some(WsMessage::response(req_id, resp_msg, Value::Null)));
            }
            R2Request::UnsubscribeEvents => {
                let ws_id = msg.ws_id.clone();
                match msg.deserialize::<SubscribeEvents>() {
                    Ok(subscribe) => self.unsubscribe(&ws_id, &subscribe.entity_ids),
                    Err(e) => return_fut_err!(e.into()),
                }
                return_fut_ok!(Some(WsMessage::response(req_id, resp_msg, Value::Null)));
            }
            R2Request::EntityCommand => self.entity_command(msg, resp_msg),
            R2Request::BrowseMedia => self.browse_media(msg, resp_msg),
            R2Request::SearchMedia => {
                return_fut_err!(ServiceError::NotSupported("search_media".into()))
            }
            // Handled above. Listed so that the compiler reports new variants.
            R2Request::GetDriverVersion
            | R2Request::GetDriverMetadata
            | R2Request::GetDeviceState
            | R2Request::SetupDriver
            | R2Request::SetDriverUserData => {
                return_fut_err!(ServiceError::InternalServerError(format!(
                    "Request {} not handled",
                    msg.request
                )))
            }
        }
    }
}
