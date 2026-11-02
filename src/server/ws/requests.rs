// Copyright (c) 2022 Unfolded Circle ApS, Markus Zehnder <markus.z@unfoldedcircle.com>
// SPDX-License-Identifier: MPL-2.0

//! Handle request messages from the Remote.

use crate::controller::{Controller, R2RequestMsg};
use crate::errors::ServiceError;
use crate::server::ws::WsConn;
use actix::Addr;
use log::{debug, warn};
use std::str::FromStr;
use uc_api::intg::ws::R2Request;
use uc_api::ws::WsMessage;

impl WsConn {
    /// Forward a request message to the controller.
    ///
    /// - A returned [`ServiceError`] is sent back as an error response.
    /// - `Ok(None)` means the controller sends the response later.
    pub(crate) async fn on_request(
        session_id: &str,
        request: WsMessage,
        controller_addr: Addr<Controller>,
    ) -> Result<Option<WsMessage>, ServiceError> {
        let id = request
            .id
            .ok_or_else(|| ServiceError::BadRequest("Missing property: id".into()))?;
        let msg = request
            .msg
            .as_deref()
            .ok_or_else(|| ServiceError::BadRequest("Missing property: msg".into()))?;

        debug!("[{session_id}] Got request: {msg}");

        if let Ok(req_msg) = R2Request::from_str(msg) {
            controller_addr
                .send(R2RequestMsg {
                    ws_id: session_id.into(),
                    req_id: id,
                    request: req_msg,
                    msg_data: request.msg_data,
                })
                .await?
        } else {
            warn!("[{session_id}] Unknown message: {msg}");
            Err(ServiceError::BadRequest(format!("Unknown message: {msg}")))
        }
    }
}
