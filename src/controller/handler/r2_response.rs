// Copyright (c) 2023 Unfolded Circle ApS, Markus Zehnder <markus.z@unfoldedcircle.com>
// Copyright (c) 2026 Henrik Widlund
// SPDX-License-Identifier: MPL-2.0

//! Handler for [`R2ResponseMsg`].

use crate::controller::{Controller, R2ResponseMsg};
use actix::Handler;
use log::debug;

impl Handler<R2ResponseMsg> for Controller {
    type Result = ();

    fn handle(&mut self, msg: R2ResponseMsg, _ctx: &mut Self::Context) -> Self::Result {
        debug!("[{}] Remote response: {}", msg.ws_id, msg.msg);
    }
}
