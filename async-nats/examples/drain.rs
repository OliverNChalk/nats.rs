// Copyright 2020-2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Reproduction for drain() timeout bug.
//!
//! Bug: When calling `subscriber.drain()` on an idle connection, the
//! connection handler wouldn't wake itself after processing the drain
//! command. This caused it to wait for the next server message (ping)
//! before making progress, resulting in ~60s delays.
//!
//! Fix: Wake self after processing a Drain command.
//!
//! Expected with fix: completes in <100ms
//! Without fix: hangs for ~60s (ping interval)

use futures_util::stream::StreamExt;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;
    let mut subscriber = client.subscribe("test.>").await?;

    println!("let bg thread finish warmup");
    tokio::time::sleep(Duration::from_secs(5)).await;

    println!("start drain");
    let start = Instant::now();
    subscriber.drain().await?;

    // With the bug: next() would block until the server sends a ping (~60s)
    // With the fix: next() returns None immediately after drain completes
    let msg = subscriber.next().await;
    let elapsed = start.elapsed();

    println!("next() returned {:?} after {:?}", msg, elapsed);
    assert!(
        elapsed.as_secs() < 5,
        "drain took too long: {:?} - bug likely present",
        elapsed
    );
    println!("drain completed promptly");

    Ok(())
}
