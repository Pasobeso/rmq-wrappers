//! Minimal RabbitMQ wrapper around `lapin` with sensible defaults
//! for classroom/event-driven demos.
//!
//! # Quick start
//! ```no_run
//! use anyhow::Result;
//! use rmq_wrappers:z:{Rmq, RmqQueue};
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let rmq = Rmq::create("amqp://guest:guest@localhost:5672/%2f").await?;
//!     let ch = rmq.create_channel().await?;
//!
//!     // Durable queue
//!     let q = ch.create_queue("appointments.requested").await?;
//!     q.publish(r#"{"hello":"world"}"#).await?;
//!
//!     // Delete when done (optional)
//!     // q.delete().await?;
//!     Ok(())
//! }
//! ```

use anyhow::Result;
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties, Consumer, Queue,
    options::{BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions, QueueDeleteOptions},
    types::FieldTable,
};
use serde::Serialize;
use std::fmt::Debug;

/// Thin wrapper over a `lapin::Connection`.
///
/// Keep this alive for the lifespan of your app. Create channels per logical
/// producer/consumer (channels are cheap to clone).
#[derive(Debug)]
pub struct Rmq {
    conn: Connection,
}

/// Wrapper over a `lapin::Channel`.
///
/// You typically obtain this from [`Rmq::create_channel`], then declare queues,
/// publish messages, or start consumers using plain `lapin` if needed.
#[derive(Clone, Debug)]
pub struct RmqChannel {
    channel: Channel,
}

/// A declared queue handle with a bound `Channel`.
///
/// Use [`publish`](RmqQueue::publish) to send messages to the *default exchange*
/// (`""`) with the queue name as the routing key. This is convenient when you
/// don’t need custom exchanges for a class/demo.
#[derive(Debug)]
pub struct RmqQueue {
    channel: Channel,
    queue: Queue,
}

impl Rmq {
    /// Connects to RabbitMQ.
    ///
    /// The URL typically looks like `amqp://user:pass@host:5672`.
    pub async fn connect(amqp_url: &str) -> Result<Self> {
        let conn = Connection::connect(amqp_url, ConnectionProperties::default()).await?;
        Ok(Self { conn })
    }

    /// Opens a new channel on the connection.
    ///
    /// Channels are lightweight and cheap to create/clone.
    pub async fn create_channel(&self) -> Result<RmqChannel> {
        let channel = self.conn.create_channel().await?;
        Ok(RmqChannel { channel })
    }

    /// Access the inner `lapin::Connection` (advanced use).
    pub fn get_inner(&self) -> &Connection {
        &self.conn
    }
}

impl RmqChannel {
    /// Declares a **durable, non-exclusive, non-auto-delete** wrapped queue with sensible defaults.
    ///
    /// Use this when you just want a named queue to publish to or consume from.
    ///
    /// # Durability
    /// - The queue is **durable** and survives broker restarts.
    /// - Pair this with `delivery_mode=2` when publishing to persist messages.
    pub async fn create_queue(&self, queue_name: &str) -> Result<RmqQueue> {
        let queue = self
            .channel
            .queue_declare(
                queue_name,
                QueueDeclareOptions {
                    passive: false,
                    durable: true,
                    auto_delete: false,
                    exclusive: false,
                    nowait: false,
                },
                FieldTable::default(),
            )
            .await?;

        Ok(RmqQueue {
            channel: self.channel.clone(),
            queue,
        })
    }

    /// Declares a non-wrapped consumer for the given queue.
    /// (I see no need for a consumer wrapper)
    pub async fn create_consumer(&self, queue_name: &str, consumer_tag: &str) -> Result<Consumer> {
        let consumer = self
            .channel
            .basic_consume(
                queue_name,
                consumer_tag,
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        Ok(consumer)
    }

    /// Access the inner `lapin::Channel` (advanced use).
    pub fn get_inner(&self) -> &Channel {
        &self.channel
    }
}

impl RmqQueue {
    /// Publishes a message to the **default exchange** (`""`) using the queue name as routing key.
    ///
    /// By default, this sets:
    /// - `content_type = "application/json"`
    /// - `delivery_mode = 2` (persistent), so messages survive broker restarts **if** stored on a durable queue.
    ///
    /// For other content types or headers, pass your own `BasicProperties`.
    ///
    /// # Reliability
    /// This awaits the broker’s **publisher confirm** to ensure the message was accepted.
    pub async fn publish<T>(&self, message: T) -> Result<()>
    where
        T: Serialize,
    {
        let message_bytes = serde_json::to_string(&message)?;
        self.channel
            .basic_publish(
                "",          // default direct exchange
                self.name(), // routing key = queue name
                BasicPublishOptions::default(),
                message_bytes.as_bytes(),
                BasicProperties::default()
                    .with_content_type("application/json".into())
                    .with_delivery_mode(2), // persistent message
            )
            .await?
            .await?; // wait for broker confirm

        Ok(())
    }

    /// Deletes the queue.
    ///
    /// Defaults: `if_unused = false`, `if_empty = false`.
    /// Be careful in shared environments—this removes the queue for all consumers.
    pub async fn delete(&self) -> Result<()> {
        self.channel
            .queue_delete(
                self.name(),
                QueueDeleteOptions {
                    if_unused: false,
                    if_empty: false,
                    nowait: false,
                },
            )
            .await?;
        Ok(())
    }

    /// Queue name as `&str`.
    pub fn name(&self) -> &str {
        self.queue.name().as_str()
    }

    /// Access the inner `lapin::Queue` (advanced use).
    pub fn get_inner(&self) -> &Queue {
        &self.queue
    }
}
