use anyhow::{Ok, Result};
use futures_lite::StreamExt;
use lapin::options::BasicAckOptions;
use rmq_bus::Rmq;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let rmq = Rmq::create("amqp://localhost:5672").await?;
    info!("Connected to RabbitMQ");

    let producer_ch = rmq.create_channel().await?;
    let consumer_ch = rmq.create_channel().await?;

    let queue = producer_ch.create_queue("hello_queue").await?;

    info!("Created queue: \"{}\"", queue.name());

    let mut consumer = consumer_ch
        .create_consumer(queue.name(), "my_consumer")
        .await?;

    info!("Created consumer: \"{}\"", consumer.tag());

    // queue.publish("HELLO GUYS").await?;

    while let Some(next) = consumer.next().await {
        let delivery = next?;
        let data = String::from_utf8_lossy(&delivery.data);

        info!("Received message: {}", data);

        delivery.ack(BasicAckOptions::default()).await?;
    }

    Ok(())
}
