#![allow(unknown_lints, dependency_on_unit_never_type_fallback)]
use futures_util::StreamExt as _;
use redis::{AsyncCommands, GlideConnectionOptions};

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let mut publish_conn = client
        .get_multiplexed_async_connection(GlideConnectionOptions::default())
        .await?;
    let mut pubsub_conn = client.get_async_pubsub().await?;

    pubsub_conn.subscribe("wavephone").await?;
    let mut pubsub_stream = pubsub_conn.on_message();

    publish_conn.publish("wavephone", "banana").await?;

    let pubsub_msg: String = pubsub_stream.next().await.unwrap().get_payload()?;
    assert_eq!(&pubsub_msg, "banana");

    Ok(())
}
