use crate::model::{Level, Meta, Orderbook, Side, Trade, SCHEMA_VERSION};
use anyhow::{bail, Result};
use chrono::{TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio_tungstenite::{connect_async, tungstenite::Message};

pub const WS_URL: &str = "wss://api.upbit.com/websocket/v1";
pub fn subscription(markets: &[String]) -> String {
    serde_json::to_string(&json!([{"ticket":"rustle-public"},{"type":"trade","codes":markets,"isOnlyRealtime":true},{"type":"orderbook","codes":markets,"isOnlyRealtime":true},{"format":"DEFAULT"}])).unwrap()
}
pub async fn top_krw_markets(count: usize) -> Result<Vec<String>> {
    let client = reqwest::Client::new();
    let markets: Vec<Value> = client
        .get("https://api.upbit.com/v1/market/all?isDetails=true")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let krw: Vec<String> = markets
        .into_iter()
        .filter_map(|x| {
            x.get("market")
                .and_then(Value::as_str)
                .filter(|m| m.starts_with("KRW-"))
                .map(str::to_owned)
        })
        .collect();
    // Upbit's ticker endpoint has a market-count limit, so request KRW markets in chunks.
    let mut tickers: Vec<Value> = Vec::new();
    for chunk in krw.chunks(100) {
        let mut page: Vec<Value> = client
            .get("https://api.upbit.com/v1/ticker")
            .query(&[("markets", chunk.join(","))])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        tickers.append(&mut page);
    }
    let mut ranked: Vec<(String, f64)> = tickers
        .into_iter()
        .filter_map(|x| {
            Some((
                x.get("market")?.as_str()?.to_owned(),
                x.get("acc_trade_price_24h")?.as_f64()?,
            ))
        })
        .collect();
    ranked.sort_by(|a, b| b.1.total_cmp(&a.1));
    Ok(ranked.into_iter().take(count).map(|x| x.0).collect())
}
pub fn normalize(v: Value, received: chrono::DateTime<Utc>) -> Result<Incoming> {
    let kind = v.get("type").and_then(Value::as_str).unwrap_or("");
    let market = v
        .get("code")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("missing code"))?
        .to_owned();
    let ms = v
        .get("timestamp")
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow::anyhow!("missing timestamp"))?;
    let exchange_ts = Utc
        .timestamp_millis_opt(ms)
        .single()
        .ok_or_else(|| anyhow::anyhow!("bad timestamp"))?;
    let meta = Meta {
        schema_version: SCHEMA_VERSION,
        market,
        exchange_ts,
        receive_ts: received,
    };
    match kind {
        "trade" => Ok(Incoming::Trade(Trade {
            meta,
            price: num(&v, "trade_price")?,
            volume: num(&v, "trade_volume")?,
            side: if v.get("ask_bid").and_then(Value::as_str) == Some("BID") {
                Side::Buy
            } else {
                Side::Sell
            },
            sequential_id: v.get("sequential_id").and_then(Value::as_u64),
        })),
        "orderbook" => {
            let levels = v
                .get("orderbook_units")
                .and_then(Value::as_array)
                .ok_or_else(|| anyhow::anyhow!("missing units"))?
                .iter()
                .map(|u| {
                    Ok(Level {
                        ask_price: num(u, "ask_price")?,
                        bid_price: num(u, "bid_price")?,
                        ask_size: num(u, "ask_size")?,
                        bid_size: num(u, "bid_size")?,
                    })
                })
                .collect::<Result<_>>()?;
            Ok(Incoming::Orderbook(Orderbook {
                meta,
                total_ask_size: num(&v, "total_ask_size")?,
                total_bid_size: num(&v, "total_bid_size")?,
                levels,
            }))
        }
        _ => bail!("unsupported type {kind}"),
    }
}
fn num(v: &Value, k: &str) -> Result<f64> {
    v.get(k)
        .and_then(Value::as_f64)
        .ok_or_else(|| anyhow::anyhow!("missing {k}"))
}
pub enum Incoming {
    Trade(Trade),
    Orderbook(Orderbook),
}
pub async fn connect(
    markets: &[String],
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
> {
    let (mut ws, _) = connect_async(WS_URL).await?;
    // tungstenite 0.26+ carries text frames as `Utf8Bytes` rather than `String`.
    ws.send(Message::Text(subscription(markets).into())).await?;
    Ok(ws)
}
pub async fn next(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Result<Option<Incoming>> {
    match ws.next().await {
        Some(Ok(Message::Binary(b))) => {
            Ok(Some(normalize(serde_json::from_slice(&b)?, Utc::now())?))
        }
        Some(Ok(Message::Text(s))) => Ok(Some(normalize(serde_json::from_str(&s)?, Utc::now())?)),
        Some(Ok(_)) => Ok(None),
        Some(Err(e)) => Err(e.into()),
        None => bail!("websocket closed"),
    }
}
