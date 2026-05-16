//! WebSocket source/sink — receive and send events over WebSocket connections.

use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

use fluvius_core::event::{Event, OutputEvent};

/// Start a WebSocket server that receives events and sends them to the channel.
pub async fn ws_source(bind: &str, tx: mpsc::Sender<Event>) -> Result<(), std::io::Error> {
    let listener = TcpListener::bind(bind).await?;

    while let Ok((stream, _)) = listener.accept().await {
        let tx = tx.clone();
        tokio::spawn(async move {
            let ws_stream = match accept_async(stream).await {
                Ok(ws) => ws,
                Err(e) => {
                    eprintln!("WebSocket accept error: {e}");
                    return;
                }
            };

            let (_, mut read) = ws_stream.split();

            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        if let Ok(event) = serde_json::from_str::<Event>(&text)
                            && tx.send(event).await.is_err()
                        {
                            break;
                        }
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    _ => {}
                }
            }
        });
    }

    Ok(())
}

/// WebSocket sink — connects to clients and sends output events.
pub struct WsSink {
    _tx: mpsc::Sender<OutputEvent>,
}

impl WsSink {
    /// Start a WebSocket server that broadcasts output events to connected clients.
    pub async fn start(
        bind: &str,
        mut rx: mpsc::Receiver<OutputEvent>,
    ) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(bind).await?;
        let (broadcast_tx, _) = tokio::sync::broadcast::channel::<String>(1000);
        let broadcast_tx_clone = broadcast_tx.clone();

        // Spawn broadcaster
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                if let Ok(json) = serde_json::to_string(&event) {
                    let _ = broadcast_tx_clone.send(json);
                }
            }
        });

        // Accept connections
        while let Ok((stream, _)) = listener.accept().await {
            let mut broadcast_rx = broadcast_tx.subscribe();
            tokio::spawn(async move {
                let ws_stream = match accept_async(stream).await {
                    Ok(ws) => ws,
                    Err(_) => return,
                };

                let (mut write, _): (SplitSink<_, Message>, _) = ws_stream.split();

                while let Ok(msg) = broadcast_rx.recv().await {
                    if write.send(Message::Text(msg.into())).await.is_err() {
                        break;
                    }
                }
            });
        }

        Ok(())
    }
}
