use anyhow::Result;
use codex_core::TurnInputRequest;
use codex_features::Feature;
use codex_protocol::protocol::EventMsg;
use codex_protocol::user_input::UserInput;
use core_test_support::responses;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_sse_sequence;
use core_test_support::responses::sse;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::test_codex;
use futures::SinkExt;
use futures::StreamExt;
use pretty_assertions::assert_eq;
use serde_json::Value;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_util::task::AbortOnDropHandle;

#[derive(Clone, Copy)]
enum Reject {
    Sampling,
    Prewarm,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn size_rejection_falls_back_without_replaying_websocket_request() -> Result<()> {
    skip_if_no_network!(Ok(()));
    check_size_rejection(Reject::Sampling, /*retry_budget*/ 2).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn size_rejection_still_falls_back_with_zero_retry_budget() -> Result<()> {
    skip_if_no_network!(Ok(()));
    check_size_rejection(Reject::Sampling, /*retry_budget*/ 0).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prewarm_size_rejection_falls_back_before_sampling() -> Result<()> {
    skip_if_no_network!(Ok(()));
    check_size_rejection(Reject::Prewarm, /*retry_budget*/ 2).await
}

async fn check_size_rejection(reject: Reject, retry_budget: u64) -> Result<()> {
    let server = responses::start_mock_server().await;
    let response_mock = mount_sse_sequence(
        &server,
        vec![
            sse(vec![ev_response_created("http-1"), ev_completed("http-1")]),
            sse(vec![ev_response_created("http-2"), ev_completed("http-2")]),
        ],
    )
    .await;
    let upstream = *server.address();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}/v1", listener.local_addr()?);
    let rejected_requests = Arc::new(Mutex::new(Vec::<Value>::new()));
    let captured = Arc::clone(&rejected_requests);
    let _server_task = AbortOnDropHandle::new(tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let captured = Arc::clone(&captured);
            connections.spawn(async move {
                // Route real WebSocket upgrades here; keep the existing HTTP
                // response mock responsible for decoding and recording POSTs.
                let mut header = [0; 16 * 1024];
                let header_len = timeout(Duration::from_secs(10), async {
                    loop {
                        let count = stream.peek(&mut header).await?;
                        if count == 0 || header[..count].windows(4).any(|s| s == b"\r\n\r\n") {
                            return Ok::<_, std::io::Error>(count);
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await??;
                let header_text =
                    String::from_utf8_lossy(&header[..header_len]).to_ascii_lowercase();
                if !header_text.contains("upgrade: websocket\r\n") {
                    let mut http_stream = TcpStream::connect(upstream).await?;
                    tokio::io::copy_bidirectional(&mut stream, &mut http_stream).await?;
                    return Ok::<_, anyhow::Error>(());
                }
                let mut ws = accept_async(stream).await?;
                while let Some(message) = ws.next().await {
                    let message = message?;
                    match message {
                        Message::Text(text) => {
                            let request: Value = serde_json::from_str(&text)?;
                            let prewarm = request["generate"] == false;
                            if prewarm && matches!(reject, Reject::Sampling) {
                                for event in [ev_response_created("warmup"), ev_completed("warmup")]
                                {
                                    ws.send(Message::Text(event.to_string().into())).await?;
                                }
                                continue;
                            }
                            captured.lock().unwrap().push(request);
                            ws.close(Some(CloseFrame {
                                code: CloseCode::Size,
                                reason: "".into(),
                            }))
                            .await?;
                            break;
                        }
                        Message::Ping(payload) => ws.send(Message::Pong(payload)).await?,
                        Message::Close(_) => break,
                        Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
                    }
                }
                Ok(())
            });
        }
    }));

    let mut builder = test_codex().with_config(move |config| {
        config.model_provider.base_url = Some(base_url);
        config.model_provider.supports_websockets = true;
        config.model_provider.stream_max_retries = Some(retry_budget);
        config.model_provider.request_max_retries = Some(0);
        config.features.enable(Feature::UnboundedConnectionRetries);
    });
    let test = builder.build_with_auto_env(&server).await?;
    let image_url = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aD1sAAAAASUVORK5CYII=";
    test.codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![
            UserInput::Text {
                text: "preserve this image during fallback".into(),
                text_elements: Vec::new(),
            },
            UserInput::Image {
                image_url: image_url.into(),
                detail: None,
            },
        ]))
        .await?;
    let (retry_messages, warnings) = timeout(Duration::from_secs(20), async {
        let mut messages = Vec::new();
        let mut warnings = Vec::new();
        loop {
            match test.codex.next_event().await?.msg {
                EventMsg::StreamError(event) => messages.push(event.message),
                EventMsg::Warning(event) if event.message.contains("1009") => {
                    warnings.push(event.message);
                }
                EventMsg::Error(event) => anyhow::bail!("unexpected error: {}", event.message),
                EventMsg::TurnComplete(_) => return Ok::<_, anyhow::Error>((messages, warnings)),
                _ => {}
            }
        }
    })
    .await??;
    assert_eq!(retry_messages, Vec::<String>::new());
    match reject {
        Reject::Sampling => {
            assert_eq!(warnings.len(), 1);
            insta::assert_snapshot!(warnings[0], @"Falling back from WebSockets to HTTPS transport. server rejected the WebSocket message as too large (close code 1009)");
        }
        Reject::Prewarm => assert_eq!(warnings, Vec::<String>::new()),
    }
    let first_http = response_mock.single_request().body_json();
    assert_eq!(
        image_urls(&first_http["input"]),
        vec![Value::String(image_url.into())]
    );
    assert!(
        first_http["input"]
            .to_string()
            .contains("preserve this image during fallback")
    );
    // The existing session fallback remains sticky; later turns do not retry
    // the already rejected transport.
    test.submit_turn("follow-up").await?;
    let rejected = rejected_requests.lock().unwrap();
    assert_eq!((rejected.len(), response_mock.requests().len()), (1, 2));
    if matches!(reject, Reject::Sampling) {
        assert_eq!(
            image_urls(&rejected[0]["input"]),
            vec![Value::String(image_url.into())]
        );
    }
    Ok(())
}

fn image_urls(input: &Value) -> Vec<Value> {
    input
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|item| item["content"].as_array())
        .flatten()
        .filter(|item| item["type"] == "input_image")
        .map(|item| item["image_url"].clone())
        .collect()
}
