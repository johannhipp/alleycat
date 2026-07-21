use std::env;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

use serde_json::{Value, json};

const SESSION_ID: &str = "omp-e2e-session";

fn response(id: &Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result,
    })
}

fn error_response(id: &Value, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message,
        },
    })
}

fn state_path() -> Option<PathBuf> {
    env::var_os("FAKE_OMP_ACP_STATE").map(PathBuf::from)
}

fn load_deleted(path: Option<&PathBuf>) -> bool {
    path.and_then(|path| fs::read_to_string(path).ok())
        .is_some_and(|state| state == "deleted")
}

fn save_deleted(path: Option<&PathBuf>, deleted: bool) {
    if let Some(path) = path {
        fs::write(path, if deleted { "deleted" } else { "active" })
            .expect("write fake OMP ACP state");
    }
}

fn main() {
    let stdin = io::stdin();
    let mut stdout = io::BufWriter::new(io::stdout().lock());
    let state_path = state_path();
    let paginate = env::var("FAKE_OMP_ACP_PAGINATE").as_deref() == Ok("1");
    let delete_noop = env::var("FAKE_OMP_ACP_DELETE_NOOP").as_deref() == Ok("1");
    let prompt_error = env::var("FAKE_OMP_ACP_PROMPT_ERROR").as_deref() == Ok("1");
    let mut session_cwd = "/".to_string();
    let mut deleted = load_deleted(state_path.as_ref());

    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        let Ok(request) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let Some(method) = request.get("method").and_then(Value::as_str) else {
            continue;
        };
        let Some(id) = request.get("id") else {
            continue;
        };

        let result = match method {
            "initialize" => json!({
                "protocolVersion": 1,
                "agentInfo": {
                    "name": "oh-my-pi",
                    "title": "Oh My Pi",
                    "version": "test",
                },
                "agentCapabilities": {
                    "loadSession": true,
                    "promptCapabilities": {
                        "embeddedContext": true,
                        "image": false,
                    },
                    "sessionCapabilities": {
                        "list": {},
                        "resume": {},
                        "close": {},
                    },
                },
            }),
            "session/new" => {
                session_cwd = request
                    .get("params")
                    .and_then(|params| params.get("cwd"))
                    .and_then(Value::as_str)
                    .unwrap_or("/")
                    .to_string();
                deleted = false;
                save_deleted(state_path.as_ref(), deleted);
                json!({
                    "sessionId": SESSION_ID,
                    "configOptions": [],
                })
            }
            "session/list" => {
                let cursor = request
                    .get("params")
                    .and_then(|params| params.get("cursor"))
                    .and_then(Value::as_str);
                if deleted {
                    json!({ "sessions": [] })
                } else if paginate && cursor.is_none() {
                    json!({
                        "sessions": [{
                            "sessionId": "omp-e2e-distractor",
                            "cwd": session_cwd,
                            "updatedAt": "2026-01-01T00:00:00Z",
                        }],
                        "nextCursor": "1",
                    })
                } else {
                    json!({
                        "sessions": [{
                            "sessionId": SESSION_ID,
                            "cwd": session_cwd,
                            "updatedAt": "2026-01-01T00:00:00Z",
                        }],
                    })
                }
            }
            "session/load" => json!({ "configOptions": [] }),
            "session/prompt" => {
                let prompt = request
                    .get("params")
                    .and_then(|params| params.get("prompt"))
                    .and_then(Value::as_array)
                    .and_then(|prompt| prompt.first())
                    .and_then(|item| item.get("text"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if prompt == "/session delete" && !delete_noop {
                    deleted = true;
                    save_deleted(state_path.as_ref(), deleted);
                }
                json!({ "stopReason": "end_turn" })
            }
            "session/close" => json!({}),
            _ => json!({}),
        };

        let frame = if prompt_error && method == "session/prompt" {
            error_response(id, -32603, "fake prompt failure")
        } else {
            response(id, result)
        };
        let line = serde_json::to_string(&frame).expect("serialize response");
        writeln!(stdout, "{line}").expect("write response");
        stdout.flush().expect("flush response");
    }
}
