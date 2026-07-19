use std::path::Path;
use std::process::Stdio;

use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

struct RpcClient {
    stdin: ChildStdin,
    lines: Lines<BufReader<ChildStdout>>,
    next_id: i64,
}

impl RpcClient {
    async fn request(&mut self, method: &str, params: Value) -> (Value, Vec<Value>) {
        let id = self.next_id;
        self.next_id += 1;
        let request = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        self.stdin
            .write_all(format!("{request}\n").as_bytes())
            .await
            .expect("write bridge request");
        self.stdin.flush().await.expect("flush bridge request");

        let mut notifications = Vec::new();
        while let Some(line) = self.lines.next_line().await.expect("read bridge response") {
            let frame: Value = serde_json::from_str(&line).expect("valid JSON-RPC frame");
            if frame.get("id").and_then(Value::as_i64) == Some(id) {
                return (frame, notifications);
            }
            notifications.push(frame);
        }
        panic!("bridge exited before responding to {method}");
    }
}

async fn spawn_bridge(
    cwd: &Path,
    archive_prompt: bool,
    paginate_session_list: bool,
    delete_noop: bool,
    prompt_error: bool,
) -> (Child, RpcClient) {
    let fake_agent = env!("CARGO_BIN_EXE_fake-omp-acp");
    let mut command = Command::new(env!("CARGO_BIN_EXE_alleycat-acp-bridge"));
    command
        .env("ACP_BRIDGE_AGENT_BIN", fake_agent)
        .env("ACP_BRIDGE_AGENT_ARGS", "acp")
        .env("ACP_BRIDGE_REQUEST_TIMEOUT_SECS", "5")
        .env("FAKE_OMP_ACP_STATE", cwd.join("fake-omp-acp-state"))
        .env(
            "FAKE_OMP_ACP_PAGINATE",
            if paginate_session_list { "1" } else { "0" },
        )
        .env(
            "FAKE_OMP_ACP_DELETE_NOOP",
            if delete_noop { "1" } else { "0" },
        )
        .env(
            "FAKE_OMP_ACP_PROMPT_ERROR",
            if prompt_error { "1" } else { "0" },
        )
        .env("HOME", cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    if archive_prompt {
        command.env("ACP_BRIDGE_SESSION_ARCHIVE_PROMPT", "/session delete");
    }

    let mut child = command.spawn().expect("spawn alleycat ACP bridge");
    let stdin = child.stdin.take().expect("bridge stdin");
    let stdout = child.stdout.take().expect("bridge stdout");
    let client = RpcClient {
        stdin,
        lines: BufReader::new(stdout).lines(),
        next_id: 1,
    };
    (child, client)
}

async fn initialize(client: &mut RpcClient) {
    let (response, _) = client
        .request(
            "initialize",
            json!({
                "clientInfo": { "name": "litter-e2e", "version": "test" },
                "capabilities": { "experimentalApi": true },
            }),
        )
        .await;
    assert!(
        response.get("result").is_some(),
        "initialize failed: {response}"
    );
}

#[tokio::test]
async fn archive_deletes_omp_session_from_thread_list_after_reconnect() {
    let temp_dir = TempDir::new().expect("temporary test directory");
    let (mut child, mut client) = spawn_bridge(temp_dir.path(), true, false, false, false).await;

    initialize(&mut client).await;

    let (start, _) = client
        .request("thread/start", json!({ "cwd": temp_dir.path() }))
        .await;
    let thread_id = start["result"]["thread"]["id"]
        .as_str()
        .expect("thread/start session id");
    assert_eq!(thread_id, "omp-e2e-session");

    let (listed, _) = client
        .request("thread/list", json!({ "archived": false }))
        .await;
    assert_eq!(listed["result"]["data"].as_array().map(Vec::len), Some(1));

    let (archived, notifications) = client
        .request("thread/archive", json!({ "threadId": thread_id }))
        .await;
    assert_eq!(archived["result"], json!({}), "archive failed: {archived}");
    assert!(
        notifications.iter().any(|frame| {
            frame.get("method").and_then(Value::as_str) == Some("thread/archived")
        })
    );

    child.kill().await.expect("stop first bridge");
    child.wait().await.expect("wait for first bridge");

    let (mut reconnected_child, mut reconnected_client) =
        spawn_bridge(temp_dir.path(), true, false, false, false).await;
    initialize(&mut reconnected_client).await;
    let (after_reconnect, _) = reconnected_client
        .request("thread/list", json!({ "archived": false }))
        .await;
    assert_eq!(after_reconnect["result"]["data"], json!([]));
    reconnected_child
        .kill()
        .await
        .expect("stop reconnected bridge");
    reconnected_child
        .wait()
        .await
        .expect("wait for reconnected bridge");
}

#[tokio::test]
async fn resume_uses_authoritative_omp_session_cwd() {
    let temp_dir = TempDir::new().expect("temporary test directory");
    let cwd = temp_dir.path().to_string_lossy().to_string();
    let (mut child, mut client) = spawn_bridge(temp_dir.path(), false, false, false, false).await;

    initialize(&mut client).await;

    let (start, _) = client
        .request("thread/start", json!({ "cwd": &cwd }))
        .await;
    let thread_id = start["result"]["thread"]["id"]
        .as_str()
        .expect("thread/start session id");

    let (listed, _) = client
        .request("thread/list", json!({ "archived": false }))
        .await;
    assert_eq!(listed["result"]["data"][0]["cwd"], cwd);

    let (resumed, _) = client
        .request(
            "thread/resume",
            json!({ "threadId": thread_id, "cwd": "/" }),
        )
        .await;
    assert!(
        resumed["result"].is_object(),
        "resume failed: {resumed}"
    );
    assert_eq!(resumed["result"]["cwd"], cwd);

    child.kill().await.expect("stop bridge");
    child.wait().await.expect("wait for bridge");
}
#[tokio::test]
async fn archive_follows_omp_session_list_cursor() {
    let temp_dir = TempDir::new().expect("temporary test directory");
    let (mut child, mut client) = spawn_bridge(temp_dir.path(), true, true, false, false).await;

    initialize(&mut client).await;
    let (start, _) = client
        .request("thread/start", json!({ "cwd": temp_dir.path() }))
        .await;
    let thread_id = start["result"]["thread"]["id"]
        .as_str()
        .expect("thread/start session id");

    let (archived, notifications) = client
        .request("thread/archive", json!({ "threadId": thread_id }))
        .await;
    assert_eq!(archived["result"], json!({}), "archive failed: {archived}");
    assert!(
        notifications.iter().any(|frame| {
            frame.get("method").and_then(Value::as_str) == Some("thread/archived")
        })
    );

    let (after_archive, _) = client
        .request("thread/list", json!({ "archived": false }))
        .await;
    assert_eq!(after_archive["result"]["data"], json!([]));
    child.kill().await.expect("stop bridge");
    child.wait().await.expect("wait for bridge");
}

#[tokio::test]
async fn archive_without_omp_strategy_remains_method_not_found() {
    let temp_dir = TempDir::new().expect("temporary test directory");
    let (mut child, mut client) = spawn_bridge(temp_dir.path(), false, false, false, false).await;

    initialize(&mut client).await;
    let (start, _) = client
        .request("thread/start", json!({ "cwd": temp_dir.path() }))
        .await;
    let thread_id = start["result"]["thread"]["id"]
        .as_str()
        .expect("thread/start session id");

    let (archived, notifications) = client
        .request("thread/archive", json!({ "threadId": thread_id }))
        .await;
    assert_eq!(
        archived["error"]["code"],
        json!(-32601),
        "unexpected archive response: {archived}"
    );
    assert!(
        !notifications.iter().any(|frame| {
            frame.get("method").and_then(Value::as_str) == Some("thread/archived")
        })
    );
    child.kill().await.expect("stop bridge");
    child.wait().await.expect("wait for bridge");
}

#[tokio::test]
async fn archive_rejects_consumed_delete_when_session_remains() {
    let temp_dir = TempDir::new().expect("temporary test directory");
    let (mut child, mut client) = spawn_bridge(temp_dir.path(), true, false, true, false).await;

    initialize(&mut client).await;
    let (start, _) = client
        .request("thread/start", json!({ "cwd": temp_dir.path() }))
        .await;
    let thread_id = start["result"]["thread"]["id"]
        .as_str()
        .expect("thread/start session id");

    let (archived, notifications) = client
        .request("thread/archive", json!({ "threadId": thread_id }))
        .await;
    assert_eq!(
        archived["error"]["code"],
        json!(-32603),
        "unexpected archive response: {archived}"
    );
    assert!(
        archived["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("remained after archive prompt"))
    );
    assert!(
        !notifications.iter().any(|frame| {
            frame.get("method").and_then(Value::as_str) == Some("thread/archived")
        })
    );

    let (listed, _) = client
        .request("thread/list", json!({ "archived": false }))
        .await;
    assert_eq!(listed["result"]["data"].as_array().map(Vec::len), Some(1));
    child.kill().await.expect("stop bridge");
    child.wait().await.expect("wait for bridge");
}

#[tokio::test]
async fn failed_turn_emits_completion_resets_idle_and_persists_turn() {
    let temp_dir = TempDir::new().expect("temporary test directory");
    let (mut child, mut client) = spawn_bridge(temp_dir.path(), false, false, false, true).await;

    initialize(&mut client).await;
    let (start, _) = client
        .request("thread/start", json!({ "cwd": temp_dir.path() }))
        .await;
    let thread_id = start["result"]["thread"]["id"]
        .as_str()
        .expect("thread/start session id");

    let (turn, notifications) = client
        .request(
            "turn/start",
            json!({
                "threadId": thread_id,
                "input": [{ "type": "text", "text": "trigger failure" }],
            }),
        )
        .await;
    assert_eq!(turn["error"]["code"], json!(-32603));
    assert!(
        turn["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("fake prompt failure"))
    );

    let completed = notifications
        .iter()
        .find(|frame| frame.get("method").and_then(Value::as_str) == Some("turn/completed"))
        .expect("failed turn/completed notification");
    assert_eq!(completed["params"]["turn"]["status"], "failed");
    assert_eq!(
        completed["params"]["turn"]["error"]["type"],
        "internalError"
    );

    let idle = notifications
        .iter()
        .rev()
        .find(|frame| {
            frame.get("method").and_then(Value::as_str) == Some("thread/status/changed")
                && frame["params"]["status"]["type"] == "idle"
        })
        .expect("idle thread status notification");
    assert_eq!(idle["params"]["threadId"], thread_id);

    let (read, _) = client
        .request(
            "thread/read",
            json!({ "threadId": thread_id, "includeTurns": true }),
        )
        .await;
    assert_eq!(read["result"]["thread"]["turns"].as_array().map(Vec::len), Some(1));
    assert_eq!(read["result"]["thread"]["turns"][0]["status"], "failed");

    child.kill().await.expect("stop bridge");
    child.wait().await.expect("wait for bridge");
}
