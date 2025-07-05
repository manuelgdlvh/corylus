use corylus_core::handle::RaftNodeHandle;
use corylus_core::node::RaftNode;
use corylus_core::operation::{RaftCommand, ReadOperation, WriteOperation};
use corylus_core::peer::OpDeserializer;
use corylus_core::raft_log::InMemoryRaftLog;
use corylus_core::state_machine::RaftStateMachine;
use corylus_http_proxy::client::ReqwestHttpClient;
use corylus_http_proxy::server::ActixHttpServer;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

#[derive(Default, Clone, Copy)]
struct ReadOp;
impl ReadOperation<InMemoryRaftStateMachine> for ReadOp {
    type Output = u64;
    fn execute(&self, state: &InMemoryRaftStateMachine) -> Option<Self::Output> {
        Some(state.value)
    }

    fn serialize(&self) -> Vec<u8> {
        "READ".bytes().collect()
    }
}

#[derive(Default, Clone, Copy)]
struct IncrementOp;

impl WriteOperation<InMemoryRaftStateMachine> for IncrementOp {
    fn execute(&self, state: &mut InMemoryRaftStateMachine) {
        state.value += 1;
    }
    fn serialize(&self) -> Vec<u8> {
        "INCREMENT".bytes().collect()
    }
}

#[derive(Default, Clone, Copy)]
struct DecrementOp;

impl WriteOperation<InMemoryRaftStateMachine> for DecrementOp {
    fn execute(&self, state: &mut InMemoryRaftStateMachine) {
        state.value -= 1;
    }
    fn serialize(&self) -> Vec<u8> {
        "DECREMENT".bytes().collect()
    }
}

#[derive(Default, Clone, Copy)]
pub struct CustomDeserializer {}

impl OpDeserializer<InMemoryRaftStateMachine> for CustomDeserializer {
    fn deserialize(&self, data: &[u8]) -> Box<dyn WriteOperation<InMemoryRaftStateMachine>> {
        let str = String::from_utf8_lossy(data);
        if str.eq("DECREMENT") {
            Box::new(DecrementOp::default())
        } else {
            Box::new(IncrementOp::default())
        }
    }
}

#[derive(Default)]
struct InMemoryRaftStateMachine {
    value: u64,
}
impl RaftStateMachine for InMemoryRaftStateMachine {}

static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[tokio::test]
async fn should_replicate_operation_successfully() {
    let _guard = TEST_LOCK.lock().unwrap();

    let handle_1 = start_raft_node(8080);
    let handle_2 = start_raft_node(8081);
    let handle_3 = start_raft_node(8082);

    let num_of_messages = 10000;

    send_write_op(handle_1.clone(), num_of_messages, IncrementOp::default()).await;
    await_until_full_replicated(handle_2.clone(), ReadOp::default(), num_of_messages).await;
    await_until_full_replicated(handle_3.clone(), ReadOp::default(), num_of_messages).await;
    assert_eq!(
        Some(num_of_messages),
        handle_1.read(ReadOp::default()).recv().await.unwrap()
    );

    send_write_op(handle_1.clone(), num_of_messages, DecrementOp::default()).await;
    await_until_full_replicated(handle_2.clone(), ReadOp::default(), 0).await;
    await_until_full_replicated(handle_3.clone(), ReadOp::default(), 0).await;
    assert_eq!(
        Some(0),
        handle_1.read(ReadOp::default()).recv().await.unwrap()
    );

    let _ = handle_1.raft_command(RaftCommand::Stop).recv().unwrap();
    let _ = handle_2.raft_command(RaftCommand::Stop).recv().unwrap();
    let _ = handle_3.raft_command(RaftCommand::Stop).recv().unwrap();
}

// Write Blocking event loop. Write / Commands asynchronous?
#[tokio::test]
async fn should_forward_write_to_leader_successfully() {
    let _guard = TEST_LOCK.lock().unwrap();

    let handle_1 = start_raft_node(8080);
    let handle_2 = start_raft_node(8081);
    let handle_3 = start_raft_node(8082);

    // To avoid deadlock in write. Peer not found
    tokio::time::sleep(Duration::from_secs(5)).await;

    let num_of_messages = 10000;

    send_write_op(
        handle_2.clone(),
        num_of_messages / 2,
        IncrementOp::default(),
    )
    .await;
    send_write_op(
        handle_3.clone(),
        num_of_messages / 2,
        IncrementOp::default(),
    )
    .await;
    await_until_full_replicated(handle_1.clone(), ReadOp::default(), num_of_messages).await;
    await_until_full_replicated(handle_2.clone(), ReadOp::default(), num_of_messages).await;
    await_until_full_replicated(handle_3.clone(), ReadOp::default(), num_of_messages).await;

    send_write_op(
        handle_2.clone(),
        num_of_messages / 2,
        DecrementOp::default(),
    )
    .await;
    send_write_op(
        handle_3.clone(),
        num_of_messages / 2,
        DecrementOp::default(),
    )
    .await;
    await_until_full_replicated(handle_1.clone(), ReadOp::default(), 0).await;
    await_until_full_replicated(handle_2.clone(), ReadOp::default(), 0).await;
    await_until_full_replicated(handle_3.clone(), ReadOp::default(), 0).await;

    let _ = handle_1.raft_command(RaftCommand::Stop).recv().unwrap();
    let _ = handle_2.raft_command(RaftCommand::Stop).recv().unwrap();
    let _ = handle_3.raft_command(RaftCommand::Stop).recv().unwrap();
}

#[tokio::test]
async fn should_follower_apply_snapshot_successfully() {
    let _guard = TEST_LOCK.lock().unwrap();

    let handle_1 = start_raft_node(8080);

    let num_of_messages = 10000;
    send_write_op(
        handle_1.clone(),
        num_of_messages,
        IncrementOp::default(),
    )
    .await;

    await_until_full_replicated(handle_1.clone(), ReadOp::default(), num_of_messages).await;

    let handle_2 = start_raft_node(8081);
    let handle_3 = start_raft_node(8082);

    await_until_full_replicated(handle_2.clone(), ReadOp::default(), num_of_messages).await;
    await_until_full_replicated(handle_3.clone(), ReadOp::default(), num_of_messages).await;

    let _ = handle_1.raft_command(RaftCommand::Stop).recv().unwrap();
    let _ = handle_2.raft_command(RaftCommand::Stop).recv().unwrap();
    let _ = handle_3.raft_command(RaftCommand::Stop).recv().unwrap();
}

async fn send_write_op(
    handle: Arc<RaftNodeHandle<InMemoryRaftStateMachine>>,
    num_of_messages: u64,
    write_op: impl WriteOperation<InMemoryRaftStateMachine> + Clone + 'static,
) {
    for i in 0..(num_of_messages - 1) {
        if i % 250 == 0 {
            let _ = handle.write(write_op.clone()).recv().await;
            println!("{} messages were sent", i);
            continue;
        }
        handle.write(write_op.clone());
    }
    let _ = handle.write(write_op).recv().await;
}

async fn await_until_full_replicated(
    handle: Arc<RaftNodeHandle<InMemoryRaftStateMachine>>,
    read_op: impl ReadOperation<InMemoryRaftStateMachine, Output = u64> + Clone + 'static,
    target: u64,
) {
    loop {
        match handle.read(read_op.clone()).recv().await.unwrap() {
            None => {
                panic!("Did not receive any messages");
            }
            Some(val) => {
                if val == target {
                    break;
                }

                println!("Waiting to receive {} messages. Actual: {}", target, val);
                tokio::time::sleep(Duration::from_millis(5000)).await;
            }
        };
    }

    assert_eq!(Some(target), handle.read(read_op).recv().await.unwrap());
}

fn start_raft_node(port: u16) -> Arc<RaftNodeHandle<InMemoryRaftStateMachine>> {
    let sm = InMemoryRaftStateMachine::default();
    let rl = InMemoryRaftLog::new();
    let client_proxy = ReqwestHttpClient::default();
    let server_proxy = ActixHttpServer::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
    let deserializer = CustomDeserializer::default();

    let node = RaftNode::new(sm, deserializer, rl, client_proxy).unwrap();
    node.start(server_proxy, deserializer).unwrap()
}
