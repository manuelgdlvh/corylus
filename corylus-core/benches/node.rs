use corylus_core::operation::ReadOperation;
use corylus_core::state_machine::RaftStateMachine;
use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Default)]
struct ReadOp;
impl ReadOperation<InMemoryRaftStateMachine> for ReadOp {
    type Output = String;
    fn execute(&self, state: &InMemoryRaftStateMachine) -> Option<Self::Output> {
        let result = state.values.get("test")?;
        Some(result.to_string())
    }

    fn serialize(&self) -> Vec<u8> {
        todo!()
    }
}

#[derive(Default)]
struct InMemoryRaftStateMachine {
    values: HashMap<String, String>,
}
impl RaftStateMachine for InMemoryRaftStateMachine {}

fn node_benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_suite_test");

    let num_reads_list = [1000, 10000, 100000, 1000000];
    for num_reads in num_reads_list {
        let input = num_reads;
        let parameter = format!("Sequential Reads: {}", num_reads);

        let group_ref = group
            .sample_size(10)
            .measurement_time(Duration::from_secs(30))
            .noise_threshold(0.05)
            .sampling_mode(SamplingMode::Flat)
            .throughput(Throughput::Elements(num_reads));

        group_ref.bench_with_input(
            BenchmarkId::new("sequential_reads", parameter.as_str()),
            &input,
            |b, &num_reads| {

                /*
                let sm = InMemoryRaftStateMachine::default();
                let rl = InMemoryRaftLog::new();
                let l_proxy = NoOpRaftPeerProxy::default();

                let node = RaftNode::new(sm, rl, l_proxy).unwrap();
                let handle = node.start();

                b.iter(|| {
                    (0..num_reads).for_each(|_| {
                        handle.read(ReadOp::default()).blocking_recv().expect("");
                    })
                });
                 */
            },
        );
    }

    group.finish();
}

criterion_group!(benches, node_benches);
criterion_main!(benches);
