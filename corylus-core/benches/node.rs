use corylus_core::node::RaftNode;
use corylus_core::operation::ReadOperation;
use corylus_core::raft_log::InMemoryRaftLog;
use corylus_core::state_machine::StateMachine;
use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use std::collections::HashMap;

#[derive(Default)]
struct ReadOp;
impl ReadOperation<InMemoryStateMachine> for ReadOp {
    type Output = String;
    fn execute(&self, state: &InMemoryStateMachine) -> Option<Self::Output> {
        let result = state.values.get("test")?;
        Some(result.to_string())
    }
}

#[derive(Default)]
struct InMemoryStateMachine {
    values: HashMap<String, String>,
}
impl StateMachine for InMemoryStateMachine {}

fn node_benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_suite_test");

    let num_reads_list = [100, 1000, 10000, 100000];
    for num_reads in num_reads_list {
        let input = num_reads;
        let parameter = format!("Sequential Reads: {}", num_reads);

        let group_ref = group
            .sample_size(10)
            .noise_threshold(0.05)
            .sampling_mode(SamplingMode::Flat)
            .throughput(Throughput::Elements(num_reads));

        group_ref.bench_with_input(
            BenchmarkId::new("reads", parameter.as_str()),
            &input,
            |b, &num_reads| {
                let sm = InMemoryStateMachine::default();
                let rl = InMemoryRaftLog::new();
                let node = RaftNode::new(sm, rl).unwrap();
                let handle = node.start();

                b.iter(|| {
                    handle.read(ReadOp::default()).blocking_recv().expect("");
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, node_benches);
criterion_main!(benches);
