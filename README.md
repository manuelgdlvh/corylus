# corylus

`corylus` is a Rust library for **embedded distributed objects**, inspired by **Hazelcast**. It runs a small cluster of nodes that share **partitioned, replicated objects**. The current main object is a `DistributedMap<K, V>` (a distributed key/value map).

## Quick start (single node + a distributed map)

`corylus` is runtime-agnostic: you pass a runtime builder that can spawn async tasks (for example, a small Tokio adapter).

```rust
use std::net::{Ipv4Addr, SocketAddr};

use corylus::instance;
use corylus::network::{self, Discovery};
use corylus::object::ReplicationConfig;
use uuid::Uuid;

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 8090));

    let instance = instance::Builder::new()
        .with_id(Uuid::from_u128(1))
        .with_config(instance::Config {
            network: network::Config {
                addr,
                ..Default::default()
            },
            ..Default::default()
        })
        .with_discovery(Discovery::List {
            addresses: vec![addr],
        })
        .with_map::<String, String>("kv", ReplicationConfig::none())
        .build(/* your runtime builder */)?;

    let map = instance
        .get_map::<String, String>("kv")
        .expect("map not registered on this instance");

    map.put("hello".to_string(), "world".to_string()).await?;
    let v = map.get("hello".to_string()).await?;
    println!("hello => {v:?}");

    Ok(())
}
```

## Usage notes

- **Register objects first**: use `instance::Builder::with_map::<K, V>(name, repl_config)` before `build()`. After that, use `Instance::get_map::<K, V>(name)` to obtain a handle.
- **Discovery**:
  - `Discovery::List { addresses }` is the simplest: every node gets the same list of peer addresses.
  - `Discovery::Dns { host }` exists as an alternative (see `network::Discovery`).
- **Replication**: configure per object with `ReplicationConfig`:
  - `ReplicationConfig::none()`
  - `ReplicationConfig::synchronous(factor, repl_read)`
  - `ReplicationConfig::asynchronous(factor, repl_read)`

## Build & test

```bash
cargo test
```

