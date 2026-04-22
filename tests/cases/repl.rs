use corylus::CorylusResult;

use crate::Instance;

pub fn should_read_success_when_sync_repl_and_allow_replica_read(
    instance_1: Instance,
    instance_2: Instance,
) -> CorylusResult<()> {
    let map_1 = instance_1
        .get_map::<String, String>("str-str")
        .expect("fixture registers str-str map");

    // Local write with sync repl
    map_1.put("key-1".to_string(), "value-1".to_string())?;
    drop(instance_1);

    let map_2 = instance_2
        .get_map::<String, String>("str-str")
        .expect("fixture registers str-str map");
    assert_eq!(Some("value-1".to_string()), map_2.get("key-1".to_string())?);

    Ok(())
}

pub fn should_read_fail_when_sync_repl_and_no_allow_replica_read(
    instance_1: Instance,
    instance_2: Instance,
) -> CorylusResult<()> {
    let map_1 = instance_1
        .get_map::<String, String>("str-str")
        .expect("fixture registers str-str map");

    // Local write with sync repl
    map_1.put("key-1".to_string(), "value-1".to_string())?;
    drop(instance_1);

    let map_2 = instance_2
        .get_map::<String, String>("str-str")
        .expect("fixture registers str-str map");
    assert!(map_2.get("key-1".to_string()).is_err());

    Ok(())
}
