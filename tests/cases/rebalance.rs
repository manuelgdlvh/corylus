use corylus::CorylusResult;
use uuid::Uuid;

use crate::Instance;
use crate::{new_instance, wait_until_ready};

pub async fn should_transfer_partition_ownership_after_rebalance(
    instance_1: Instance,
    instance_2: Instance,
) -> CorylusResult<()> {
    let map = instance_1
        .get_map::<String, String>("str-str")
        .expect("fixture registers str-str map");

    // Local write without repl
    map.put("key-7".to_string(), "value-1".to_string()).await?;

    let instance_3 = new_instance(Uuid::from_u128(3), 8093, Default::default())?;

    // Data for key-7 was transfered to Instance3
    wait_until_ready(&[&instance_1, &instance_2, &instance_3]);

    drop(instance_1);
    drop(instance_2);

    wait_until_ready(&[&instance_3]);

    // Local read after partition rebalance
    let map = instance_3
        .get_map::<String, String>("str-str")
        .expect("fixture registers str-str map");
    assert_eq!(
        Some("value-1".to_string()),
        map.get("key-7".to_string()).await?
    );

    Ok(())
}
