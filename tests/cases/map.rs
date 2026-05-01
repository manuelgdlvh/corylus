use corylus::CorylusResult;

use crate::Instance;

pub fn should_register_map_successfully(
    instance_1: Instance,
    instance_2: Instance,
) -> CorylusResult<()> {
    assert!(instance_1.get_map::<String, String>("str-str").is_some());
    assert!(instance_2.get_map::<String, String>("str-str").is_some());

    assert!(instance_1.get_map::<String, String>("wrong").is_none());
    assert!(instance_2.get_map::<String, String>("wrong").is_none());

    assert!(instance_1.get_map::<u64, String>("str-str").is_none());
    assert!(instance_2.get_map::<String, u64>("str-str").is_none());

    Ok(())
}

pub async fn should_put_and_get_map_successfully(
    instance_1: Instance,
    instance_2: Instance,
) -> CorylusResult<()> {
    let map_1 = instance_1
        .get_map::<String, String>("str-str")
        .expect("fixture registers str-str map");
    let map_2 = instance_2
        .get_map::<String, String>("str-str")
        .expect("fixture registers str-str map");

    // Local write
    map_1
        .put("key-1".to_string(), "value-1".to_string())
        .await?;
    // Local read
    assert_eq!(
        Some("value-1".to_string()),
        map_1.get("key-1".to_string()).await?
    );
    // Remote read
    assert_eq!(
        Some("value-1".to_string()),
        map_2.get("key-1".to_string()).await?
    );

    // Remote Write
    map_1
        .put("key-2".to_string(), "value-2".to_string())
        .await?;
    // Remote read
    assert_eq!(
        Some("value-2".to_string()),
        map_1.get("key-2".to_string()).await?
    );
    // Local read
    assert_eq!(
        Some("value-2".to_string()),
        map_2.get("key-2".to_string()).await?
    );

    Ok(())
}
