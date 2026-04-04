use std::io;

use corylus::Instance;

pub fn should_register_map_successfully(
    instance_1: Instance,
    instance_2: Instance,
) -> io::Result<()> {
    assert!(instance_1.get_map::<String, String>("str-str").is_some());
    assert!(instance_2.get_map::<String, String>("str-str").is_some());

    assert!(instance_1.get_map::<String, String>("wrong").is_none());
    assert!(instance_2.get_map::<String, String>("wrong").is_none());

    assert!(instance_1.get_map::<u64, String>("str-str").is_none());
    assert!(instance_2.get_map::<String, u64>("str-str").is_none());

    Ok(())
}

pub fn should_put_and_get_map_successfully(
    instance_1: Instance,
    instance_2: Instance,
) -> io::Result<()> {
    let map_1 = instance_1.get_map::<String, String>("str-str").unwrap();
    let map_2 = instance_2.get_map::<String, String>("str-str").unwrap();

    // Local write
    map_1.put("key-1".to_string(), "value-1".to_string())?;
    // Local read
    assert_eq!(Some("value-1".to_string()), map_1.get("key-1".to_string())?);
    // Remote read
    assert_eq!(Some("value-1".to_string()), map_2.get("key-1".to_string())?);

    // Remote Write
    map_1.put("key-2".to_string(), "value-2".to_string())?;
    // Remote read
    assert_eq!(Some("value-2".to_string()), map_1.get("key-2".to_string())?);
    // Local read
    assert_eq!(Some("value-2".to_string()), map_2.get("key-2".to_string())?);

    Ok(())
}
