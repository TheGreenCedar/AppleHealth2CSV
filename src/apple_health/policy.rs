use crate::apple_health::types::GenericRecord;
use crate::transform::TransformPolicy;

#[derive(Debug, Clone, Copy, Default)]
pub struct AppleHealthTransformPolicy;

impl TransformPolicy<GenericRecord> for AppleHealthTransformPolicy {
    fn group_key(&self, record: &GenericRecord) -> String {
        if record.element_name == "Record"
            && let Some(typ) = record.attributes.get("type")
        {
            return typ.clone();
        }

        record.element_name.clone()
    }

    fn sort_key<'a>(&self, record: &'a GenericRecord) -> Option<&'a str> {
        const SORT_KEYS: [&str; 7] = [
            "startDate",
            "date",
            "dateComponents",
            "creationDate",
            "endDate",
            "dateIssued",
            "receivedDate",
        ];

        for key in SORT_KEYS {
            if let Some(value) = record.attributes.get(key) {
                return Some(value.as_str());
            }
        }

        None
    }
}
