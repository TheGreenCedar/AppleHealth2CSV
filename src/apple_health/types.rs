use crate::error::{AppError, Result};
use crate::record::RecordProjection;
use ahash::AHashMap;
use quick_xml::events::BytesStart;

/// Generic representation for any Apple Health XML element.
#[derive(Debug, Clone)]
pub struct GenericRecord {
    pub element_name: String,
    pub attributes: AHashMap<String, String>,
}

impl GenericRecord {
    pub fn from_xml(element: &BytesStart) -> Result<Self> {
        let element_name = String::from_utf8(element.name().as_ref().to_vec())
            .map_err(|e| AppError::ParseError(format!("Invalid element name: {}", e)))?;

        let attributes_iter = element.attributes();
        let (lower, _) = attributes_iter.size_hint();
        let mut attributes = AHashMap::with_capacity(lower);

        for attr in attributes_iter {
            let attr = attr
                .map_err(|e| AppError::ParseError(format!("Failed to parse attribute: {}", e)))?;

            let key = String::from_utf8(attr.key.as_ref().to_vec())
                .map_err(|e| AppError::ParseError(format!("Invalid attribute key: {}", e)))?;

            let value = if attr.value.as_ref().contains(&b'&') {
                attr.decode_and_unescape_value(element.decoder())
                    .map_err(|e| AppError::ParseError(format!("Invalid attribute value: {}", e)))?
                    .into_owned()
            } else {
                String::from_utf8(attr.value.into_owned())
                    .map_err(|e| AppError::ParseError(format!("Invalid attribute value: {}", e)))?
            };

            attributes.insert(key, value);
        }

        Ok(GenericRecord {
            element_name,
            attributes,
        })
    }
}

impl RecordProjection for GenericRecord {
    fn field_names(&self) -> impl Iterator<Item = &str> {
        self.attributes.keys().map(String::as_str)
    }

    fn field_value(&self, name: &str) -> Option<&str> {
        self.attributes.get(name).map(String::as_str)
    }
}
