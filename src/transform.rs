use crate::core::Processable;

pub trait TransformPolicy<T: Processable>: Send + Sync {
    fn group_key(&self, record: &T) -> String;

    fn sort_key<'a>(&self, record: &'a T) -> Option<&'a str> {
        let _ = record;
        None
    }

    fn include(&self, record: &T) -> bool {
        let _ = record;
        true
    }
}
