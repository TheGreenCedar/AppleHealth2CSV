pub trait RecordProjection {
    fn field_names(&self) -> impl Iterator<Item = &str>;

    fn field_value(&self, name: &str) -> Option<&str>;
}
