#[derive(Debug)]
pub enum QueryError {
    TupleNotFound,
    Locked,
}
