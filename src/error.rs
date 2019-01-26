use std::fmt;


#[derive(Debug)]
pub enum InternalError {
    Other(String),
}

impl std::error::Error for InternalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> { None }
}

impl fmt::Display for InternalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            InternalError::Other(s) => write!(f, "{}", s)
        }
    }
}
