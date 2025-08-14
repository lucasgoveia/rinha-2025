use bytes::BytesMut;
use std::error::Error;
use std::fmt;
use tokio_postgres::types::{IsNull, ToSql, Type};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ProcessorType {
    Default,
    Fallback,
}

impl fmt::Display for ProcessorType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ProcessorType::Default => write!(f, "default"),
            ProcessorType::Fallback => write!(f, "fallback"),
        }
    }
}

impl ToSql for ProcessorType {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let s = match self {
            ProcessorType::Default => "default",
            ProcessorType::Fallback => "fallback",
        };
        s.to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool
    where
        Self: Sized,
    {
        ty.name() == "service_type" || ty == &Type::ANYENUM
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        if !Self::accepts(ty) {
            return Err(Box::new(tokio_postgres::types::WrongType::new::<Self>(
                ty.clone(),
            )));
        }
        self.to_sql(ty, out)
    }
}
