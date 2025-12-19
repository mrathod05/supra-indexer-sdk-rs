use diesel::{r2d2, PgConnection};
use diesel::r2d2::ConnectionManager;

pub mod block_heights;

pub type DbConnection = PgConnection;
pub type DbPool = r2d2::Pool<ConnectionManager<PgConnection>>;