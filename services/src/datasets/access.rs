use crate::identifier;

identifier!(DataAccessProviderId);

pub struct DataAccessProvider {
    pub id: DataAccessProviderId,
    pub uri: String,
}
