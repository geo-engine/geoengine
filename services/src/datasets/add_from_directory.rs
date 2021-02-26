use std::{
    fs::{self, DirEntry, File},
    io::BufReader,
    path::PathBuf,
};

use geoengine_datatypes::util::Identifier;

use crate::datasets::storage::DataSetDb;
use crate::error::Result;
use crate::users::user::UserId;
use crate::util::user_input::UserInput;

use super::storage::DataSetDefinition;

pub async fn add_data_sets_from_directory<D: DataSetDb>(db: &mut D, file_path: PathBuf) {
    async fn add_data_set_definition_from_dir_entry<D: DataSetDb>(
        db: &mut D,
        entry: &DirEntry,
    ) -> Result<()> {
        let def: DataSetDefinition =
            serde_json::from_reader(BufReader::new(File::open(entry.path())?))?;

        db.add_data_set(
            UserId::new(),
            def.properties.validated()?,
            db.wrap_meta_data(def.meta_data),
        )
        .await?; // TODO: add as system user

        Ok(())
    }

    let dir = fs::read_dir(file_path);
    if dir.is_err() {
        // TODO: log
        eprintln!("Skipped adding data sets from directory because it can't be read");
    }
    let dir = dir.expect("checked");

    for entry in dir {
        if let Ok(entry) = entry {
            if let Err(e) = add_data_set_definition_from_dir_entry(db, &entry).await {
                // TODO: log
                eprintln!(
                    "Skipped adding data set from directory entry: {}",
                    e.to_string()
                );
            }
        } else {
            // TODO: log
            eprintln!("Skipped adding data set from directory entry: {:?}", entry);
        }
    }
}
