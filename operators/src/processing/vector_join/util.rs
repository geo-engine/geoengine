use std::collections::{HashMap, HashSet};

/// Create a translation table to resolve name conflicts in the `DataCollection`
pub(super) fn translation_table<'i>(
    existing_column_names: impl Iterator<Item = &'i String>,
    new_column_names: impl Iterator<Item = &'i String>,
    right_column_suffix: &str,
) -> HashMap<String, String> {
    let mut existing_column_names: HashSet<String> = existing_column_names.cloned().collect();
    let mut translation_table = HashMap::new();

    for old_column_name in new_column_names {
        let mut new_column_name = old_column_name.clone();
        while existing_column_names.contains(&new_column_name) {
            new_column_name.push_str(right_column_suffix);
        }
        existing_column_names.insert(new_column_name.clone());
        translation_table.insert(old_column_name.clone(), new_column_name);
    }

    translation_table
}
