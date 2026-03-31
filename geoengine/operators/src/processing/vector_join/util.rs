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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_conflict() {
        let existing_columns: Vec<String> =
            ["foo", "bar"].iter().map(ToString::to_string).collect();
        let new_columns: Vec<String> = ["baz"].iter().map(ToString::to_string).collect();

        let translations =
            translation_table(existing_columns.iter(), new_columns.iter(), "_SUFFIX");

        assert_eq!(
            translations,
            [("baz", "baz")]
                .iter()
                .map(|&(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<String, String>>()
        );
    }

    #[test]
    fn simple_conflict() {
        let existing_columns: Vec<String> =
            ["foo", "bar"].iter().map(ToString::to_string).collect();
        let new_columns: Vec<String> = ["bar", "baz"].iter().map(ToString::to_string).collect();

        let translations =
            translation_table(existing_columns.iter(), new_columns.iter(), "_SUFFIX");

        assert_eq!(
            translations,
            [("bar", "bar_SUFFIX"), ("baz", "baz")]
                .iter()
                .map(|&(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<String, String>>()
        );
    }

    #[test]
    fn recursive_conflict() {
        let existing_columns: Vec<String> =
            ["foo", "bar"].iter().map(ToString::to_string).collect();
        let new_columns: Vec<String> = ["bar", "bar_SUFFIX"]
            .iter()
            .map(ToString::to_string)
            .collect();

        let translations =
            translation_table(existing_columns.iter(), new_columns.iter(), "_SUFFIX");

        assert_eq!(
            translations,
            [("bar", "bar_SUFFIX"), ("bar_SUFFIX", "bar_SUFFIX_SUFFIX")]
                .iter()
                .map(|&(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<String, String>>()
        );
    }
}
