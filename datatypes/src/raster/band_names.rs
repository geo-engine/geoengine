use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::error::{
    DuplicateNameNotAllowed, DuplicateSuffixesNotAllowed, EmptyNameNotAllowed,
    InvalidNumberOfNewNames, InvalidNumberOfSuffixes,
};
use crate::util::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type", content = "values")]
pub enum RenameBands {
    Default,             // append " (n)" to the band name for the `n`-th conflict,
    Suffix(Vec<String>), // A suffix for every input, to be appended to the original band names
    Rename(Vec<String>), // A new name for each band, to be used instead of the original band names
}

impl RenameBands {
    pub fn validate(&self, num_inputs: usize, names_per_input: &[usize]) -> Result<()> {
        match self {
            Self::Default => {}
            RenameBands::Suffix(suffixes) => {
                let unique_suffixes: std::collections::HashSet<_> = suffixes.iter().collect();
                ensure!(
                    unique_suffixes.len() == suffixes.len(),
                    DuplicateSuffixesNotAllowed
                );

                ensure!(
                    suffixes.len() == num_inputs,
                    InvalidNumberOfSuffixes {
                        expected: num_inputs,
                        found: suffixes.len()
                    }
                );
            }
            RenameBands::Rename(names) => {
                ensure!(names.iter().all(|s| !s.is_empty()), EmptyNameNotAllowed);

                let unique_names: std::collections::HashSet<_> = names.iter().collect();
                ensure!(unique_names.len() == names.len(), DuplicateNameNotAllowed);

                ensure!(
                    names.len() == names_per_input.iter().sum::<usize>(),
                    InvalidNumberOfNewNames {
                        expected: num_inputs,
                        found: names.len()
                    }
                );
            }
        }

        Ok(())
    }

    pub fn apply(&self, mut inputs: Vec<Vec<String>>) -> Result<Vec<String>> {
        self.validate(
            inputs.len(),
            &inputs.iter().map(Vec::len).collect::<Vec<_>>(),
        )?;

        let mut new_names = vec![];

        match self {
            Self::Default => {
                new_names = inputs.remove(0);
                for input in inputs {
                    for name in input {
                        let mut new_name = name.clone();

                        let mut i = 1;

                        while new_names.contains(&new_name) {
                            new_name = format!("{name} ({i})");
                            i += 1;
                        }

                        new_names.push(new_name);
                    }
                }
            }
            RenameBands::Suffix(suffixes) => {
                for (i, input) in inputs.into_iter().enumerate() {
                    for name in input {
                        let new_name = format!("{}{}", name, suffixes[i]);
                        ensure!(!new_names.contains(&new_name), DuplicateNameNotAllowed);
                        new_names.push(new_name);
                    }
                }
            }
            RenameBands::Rename(names) => {
                new_names = names.clone();
            }
        }

        Ok(new_names)
    }
}
