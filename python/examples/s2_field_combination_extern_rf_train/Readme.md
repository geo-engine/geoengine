# Readme

This folder includes an example how to use the Geo Engine to process and provide data for training a random forest model in Python.

The notebook [nrw_crop_extern_s2_workflow_to_datasets_with_timeshift.ipynb](nrw_crop_extern_s2_workflow_to_datasets_with_timeshift.ipynb) shows an example how to train an external Random-Forest model using data from the Geo Engine.
In this example, we use Sentinel-2 data to train a model that predicts the field-use type in North Rhine-Westphalia, Germany.

The Sentinel-2 data is loaded from the Geo Engine and the field-use type is prepared using the [nrw_crop_extern_prep_data.ipynb](nrw_crop_extern_prep_data.ipynb) notebook and then uploaded into the Geo Engine.
For this example the EuroCrops dataset, which is avalable from [here](https://github.com/maja601/EuroCrops#vectordata_zenodo).

The notebook [nrw_crop_extern_s2_workflow_to_datasets_with_timeshift.ipynb](nrw_crop_extern_s2_workflow_to_datasets_with_timeshift.ipynb) also shows how to provide Sentinel-2 data for application of the trained Random-Forest model.
