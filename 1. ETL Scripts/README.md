# ETL Scripts & Documents

These are a collection of tools and scripts that may be useful in the process of extraction, transformation and Loading (ETL) of raw birth cohort data into the Observational Medical Outcomes Partnership (OMOP) Common Data Model (CDM). You will find details of scripts used during pre-postprocessing where necessary to work around the limitations of the software and tools we used.

Additional information about our processes is included in our [protocol paper](https://ijpds.org/index.php/ijpds/article/view/2406).
A detailed, step-by-step methodology is continually updated at the following [protocol DOI](https://dx.doi.org/10.17504/protocols.io.36wgq2b3ogk5/v1).

## Files

### mireda_pseudonymisation.py
Before running data transformation on your files it is good practice to pseudonymise your IDs but the package recommended by Carrot struggles with large datasets. This script will pseudonymise your identifiers in chunks and will produce an output file which references the link between original identifiers and new hashed identifiers.

### V2_JSON_EDITOR.py
This script will move the pregnancy concept from the observation domain to the condition occurrence domain where it belongs in the JSON file because Carrot currently misclassifies it.

### update_condition_end_dates_and_crossref_ids.py
Carrot currently only assigns events to one date. This means that the same date is assigned to both condition start date and condition end date. This script will take the pregnancy end date from your untransformed date and apply it to the condition end date for pregnancy. May need adapting for your specific setup.
