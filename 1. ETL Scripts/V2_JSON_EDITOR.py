#!/usr/bin/env python3
import json
import shutil
from copy import deepcopy
from pathlib import Path

PREG_KEY = "PREGNANCIES1.csv"


def get_cdm(rules):
    return rules["cdm"] if isinstance(rules.get("cdm"), dict) else rules


def translate_obs_to_condition(obs_entry):
    out = {}

    # copy person mapping
    if "person_id_mapping" in obs_entry:
        out["person_id_mapping"] = deepcopy(obs_entry["person_id_mapping"])

    # EXACT date mapping requested
    out["date_mapping"] = {
        "source_field": "PREGNANCY_START_DATE",
        "dest_field": [
            "condition_end_datetime",
            "condition_start_datetime",
        ]
    }

    # translate concept mappings
    cm_in = obs_entry.get("concept_mappings", {})
    cm_out = {}

    for src_field, block in cm_in.items():
        new_block = {}

        for k, v in block.items():
            if k == "original_value":
                new_block["original_value"] = "condition_source_value"
                continue

            if isinstance(v, dict):
                translated = {}
                for dest_field, dest_val in v.items():
                    if dest_field == "observation_concept_id":
                        translated["condition_concept_id"] = dest_val
                    elif dest_field == "observation_source_concept_id":
                        translated["condition_source_concept_id"] = dest_val
                    elif dest_field == "observation_source_value":
                        translated["condition_source_value"] = dest_val
                    else:
                        translated[dest_field] = dest_val
                new_block[k] = translated
            else:
                new_block[k] = v

        cm_out[src_field] = new_block

    out["concept_mappings"] = cm_out
    return out


def main():
    # Prompt for input file
    input_path = input("Enter full path to V2 rules JSON file: ").strip()
    in_file = Path(input_path)

    if not in_file.exists():
        raise FileNotFoundError(f"File not found: {in_file}")

    # Build output file name
    if in_file.suffix.lower() == ".json":
        out_file = in_file.with_name(f"{in_file.stem}_Edited{in_file.suffix}")
    else:
        out_file = in_file.with_name(f"{in_file.name}_Edited")

    shutil.copy2(in_file, out_file)

    # Load JSON
    with out_file.open("r", encoding="utf-8") as f:
        rules = json.load(f)

    cdm = get_cdm(rules)

    # Remove pregnancy from observation
    obs = cdm.get("observation")
    if not isinstance(obs, dict) or PREG_KEY not in obs:
        raise KeyError(f"Could not find cdm['observation']['{PREG_KEY}']")

    preg_obs = obs.pop(PREG_KEY)

    # Ensure condition_occurrence exists
    if "condition_occurrence" not in cdm:
        cdm["condition_occurrence"] = {}

    # Add pregnancy condition block
    cdm["condition_occurrence"][PREG_KEY] = translate_obs_to_condition(preg_obs)

    # Write file
    with out_file.open("w", encoding="utf-8") as f:
        json.dump(rules, f, indent=2, ensure_ascii=False)

    print(f"\nEdited JSON written to:\n{out_file}")

    # Print pregnancy block for verification
    print("\nResulting pregnancy condition block:")
    print(json.dumps(cdm["condition_occurrence"][PREG_KEY], indent=2))


if __name__ == "__main__":
    main()
