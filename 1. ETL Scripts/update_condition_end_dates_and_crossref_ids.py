#!/usr/bin/env python3
"""
============================================================================
Pregnancy Reference Builder & Condition Occurrence End Date Updater
Uses PyArrow for memory-efficient, batched processing of large files.
============================================================================

WHAT THIS SCRIPT DOES:
1. Builds preg_reference.tsv by joining: person_ids.tsv + PREGNANCIES1.csv
    + condition_occurrence.tsv on (PERSON_ID -> person_id) then, within rows
    where condition_concept_id = 4299535, 
    (PREGNANCY_START_DATE = condition_start_date).
2. Writes cond_occ_end_dates.tsv (copy of condition_occurrence.tsv) with 
    condition_end_date and condition_end_datetime updated from 
    PREGNANCY_END_DATE WHERE condition_concept_id = 4299535.

USAGE:
Run from Anaconda prompt: python preg_reference_builder.py
The script will ask if directories need updating before processing.
"""

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.compute as pc
import os
import re
import sys
import time
from datetime import datetime

# ==========================================================================
# DIRECTORY CONFIGURATION
# Change these by running the script and answering 'Y', or edit directly 
# here. File names must not be changed.
# ==========================================================================
TSV_DIR = r"<<INSERT FILE PATH HERE FOR OMOP TABLES>>/outputs"
CSV_DIR = r"<<INSERT FILE PATH HERE FOR ANY INTERIM, PSEUDONYMISED TABLES>>
# TSV files are the output transformed tables. CSV files are those files
# which were to be transformed.
# Code assumes that original identifiers have been pseudonymised BEFORE 
# transformation from their original content.
# ==========================================================================

# --- File names (do not change) ---
PERSON_IDS_FILE  = "person_ids.tsv"
COND_OCC_FILE    = "condition_occurrence.tsv"
PREG_FILE        = "PREGNANCIES1.csv"
PREG_REF_OUTPUT  = "preg_reference.tsv"
COND_OCC_UPDATED = "cond_occ_end_dates.tsv"

# Concept ID that flags pregnancy rows in condition_occurrence
PREGNANCY_CONCEPT_ID = 4299535

# Block size for streaming the large files (tune if memory is tight)
BLOCK_SIZE_MB 64

# ==========================================================================
# DIRECTORY MANAGEMENT
# ==========================================================================

def _update_script_directories(new_tsv: str, new_csv: str) -> None:
    """Rewrite the TSV_DIR / CSV_DIR constants in this script file."""
    try:
        script_path = os.path.abspath(_file_)
    except NameError:
        print(" [Note] Cannot auto-update script: not running from a .py file.")
        return
    
    with open(script_path, "r", encoding="utf-8") as fh:
        src = fh.read()
        
    src = re.sub(r'(TSV_DIR\s*=\s*r?)["\'].*?["\']', f'\\1r"{new_tsv}"', src)
    src = re.sub(r'(CSV_DIR\s*=\s*r?)["\'].*?["\']', f'\\1r"{new_csv}"', src)
    
    with open(script_path, "w", encoding="utf-8") as fh:
        fh.write(src)
        
    print("  Script file updated with new directories.")
    
def configure_directories() -> tuple:
    """Prompt user about directory changes; optionally rewrite script."""
    global TSV_DIR, CSV_DIR
    
    sep = "=" * 65
    print(f"\n{sep}")
    print(" DIRECTORY CONFIGURATION")
    print(sep)
    print(f" TSV directory: {TSV_DIR}")
    print(f" CSV directory: {CSV_DIR}")
    print(sep)
    
    while True:
        ans = input("\n Change directories? (Y/N): ").strip().upper()
        if ans in ("Y", "N"):
            break
        print(" Please enter Y or N.")

    if ans == "Y":
        new_tsv = input(f"\n New TSV directory [Enter = keep '{TSV_DIR}']: ").strip()
        new_csv = input(f" New CSV directory [Enter = keep '{CSV_DIR}']: ").strip()
        
        if new_tsv:
            TSV_DIR = new_tsv.rstrip("/\\")
        if new_csv:
            CSV_DIR = new_csv.rstrip("/\\")
            
        _update_script_directories(TSV_DIR, CSV_DIR)
        print(f"\n Directories confirmed:")
        print(f" TSV: {TSV_DIR}")
        print(f" CSV: {CSV_DIR}")
    
    return TSV_DIR, CSV_DIR

def _check_files(*paths: str) -> None:
    """Abort with a clear message if any required file is missing."""
    missing = [p for p in paths if not os.path.exists(p)]
    if missing:
        print("\n ERROR required file(s) not found: ")
        for m in missing:
            print(f" {m}")
        sys.exit(1)

# ==========================================================================        
# PROGRESS DISPLAY
# ==========================================================================

class Progress:
    """
    Lightweight terminal progress indicator.
    Prints a rolling status line every interval seconds.
    Shows an ASCII bar when file_bytes is supplied.
    """

    def __init__(self, label: str, file_bytes: int = 0, interval: float = 5.0):
        self.label = label
        self.file_bytes = file_bytes
        self.interval = interval
        self.rows = 0
        self.batches = 0
        self.start = time.time()
        self.last_print = self.start
        print(f"\n ─ {label} {'─' * 22}")

    def update(self, n_rows: int) -> None:
        self.rows += n_rows
        self.batches += 1
        now = time.time()
        if now - self.last_print >= self.interval:
            self._render (now)
            self.last_print = now

    def _render(self, now: float = None) -> None:
        now = now or time.time()
        elapsed = max(now - self.start, 1e-9)
        rate = self.rows / elapsed

        line = (f"\r Rows: {self.rows:>12,} ☻ "
                f"Batches: {self.batches: >4} ☻ "
                f"Speed: {rate:>9,.0f} rows/s")
        
        if self.file_bytes:
            # Estimate fraction from rows processed vs file size
            # (not byte-exact, but good enough for large files)
            pct = min(self.batches * BLOCK_SIZE_MB * 1024 * 1024
                      / self.file_bytes * 100, 99.0)
            filled = int(pct / 5)
            bar    = "_" * filled + "░" * (20 - filled)
            line  += f" ☻ [{bar}] {pct:4.1f}%"

        print(line, end="", flush=True)

    def finish(self) -> None:
        elapsed = time.time() - self.start
        self._render()
        print(f"\n Done: {self.rows:,} rows in {elapsed:.1f}s\n")

# ==========================================================================   
# PYARROW HELPERS
# ==========================================================================

def _tsv_parse_opts() -> pa_csv.ParseOptions:
    return pa_csv.ParseOptions(delimiter="\t")

def _stream_read_opts() -> pa_csv.ReadOptions:
    return pa_csv.ReadOptions(block_size=BLOCK_SIZE_MB * 1_048_576)

def _write_opts() -> pa_csv.WriteOptions:
    return pa_csv.WriteOptions (delimiter="\t")

def read_small_tsv(path: str) -> pa. Table:
    return pa_csv.read_csv(path, parse_options=_tsv_parse_opts())

def write_tsv(table: pa.Table, path: str) -> None:
    with pa_csv.CSVWriter(path, table.schema, write_options=_write_opts()) as w:
        w.write_table(table)

# ==========================================================================
# PHASE 1 - Build lookup structures from the small reference files
# ==========================================================================

def build_lookups (person_ids_path: str, preg_path: str) -> dict:
    """
    Returns a dict (person_id_int, start_date) -> {pregnancy_id, PERSON_ID, end_date}
    using person_ids.tsv to map SOURCE_SUBJECT -> TARGET_SUBJECT.
    """
    # ---- person_ids ------------------------------------------------------
    print("\n Reading person_ids.tsv")
    pid_tbl = read_small_tsv(person_ids_path)
    
    id_map: dict = dict(zip(
        pid_tbl.column("SOURCE_SUBJECT").to_pylist(), # str
        pid_tbl.column("TARGET_SUBJECT").to_pylist(), # int64
        ))
    print (f" {len(id_map):, } person ID mappings loaded")

    # ---- PREGNANCIES1.csv ------------------------------------------------
    print("\n Reading PREGNANCIES1.csv &")
    preg_tbl = pa_csv.read_csv(preg_path)
    print (f" {preg_tbl.num_rows:,} pregnancy records loaded")
    
    # Convert PERSON_ID (str) -> person_id (int64) via id_map
    pid_str_list = preg_tbl.column("PERSON_ID").to_pylist()
    pid_int_list = [id_map.get(s) for s in pid_str_list]
    
    preg_tbl = preg_tbl.append_column(
        "person_id",
        pa.array(pid_int_list, type=pa.int64()),
        )
    
    # Drop rows without a mapping
    valid_mask = pc.is_valid(preg_tbl.column("person_id"))
    n_before = preg_tbl.num_rows
    preg_tbl = preg_tbl.filter(valid_mask)
    n_drop = n_before - preg_tbl.num_rows
    print(f" {preg_tbl.num_rows:,} pregnancies matched "
          f"({n_drop:,} without person_id mapping excluded)")

    # ---- Build lookup dict ------------------------------------------------
    pids_int = preg_tbl.column("person_id").to_pylist()
    starts = preg_tbl.column("PREGNANCY_START_DATE").to_pylist() # datetime.date
    ends = preg_tbl.column("PREGNANCY_END_DATE").to_pylist()     # datetime.date
    preg_ids = preg_tbl.column("pregnancy_id").to_pylist()
    pids_str = preg_tbl.column("PERSON_ID").to_pylist()

    lookup: dict = {}
    for i in range(len(pids_int)):
        key = (pids_int[i], starts[i]) # (int64, datetime.date)
        lookup [key] = {
            "pregnancy_id" : preg_ids[i],
            "PERSON_ID" : pids_str[i],
            "PREGNANCY_END_DATE": ends[i],
        }
    print(f" Lookup built: {len(lookup):,} unique (person_id, start_date) keys")
    return lookup

# ==========================================================================
# PHASE 2 - Stream condition_occurrence to build preg_reference.tsv
# ==========================================================================

def build_preg_reference(cond_occ_path: str,
                         preg_ref_path: str,
                         preg_lookup: dict) -> dict:
    """
    Streams condition_occurrence.tsv in batches.

    For each batch, FIRST filters to rows where condition_concept_id = PREGNANCY_CONCEPT_ID (4299535)
    only those rows represent pregnancies and have valid condition_start_dates. THEN matches filtered
    rows to preg_lookup on (person_id, condition_start_date) to resolve pregnancy_id and 
    condition_occurrence_id.

    Writes preg_reference.tsv and returns: end_date_lookup: {condition_occurrence_id -> datetime.date}
    """
    file_bytes = os.path.getsize(cond_occ_path)
    progress = Progress("Scanning condition_occurrence.tsv (Phase 2)", file_bytes)
    
    # Accumulate output rows
    out: dict = {
        "PERSON_ID"              : [],
        "person_id"              : [],
        "pregnancy_id"           : [],
        "condition_occurrence_id": [],
        "PREGNANCY_START_DATE"   : [],
        "PREGNANCY_END_DATE"     : [],
        }
    
    reader = pa_csv.open_csv(
        cond_occ_path,
        parse_options=_tsv_parse_opts(),
        read_options=_stream_read_opts(),
    )
    
    while True:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            break
        
        # ---- KEY FIX: only consider pregnancy rows -------------------------
        # condition_start_date is only meaningful (non-blank) for rows with
        # condition_concept_id = 4299535. Matching across all rows would 
        # attempt lookups on null dates and produce wrong results.
        preg_mask = pc.equal(batch.column("condition_concept_id"),
                             PREGNANCY_CONCEPT_ID)
        preg_batch = batch.filter(preg_mask)
        
        if preg_batch.num_rows > 0:
            b_pids = preg_batch.column("person_id").to_pylist()
            b_dates = preg_batch.column("condition_start_date").to_pylist()
            b_cids = preg_batch.column("condition_occurrence_id").to_pylist()
            
            for j in range(len(b_pids)):
                info = preg_lookup.get((b_pids[j], b_dates[j]))
                if info is not None:
                    out["PERSON_ID"].append(info["PERSON_ID"])
                    out["person_id"].append(b_pids[j])
                    out["pregnancy_id"].append(info["pregnancy_id"])
                    out["condition_occurrence_id"].append(b_cids[j])
                    out["PREGNANCY_START_DATE"].append(b_dates[j])
                    out["PREGNANCY_END_DATE"].append(info["PREGNANCY_END_DATE"])
                    
        progress.update(batch.num_rows)

    progress.finish()

    # Build and write preg_reference table
    preg_ref_tbl = pa.table({
        "PERSON ID"    : pa.array(out["PERSON_ID"], pa.string()),
        "person_id"    : pa.array(out["person_id"], pa.int64()),
        "pregnancy_id" : pa.array(out["pregnancy_id"]),
        "condition_occurrence_id": pa.array(out["condition_occurrence_id"],  pa.int64()),
        "PREGNANCY_START_DATE"   : pa.array(out ["PREGNANCY_START_DATE"], pa.date32()),
        "PREGNANCY_END_DATE"     : pa.array(out["PREGNANCY_END_DATE"], pa.date32()),
    })
    
    print(f" Writing preg_reference.tsv ({preg_ref_tbl.num_rows:,} rows)")
    write_tsv(preg_ref_tbl, preg_ref_path)
    print(f" Saved {preg_ref_path}")
    
    # Compact end-date lookup for Phase 3
    end_date_lookup: dict = dict(zip(
        out["condition_occurrence_id"],
        out["PREGNANCY_END_DATE"],
    ))
    return end_date_lookup

# ==========================================================================
# PHASE 3 - Stream again, update end dates, write cond_occ_end_dates.tsv
# ==========================================================================

def write_updated_cond_occ(cond_occ_path: str,
                           out_path: str,
                           end_date_lookup: dict) -> int:
    """
    Streams condition_occurrence.tsv. For each row where condition_concept_id 
    == PREGNANCY_CONCEPT_ID, replaces condition_end_date and 
    condition_end_datetime from end_date_lookup. Writes the full file 
    (modified + unmodified rows) to out_path.
    
    Returns total number of rows updated.
    """
    file_bytes = os.path.getsize(cond_occ_path)
    progress = Progress("Writing cond_occ_end_dates.tsv (Phase 3)", file_bytes)
    total_updated = 0
    writer = None

    reader = pa_csv.open_csv(
        cond_occ_path,
        parse_options=_tsv_parse_opts(),
        read_options=_stream_read_opts(),           
    )

    while True:
        try:
            batch = reader.read_next_batch()
            except StopIteration:
            break

        tbl = pa.Table.from_batches([batch])
        
        # Identify rows where concept = 4299535
        concept_mask = pc.equal(tbl.column("condition_concept_id"),
                                PREGNANCY_CONCEPT_ID)
        n_candidates = pc.sum(concept_mask.cast(pa.int32())).as_py()
        
        if n_candidates > 0:
            cids = tbl.column("condition_occurrence_id").to_pylist()
            end_dates = tbl.column("condition_end_date").to_pylist()
            end_dts = tbl.column("condition_end_datetime").to_pylist()
            mask_list = concept_mask.to_pylist()
            
            n_updated_batch = 0
            for j in range(len(mask_list)):
                if mask_list[j]:
                    new_date = end_date_lookup.get(cids[j])
                    if new_date is not None:
                        end_dates[j] = new_date
                        end_dts[j] = datetime(new_date.year,
                                              new_date.month,
                                              new_date.day, 0, 0, 0)
                        n_updated_batch += 1

            if n_updated_batch > 0:
                # Replace the two end-date columns in this batch
                idx_d = tbl.schema.get_field_index("condition_end_date")
                idx_dt = tbl.schema.get_field_index("condition_end_datetime")
                tbl = tbl.set_column(idx_d, "condition_end_date",
                                     pa.array(end_dates, pa.date32()))
                tbl = tbl.set_column(idx_dt, "condition_end_datetime",
                                     pa.array (end_dts, pa.timestamp("s")))
                total_updated += n_updated_batch

            # Initialise writer on first batch (captures schema incl. null cols)
            if writer is None:
                writer = pa_csv.CSVWriter(out_path, tbl.schema,
                                          write_options=_write_opts())
                writer.write_table(tbl)
                progress.update(tbl.num_rows)

        if writer:
            writer.close()
            
        progress.finish()
        print(f" Saved -> {out_path}")
        return total_updated

# ==========================================================================
# ENTRY POINT
# ==========================================================================

def main() -> None:
    sep = "=" * 65
    
    # --- Configure directories
    tsv_dir, csv_dir = configure_directories()

    person_ids_path = os.path.join(tsv_dir, PERSON_IDS_FILE)
    cond_occ_path = os.path.join(tsv_dir, COND_OCC_FILE)
    preg_path os.path.join(csv_dir, PREG_FILE)
    preg_ref_path = os.path.join(tsv_dir, PREG_REF_OUTPUT)
    out_path = os.path.join(tsv_dir, COND_OCC_UPDATED)

_check_files(person_ids_path, cond_occ_path, preg_path)

# Phase 1
print(f"\n{sep}")
print(" PHASE 1 - Load reference files & build lookup")
print(sep)
to = time.time()

preg_lookup = build_lookups(person_ids_path, preg_path)

# ---Phase 2
print (f"\n{sep}")
print(" PHASE 2 - Build preg_reference.tsv")
print(sep)

end_date_lookup = build_preg_reference(cond_occ_path, preg_ref_path, preg_lookup)

# Phase 3
print(f"\n{sep}")
print(" PHASE 3 - Write cond_occ_end_dates.tsv (updating end dates)")
print(sep)

n_updated = write_updated_cond_occ(cond_occ_path, out_path, end_date_lookup)

# -Summary
elapsed = time.time() - t0
print(f"\n{sep}")
print(" ALL PHASES COMPLETE")
print(sep)
print (f" preg_reference.tsv : {len(end_date_lookup):,} rows written")
print (f" cond_occ_end_dates.tsv: end dates updated for "
       f"{n_updated:,} rows (concept_id = {PREGNANCY_CONCEPT_ID})")
print(f" Total elapsed time : {elapsed / 60:.1f} minutes")
print(sep)
print()

if __name__ == "__main__":
    main()