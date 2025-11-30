# Coffee Shop Supplier ETL — Clear Student Instructions

Purpose
- Build a Python ETL that reads two daily CSVs, cleans and merges them, detects deltas across days, and maintains a supplier dimension CSV at `database/schema/dim_suppliers.csv`.

Quick overview
- Inputs: `data/<day>/suppliers.csv` and `data/<day>/supplier_details.csv`.
- Output: `database/schema/dim_suppliers.csv` (single CSV updated each run).
- Example days to process: `day1`, `day2`, `day3`.

Required features (explicit)
1. Read and clean the two source CSVs for a given day.
2. Merge them into a staging table.
3. Detect new and changed suppliers using `supplier_id` as the business key.
4. Implement both SCD behaviors together:
   - Type 2 for designated attributes (create new historical row + mark previous row inactive when these change).
   - Type 1 for designated attributes (overwrite the current row when these change).
   You must implement both behaviors in the same pipeline — Type 2 governs history for some columns while Type 1 governs others.

Project layout (what to use)
```
coffee_shop_etl_assignment/
├── data/                     # day1, day2, day3
├── database/
│   └── schema/
│       └── dim_suppliers.csv  # final dim CSV (your script updates this)
├── solution/
│   └── answer.py              # example (instructor)
└── assignment_instructions.md
```

Input columns you will see
- suppliers.csv: SupplierID, SupplierName, SupplierCategory, City, IsActive
- supplier_details.csv: SupplierID, Email, DeliveryFrequency, Notes

Output columns (required)
- supplier_key, supplier_id, supplier_name, supplier_category, city, email, delivery_frequency, notes, is_active, source_date, last_updated_at

SCD column mapping (required mapping)
- Type 2 (history — append new row if any change): supplier_name, supplier_category, city, delivery_frequency, email
- Type 1 (overwrite current row): notes, is_active

Why this mapping?
- Type 2 columns are stable attributes whose historical values matter for reporting/audit.
- Type 1 columns are noisy or represent current status; overwriting is simpler and appropriate here.

Recommended step-by-step implementation
1. Extract
   - Read both CSVs for the day into pandas DataFrames.
2. Clean
   - Strip leading/trailing whitespace from string columns.
   - Lowercase emails.
   - Convert IsActive to boolean.
3. Staging
   - Merge (inner join) suppliers + supplier_details on `SupplierID`.
   - Rename columns to the output names.
4. Add metadata
   - Add `source_date` (map day name to a date) and `last_updated_at` (now).
5. Load current dimension
   - Read `database/schema/dim_suppliers.csv` if it exists; otherwise, start empty.
   - Identify the active row for each `supplier_id` (row where `is_active == True`).
6. Delta detection + apply SCD rules (core logic)
   - New supplier IDs: append as new rows with new `supplier_key` and `is_active = True`.
   - For existing supplier IDs:
     a) If any Type 2 column differs (compare staging vs the active dim row) → Type 2 flow:
        - Set the existing active row `is_active = False` and update its `last_updated_at`.
        - Append a new row with the updated attribute values, a new `supplier_key`, and `is_active = True`.
     b) Else if any Type 1 column differs → Type 1 flow:
        - Update the active row in-place with the new Type 1 values and update `last_updated_at`.
     c) If both Type 2 and Type 1 differ, perform Type 2 and ensure the new appended row contains the new Type 1 values as well (so the current row reflects all changes).
   - (Optional) If a previously active supplier is missing from staging, you may set its `is_active = False`.

Decision logic (pseudocode)
```
TYPE2 = ["supplier_name","supplier_category","city","delivery_frequency","email"]
TYPE1 = ["notes","is_active"]

for id in intersection(staging_ids, dim_ids):
    if any(staging[id][c] != dim_active[id][c] for c in TYPE2):
        # Type 2: deactivate old active row; append new active row
    elif any(staging[id][c] != dim_active[id][c] for c in TYPE1):
        # Type 1: update active row in-place
```

Practical implementation hints
- supplier_key: when appending, set to `dim_df['supplier_key'].max() + 1` (or 1 if empty).
- Always deactivate the old row before appending a Type 2 row (so only one active row exists per supplier).
- Save CSV atomically (write to a temp file then replace) to avoid partial writes.
- Log counts each run: New, Type1-updates, Type2-updates, Deactivations.

How to run
```
python etl_pipeline.py day1
python etl_pipeline.py day2
python etl_pipeline.py day3
```