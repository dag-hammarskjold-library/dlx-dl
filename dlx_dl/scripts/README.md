## script descriptions

All of these Python files can be run as Python scripts form source. `export.py` and `sync.py` are also installed as command line programs when installing dlx-dl into a virtual environment. See main README for usage.

### export.py

Exports whole records that match the given citeria.

### sync.py

Compares records between the two systems, and updates only the fields that are different.

### retro.py

Runs `sync.py` over a potentially large range of IDs during non-business hours. This is intended to compare and update any records that may not have been properply updated in UNDL in the past for whatever reason, and have not been updated in dlx recently. It manages the sync runs in batches so that they do not overwhelm the UNDL APIs.
> [!NOTE]
> This was succesffuly run on the whole database (both bibs and auths) over the course of a few weeks in Spring 2025

### find_undeleted.py

Writes a report of records that have been deleted in dlx but are still in UNDL
> [!NOTE]
> This was run in Summer 2024, and action was taken on the report to delete the relevant records from UNDL. Since then, functionality has been added to `sync.py` that automatically deletes records in UNDL when they are deleted in dlx
