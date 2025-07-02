## script descriptions

All of these Python files can be run as Python scripts from source. They can also be imported as functions into other Python code. `export.py` and `sync.py` are also installed as command line programs when installing dlx-dl into a virtual environment. See main README for usage.

### export.py

Exports whole records that match the given citeria. The records can be exported as MARCXML to a file/STDOUT, or submitted directly to the UNDL submission API.

### sync.py

Compares records between the two systems that match the given criteria, and updates any records in UNDL that are different using the submission API run in "correct" mode. Only the fields that are different are updated. This process is also called to run on a schedule in AWS Lambda, which automates all updates to UNDL.

### retro.py

Runs `sync.py` over a potentially large range of IDs during non-business hours. This is intended to compare and update any records that may not have been properply updated in UNDL in the past for whatever reason, and have not been updated in dlx recently. It manages the sync runs in batches so that they do not overwhelm the UNDL APIs. It runs continuously until the last ID is reached, pausing during business hours in order not to interfere with normal operations.
> [!NOTE]
> This was succesffuly run on the whole database (both bibs and auths) over the course of a few weeks in Spring 2025

### find_undeleted.py

Writes a report of records that have been deleted in dlx but are still in UNDL
> [!NOTE]
> This was run in Summer 2024, and action was taken on the report to delete the relevant records from UNDL. Since then, functionality has been added to `sync.py` that automatically deletes records in UNDL when they are deleted in dlx
