
### Installation 
```bash
pip install git+https://github.com/dag-hammarskjold-library/dlx-dl@<latest version>
```

### Usage
From the command line:
```bash
dlx-dl-export --help
```

```bash
dlx-dl-sync --help
```

From Python:
```python
from dlx_dl.scripts import export, sync

export.run(help=True)
sync.run(help=True)
```

### Notes
* These scripts can be run from the command line for ad hoc operations, or as Python functions for use in scripts or AWS Lambda.
* When submitting records to DL using the API, the result is printed to STDOUT.
* Only exports using the API are logged in the database

#### Running as Python function

To run the scripts as Python functions, import the scripts as modules from `dlx_dl.scripts` and pass the arguments specified in --help to the `run()` function as normal Python keyword arguments.

Python:
```Python
from dlx_dl.scripts import export

export.run(source='export_id', type='bib', id=1, xml='output.xml')

export.run(source='export_id', type='bib', id=1, use_api=True)
```

### Command line examples
> Preview (display in console) records that meet export criteria and quit
```bash
$ dlx-dl-export --source=export_id --type=bib --modified_within=3600 --preview
```

> Write single record to DL by ID
```bash
$ dlx-dl-export --source=export_id --type=bib --id=1 --use_api
```

> Write records to DL from a list of IDs
```bash
$ dlx-dl-export --source=export_id --type=bib --list=ids.txt --use_api
```

> Write records to file
```bash
$ dlx-dl-export --source=export_id --type=bib --ids 1 2 3 --xml=output.xml
```

#### Batch API submission

The `--batch` flag submits all matching records to the UNDL API in a single request instead of one record at a time. Results are delivered by email rather than a per-record callback URL, so `--email` is required.

```bash
$ dlx-dl-export --source=export_id --type=bib --modified_within=86400 --use_api --batch --email=you@example.com
```

The `--batch_size` option (used together with `--batch`) splits the records into chunks and submits each chunk as a separate API request. This avoids creating a single oversized payload when a large number of records have been updated.

```bash
# Submit records in chunks of 500
$ dlx-dl-export --source=export_id --type=bib --modified_within=86400 --use_api --batch --batch_size=500 --email=you@example.com
```

Progress is printed to STDOUT as each chunk is submitted:

```
Submitting batch 1 (500 records)
Submitting batch 2 (500 records)
Submitting batch 3 (243 records)
```

The same options are available when calling `export.run()` from Python:

```python
export.run(
    source='export_id',
    type='bib',
    modified_within=86400,
    use_api=True,
    batch=True,
    batch_size=500,
    email='you@example.com',
)
```

#### other scripts

https://github.com/dag-hammarskjold-library/dlx-dl/blob/main/dlx_dl/scripts



