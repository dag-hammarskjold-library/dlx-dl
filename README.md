
### Installation 
```bash
pip install git+https://github.com/dag-hammarskjold-library/dlx-dl
```

### Usage
From the command line:
```bash
$ dlx-dl --help
```

From Python:
```python
import dlx_dl

dlx_dl.run(help=True)
```

```
usage: dlx-dl [-h] [--files_only] [--delete_only] [--preview] [--queue QUEUE] --source SOURCE --type {bib,auth}
              (--modified_within MODIFIED_WITHIN | --modified_since_log | --list LIST | --id ID | --ids IDS [IDS ...] | --query QUERY) (--xml XML | --use_api)
              [--connect CONNECT] [--api_key API_KEY] [--callback_url CALLBACK_URL] [--nonce_key NONCE_KEY]

optional arguments:
  -h, --help            show this help message and exit
  --files_only          only export records with new files
  --delete_only         only export records to delete
  --preview             list records that meet criteria and exit (boolean)
  --queue QUEUE         number of records at which to limit export and place in queue

required:
  --source SOURCE       an identity to use in the log
  --type {bib,auth}

criteria:
  one criteria argument is required

  --modified_within MODIFIED_WITHIN
                        export records modified within the past number of seconds
  --modified_since_log  export records modified since the last logged run from --source (boolean)
  --list LIST           file with list of IDs (max 5000)
  --id ID               a single record ID
  --ids IDS [IDS ...]   variable-length list of record IDs
  --query QUERY         JSON MongoDB query

output:
  one output argument is required

  --xml XML             write XML as batch to this file. use "STDOUT" to print in console
  --use_api             submit records to DL through the API

credentials:
  these arguments are automatically supplied by AWS SSM if AWS credentials are configured

  --connect CONNECT     MongoDB connection string
  --api_key API_KEY     UNDL-issued api key
  --callback_url CALLBACK_URL
                        A URL that can receive the results of a submitted task.
  --nonce_key NONCE_KEY
                        A validation key that will be passed to and from the UNDL API.
```

### Notes
* The program can be run from the command line for ad hoc operations, or as a Python function for use in scripts or AWS Lambda.
* When submitting records to DL using the API, the result is printed to STDOUT.
* Only exports using the API are logged in the database

#### Running as Python function

To run the program as a Python function, import the `dlx_dl` module and pass the arguments specified in --help to the `run()` function as normal Python keyword arguments.

Python:
```Python
import dlx_dl

dlx_dl.run(source='export_id', type='bib', id=1, xml='output.xml')

dlx_dl.run(source='export_id', type='bib', id=1, use_api=True)
```

### Command line examples
> Preview (display in console) records that meet export criteria and quit
```bash
$ dlx-dl --source=export_id --type=bib --modified_within=3600 --preview
```

> Write single record to DL by ID
```bash
$ dlx-dl --source=export_id --type=bib --id=1 --use_api
```

> Write records to DL from a list of IDs
```bash
$ dlx-dl --source=export_id --type=bib --list=ids.txt --use_api
```

> Write records to file
```bash
$ dlx-dl --source=export_id--type=bib --ids 1 2 3 --xml=output.xml
```
