
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
 
### Credentials
Starting with dlx v1.6.0, it is possible to use temporary AWS credentials rather than saving static AWS access keys in your environment. To use temporary credentials, run:
```bash
aws login
```

You'll see the following in your console:
```
Attempting to open your default browser. If the browser does not open, open the following URL.
If you are unable to open the URL on this device, run this command again with the '--remote' option.
```

Complete the sign-in in your browser, and note the message in your console indicating the profile and credentials applied:
```
Updated profile <profile> to use arn:aws:iam::<account>:<user> credentials.
```

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

#### other scripts

https://github.com/dag-hammarskjold-library/dlx-dl/blob/main/dlx_dl/scripts



