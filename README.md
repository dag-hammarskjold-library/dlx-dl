
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

dlx_dl.main(help=True)
```

### Notes
To run the program as a Python function, pass the arguments to the main() function as normal Python keyword arguments.

Command line:
```bash
$ dlx-dl --connect=<connection_string> --type=bib --id=1
```

Python:
```Python
import dlx_dl

dlx_dl.main(connect='<connection_string>', type='bib', id=1)
```

* Required arguments are `--connect` and `--type`, and one of `--id`, `--list`, `--modified_from`, or `--modified_within` 
* If you supply `--output_file`, an XML file of all the records meeting the criteria will be written to that path. Use "STDOUT" to print the XML to the console
* if you supply `--api_key`, each file that meets the criteria will be written to DL in "insertorreplace" mode

### Examples
> Preview (display in console) records that meet export criteria without writing XML
```bash
$ dlx-dl --connect=<connection_string> --type=bib --modified_from=2020-04-06 --preview
```

> Write single record to DL by ID
```bash
$ dlx-dl --connect=<connection_string> --type=bib --id=1000000 --api_key=<api_key>
```

> Write records to DL from a list of IDs
```bash
$ dlx-dl --connect=<connection_string> --type=bib --list=ids.txt --api_key=<api_key> 
```

> Write records to DL that were modified in the last hour
```bash
$ dlx-dl --connect=<connection_string> --type=bib --modified_within=3600 --api_key=<api_key> 
```