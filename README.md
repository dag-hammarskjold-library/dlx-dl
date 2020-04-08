
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

sys.argv[1:] = ['--help']

dlx_dl.main()
```

### Notes
* Required arguments are `--connect` and `--type`, and one of `--id`, `--list`, or `--modified_from` 
* If you supply `--output_file`, an XML file of all the records meeting the criteria will be written to that path
* if you supply `--api_key`, each file that meets the criteria will be written to DL in "insertorreplace" mode
* If you supply `--log`, a log will be written to the specified database in a collection called "log".
* If you run the program as a function, you can access the log data through the module's global variable `LOG_DATA`

### Examples
> Preview (display in console) records that meet export criteria and exit
```bash
$ dlx-dl --connect=<connection string> --type=bib --modified_from=2020-04-06 --preview
```

> Write records as batch to XML file
```bash
$ dlx-dl --connect=<connection string> --type=bib --modified_from=2020-04-06 --output_file=<path to file>
```

> Write single record to XML file
```bash
$ dlx-dl --connect=<connection string> --type=bib --id=1000000 --output_file=<path to file>
```

> Write single record to DL and log
```bash
$ dlx-dl --connect=<connection string> --type=bib --id=1000000 --api_key=<api key> --log=<connection string>
```

> Write multiple records one at a time to DL and log
```bash
$ dlx-dl --connect=<connection string> --type=bib --modified_from=2020-04-06Z00:00 --api_key=<api key> --log=<connection string>
```
