import sys
from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()
    
with open("requirements.txt") as f:
    requirements = list(filter(None,f.read().split('\n')))

setup(
    name = 'dlx_dl',
    version = '1.2.13',
    url = 'http://github.com/dag-hammarskjold-library/dlx-dl',
    author = 'United Nations Dag Hammarskjöld Library',
    author_email = 'library-ny@un.org',
    license = 'http://www.opensource.org/licenses/bsd-license.php',
    packages = find_packages(exclude=['test']),
    test_suite = 'tests',
    install_requires = requirements,
    description = 'Export data fom DLX to DL',
    long_description = long_description,
    long_description_content_type = "text/markdown",
    python_requires = '>=3.8, <3.14',
    entry_points = {
        'console_scripts': [
            'dlx-dl=dlx_dl.scripts.export:run', # to deprecate
            'dlx-dl-export=dlx_dl.scripts.export:run',
            'dlx-dl-sync=dlx_dl.scripts.sync:run'
        ]
    }
)

