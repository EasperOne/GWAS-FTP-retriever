# GWAS-FTP-retriever
This github repo contains only a Python script designed to automate the download of Genome-Wide Association Study (GWAS) summary statistics from an FTP server of NHGRI-EBI GWAS catalog. The script is particularly useful for downloading data related to specific study accessions from the GWAS catalog hosted by the European Bioinformatics Institute (EBI).

## Usage:
```
python FTP_retriever_advanced.py <Accession number 1> <Accession number 2> ...
```

## Features

- **Automated FTP Connection**: Establishes an anonymous FTP connection to the EBI GWAS server.
- **Recursive Directory Download**: Supports downloading entire directories or individual files recursively from the FTP server.
- **Robust Error Handling**: Implements retry logic with exponential backoff for handling temporary errors during FTP operations.
- **Colored Console Output**: Uses `colorama` for color-coded messages in the terminal, enhancing readability of the process.
- **Beautiful progressbar**: Display a progressbar from `tqdm` to show the download progress.
- **Download Time Tracking**: Measures and displays the time taken for each download.

## Requirements

- Python 3.x
- The following Python packages:
  - `ftplib`
  - `tqdm`
  - `colorama`

You can install the required packages using `pip`:

```bash
pip install tqdm colorama
```
