import sys
import os
import ftplib
import time
import random
from urllib.parse import urlparse
from tqdm import tqdm
from datetime import datetime
from contextlib import contextmanager
from colorama import Fore, init

init(autoreset=True)

@contextmanager
def ftp_connection(host):
    ftp = None
    try:
        ftp = ftplib.FTP(host)
        ftp.login()  # anonymous login
        yield ftp
    finally:
        if ftp:
            try:
                ftp.quit()
            except:
                ftp.close()

def ensure_dir_exists(directory):
    os.makedirs(directory, exist_ok=True)

def retry_with_backoff(func, max_retries=5, initial_delay=1, max_delay=60):
    retries = 0
    while True:
        try:
            return func()
        except ftplib.error_temp as e:
            if retries >= max_retries:
                raise Exception(f"Max retries reached. Last error: {e}")
            delay = min(initial_delay * (2 ** retries) + random.uniform(0, 1), max_delay)
            print(f"Temporary error: {e}. Retrying in {delay:.2f} seconds...")
            time.sleep(delay)
            retries += 1

def format_time(seconds):
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours):02d}:{int(minutes):02d}:{seconds:05.2f}"

def download_ftp_item(ftp, remote_path, local_path, position):
    def download_attempt():
        nonlocal start_time
        file_size = ftp.size(remote_path)
        
        print(f"Downloading: {os.path.basename(local_path)}")
        print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        with open(local_path, 'wb') as local_file:
            with tqdm(total=file_size, unit='B', unit_scale=True, ncols=100, position=position, leave=True, colour='green') as pbar:
                def callback(data):
                    local_file.write(data)
                    pbar.update(len(data))
                
                ftp.retrbinary(f"RETR {remote_path}", callback)

    try:
        ensure_dir_exists(os.path.dirname(local_path))
        start_time = datetime.now()
        retry_with_backoff(download_attempt)
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print(f"Downloaded file: {local_path}")
        print(f"Finished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Download duration: {format_time(duration)}")
    except ftplib.error_perm as e:
        if str(e).startswith('550'):
            print(f"{Fore.CYAN}{remote_path} is a directory. Entering...")
            download_ftp_directory(ftp, remote_path, local_path, position)
        else:
            print(f"{Fore.LIGHTRED_EX}Error downloading {remote_path}: {e}")
    except Exception as e:
        print(f"{Fore.LIGHTRED_EX}Failed to download {remote_path} after multiple retries: {e}")


def directory_exists(ftp, path):
    current = ftp.pwd()
    try:
        ftp.cwd(path)
        ftp.cwd(current)
        return True
    except ftplib.error_perm:
        return False

def download_ftp_directory(ftp, remote_dir, local_dir, position=0):
    if not directory_exists(ftp, remote_dir):
        print(f"{Fore.LIGHTRED_EX}Error: Directory {remote_dir} does not exist on the server.")
        return
    
    ensure_dir_exists(local_dir)

    def change_directory():
        ftp.cwd(remote_dir)

    try:
        retry_with_backoff(change_directory)
    except Exception as e:
        print(f"{Fore.LIGHTRED_EX}Error: Cannot access directory {remote_dir}. Error: {e}")
        return

    print(f"{Fore.BLUE}Entering directory: {remote_dir}")
    
    def list_items():
        items = []
        ftp.dir(items.append)
        return items

    items = retry_with_backoff(list_items)
    
    for index, item in enumerate(items):
        tokens = item.split(maxsplit=8)
        if len(tokens) < 9:
            continue
        
        item_name = tokens[8]
        item_type = tokens[0][0]
        
        remote_path = f"{remote_dir}/{item_name}"
        local_path = os.path.join(local_dir, item_name)
        
        if item_type == 'd':
            download_ftp_directory(ftp, remote_path, local_path, position + index)
        else:
            download_ftp_item(ftp, remote_path, local_path, position + index)
    
    ftp.cwd('..')
    print(f"{Fore.BLUE}Leaving directory: {remote_dir}")

def download_from_ftp_url(ftp_url, accession):
    parsed_url = urlparse(ftp_url)
    ftp_host = parsed_url.netloc
    ftp_path = parsed_url.path

    with ftp_connection(ftp_host) as ftp:
        start_time = datetime.now()
        print(f"{Fore.GREEN}Starting download for {accession} at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        download_ftp_directory(ftp, ftp_path, accession)
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print(f"{Fore.GREEN}Finished download for {accession} at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total download time for {accession}: {format_time(duration)}")

    print(f"All available files have been downloaded to the '{accession}' directory.")

def construct_ftp_url(accession):
    base_url = "ftp://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/"
    
    if accession.startswith("GCST9"):
        # New format (e.g., GCST90243138)
        accession_num = int(accession[4:])
        range_start = (accession_num // 1000) * 1000 + 1
        range_end = range_start + 999
        return f"{base_url}GCST{range_start:08d}-GCST{range_end:08d}/{accession}/"
    else:
        # Old format (e.g., GCST004426)
        accession_num = int(accession[4:])
        range_start = (accession_num // 1000) * 1000 + 1
        range_end = range_start + 999
        return f"{base_url}GCST{range_start:06d}-GCST{range_end:06d}/{accession}/"

def download_gwas_data(accession):
    ftp_url = construct_ftp_url(accession)
    print(f"{Fore.GREEN}Attempting to download data for {accession}")
    print(f"FTP URL: {ftp_url}")
    download_from_ftp_url(ftp_url, accession)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script_name.py <study_accession1> [<study_accession2> ...]")
        sys.exit(1)
    
    for accession in sys.argv[1:]:
        download_gwas_data(accession)
        time.sleep(1)  # Add a small delay between accessions to avoid overwhelming the server
