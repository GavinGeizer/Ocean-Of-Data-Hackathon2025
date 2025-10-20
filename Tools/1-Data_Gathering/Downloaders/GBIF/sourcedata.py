import os
import requests
import time
from tqdm import tqdm
import zipfile

# This script requires the 'tqdm' library for progress bars.
# You can install it by running: pip install tqdm

def get_publisher_datasets(publisher_uuid):
    """
    Retrieves a list of all datasets for a given publisher.

    Args:
        publisher_uuid (str): The UUID of the GBIF publisher.

    Returns:
        list: A list of dataset metadata dictionaries, or an empty list if an error occurs.
    """
    datasets = []
    offset = 0
    limit = 100  # GBIF API page size
    is_end_of_records = False
    
    base_url = "https://api.gbif.org/v1/dataset/search"
    
    print(f"Fetching datasets for publisher: {publisher_uuid}")

    while not is_end_of_records:
        try:
            params = {'publishingOrg': publisher_uuid, 'offset': offset, 'limit': limit}
            print(f"Requesting page of datasets (offset: {offset}, limit: {limit})...")
            response = requests.get(base_url, params=params)
            response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)

            page_data = response.json()
            
            if not page_data.get('results'):
                print("No more datasets found for this publisher.")
                break
                
            datasets.extend(page_data['results'])
            
            # Check if we have reached the end of the dataset list
            is_end_of_records = page_data.get('endOfRecords', True)
            
            print(f"Found {len(datasets)} datasets so far...")
            
            offset += limit
            time.sleep(1) # Be polite to the API

        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 404:
                print(f"Error: Organization with UUID '{publisher_uuid}' not found.")
            else:
                print(f"An HTTP error occurred: {err}")
            return []
        except requests.exceptions.RequestException as err:
            print(f"An error occurred: {err}")
            return []
            
    return datasets

def download_dataset_archive(dataset, download_path):
    """
    Downloads the Darwin Core Archive for a single dataset with a progress bar.

    Args:
        dataset (dict): The dataset metadata dictionary from GBIF.
        download_path (str): The directory where the file should be saved.
    """
    dataset_key = dataset.get('key')
    if not dataset_key:
        print("Skipping dataset because it has no key.")
        return

    dataset_title = dataset.get('title', 'unknown_title').replace('/', '_') # Sanitize title for filename
    print(f"Processing dataset: {dataset_title} ({dataset_key})")
    
    try:
        # Get detailed dataset information to find the download link
        print(f"-> Getting details for dataset {dataset_key}...")
        response = requests.get(f"https://api.gbif.org/v1/dataset/{dataset_key}")
        response.raise_for_status()
        dataset_details = response.json()
        
        # Find the Darwin Core Archive endpoint
        dwca_url = None
        for endpoint in dataset_details.get('endpoints', []):
            if endpoint.get('type') == 'DWC_ARCHIVE':
                dwca_url = endpoint.get('url')
                break
        
        if not dwca_url:
            print(f"-> No Darwin Core Archive download link found for this dataset. Skipping.")
            return

        print(f"-> Found DwC-A link: {dwca_url}")
        
        # Download the file with a progress bar
        with requests.get(dwca_url, stream=True) as r:
            r.raise_for_status()
            
            # Get file size for the progress bar
            total_size_in_bytes = int(r.headers.get('content-length', 0))
            block_size = 1024 # 1 Kilobyte
            
            filename = f"{dataset_key}.zip"
            filepath = os.path.join(download_path, filename)
            
            print(f"-> Downloading to {filepath}...")
            
            with open(filepath, 'wb') as f, tqdm(
                desc=f"  - {dataset_key}",
                total=total_size_in_bytes,
                unit='iB',
                unit_scale=True,
                unit_divisor=1024,
                leave=False
            ) as progress_bar:
                for chunk in r.iter_content(chunk_size=block_size):
                    progress_bar.update(len(chunk))
                    f.write(chunk)
            
            print(f"-> Download complete.")

    except requests.exceptions.RequestException as err:
        print(f"-> Failed to download dataset {dataset_key}. Error: {err}")
    finally:
        time.sleep(1) # Be polite to the API


def unzip_all_downloads(download_dir):
    """
    Unzips all .zip files in the given download directory into subfolders (one per zip)
    and deletes the original zip files after successful extraction.
    """
    if not os.path.isdir(download_dir):
        print(f"Download directory not found: {download_dir}")
        return

    zip_files = [f for f in os.listdir(download_dir) if f.lower().endswith('.zip')]
    if not zip_files:
        print("No zip files found to unzip.")
        return

    print(f"Unzipping {len(zip_files)} files in '{download_dir}'...")

    for zf in tqdm(zip_files, desc="Unzipping", unit="file"):
        zippath = os.path.join(download_dir, zf)
        try:
            with zipfile.ZipFile(zippath, 'r') as zip_ref:
                extract_dir = os.path.join(download_dir, os.path.splitext(zf)[0])
                os.makedirs(extract_dir, exist_ok=True)
                zip_ref.extractall(extract_dir)
            # remove original zip
            try:
                os.remove(zippath)
            except OSError as e:
                print(f"Warning: could not remove zip file {zippath}: {e}")
        except zipfile.BadZipFile:
            print(f"Failed to unzip {zippath}: bad zip file. Skipping.")
        except Exception as e:
            print(f"Error unzipping {zippath}: {e}")

    print("Unzip and cleanup complete.")

def main():
    """
    Main function to run the script.
    """
    print("--- GBIF Publisher Dataset Downloader ---")
    
    # Example UUID for "United States Geological Survey"
    # b351a324-872b-47c5-a476-c08b28f73e73
    publisher_uuid = input("Please enter the GBIF publisher UUID: ").strip()

    if not publisher_uuid:
        print("Publisher UUID cannot be empty. Exiting.")
        return

    download_dir = "gbif_downloads"
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
        print(f"Created download directory: {download_dir}")

    datasets_to_download = get_publisher_datasets(publisher_uuid)

    if not datasets_to_download:
        print("No datasets found to download. Exiting.")
        return
        
    total_datasets = len(datasets_to_download)
    print(f"\nFound a total of {total_datasets} datasets. Starting download process...")
    
    # Overall progress bar for all datasets
    with tqdm(total=total_datasets, desc="Overall Progress") as pbar:
        for i, dataset in enumerate(datasets_to_download):
            pbar.set_description(f"Overall Progress (Dataset {i+1}/{total_datasets})")
            download_dataset_archive(dataset, download_dir)
            pbar.update(1)

    # After downloads complete, ask the user whether to unzip all files
    should_unzip = input("\nUnzip all downloaded archives and delete the original zips? [y/N]: ").strip().lower()
    if should_unzip in ('y', 'yes'):
        unzip_all_downloads(download_dir)
    else:
        print("Skipping unzip step.")

    print("\n\nAll tasks are complete. Files are saved in the 'gbif_downloads' directory.")

    


if __name__ == "__main__":
    main()

