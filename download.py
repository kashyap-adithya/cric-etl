import os
import requests
import zipfile

def download_and_extract_zip(zip_url, download_path):
    """Downloads a ZIP file and extracts its JSON contents."""
    
    os.makedirs(download_path, exist_ok=True)
    zip_file_path = os.path.join(download_path, "ipl_json.zip")

    # Download the ZIP file
    print(f"Downloading ZIP file from {zip_url}...")
    response = requests.get(zip_url, stream=True)
    if response.status_code == 200:
        with open(zip_file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
        print(f"Downloaded ZIP file: {zip_file_path}")
    else:
        print("Failed to download ZIP file.")
        return

    # Extract JSON files
    print("Extracting JSON files...")
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(download_path)

    print(f"Extracted files to {download_path}")
    os.remove(zip_file_path)  # Cleanup ZIP file after extraction
