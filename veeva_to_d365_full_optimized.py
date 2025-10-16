import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Tuple, Optional
import threading
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('veeva_upload.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration - Load from environment variables
TENANT_ID = os.getenv('TENANT_ID')
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')

VEEVA_BASE_URL = os.getenv('VEEVA_BASE_URL')
VEEVA_TOKEN = os.getenv('VEEVA_TOKEN')

DRIVE_ID = os.getenv('DRIVE_ID')
FOLDER_NAME = os.getenv('FOLDER_NAME', 'veeva_documents')

# Performance settings - Load from environment with defaults
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '20'))  # Number of parallel uploads
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '200'))  # Number of documents to fetch per API call
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))  # Number of retry attempts for failed uploads
RETRY_DELAY = int(os.getenv('RETRY_DELAY', '2'))  # Seconds to wait before retry

# Validate required environment variables
required_vars = {
    'TENANT_ID': TENANT_ID,
    'CLIENT_ID': CLIENT_ID,
    'CLIENT_SECRET': CLIENT_SECRET,
    'VEEVA_BASE_URL': VEEVA_BASE_URL,
    'VEEVA_TOKEN': VEEVA_TOKEN,
    'DRIVE_ID': DRIVE_ID
}

missing_vars = [var for var, value in required_vars.items() if not value]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}. Please check your .env file.")

# Token management
token_lock = threading.Lock()
current_token = None
token_expiry = None


class TokenManager:
    """Manages OAuth token with automatic refresh"""

    def __init__(self):
        self.token = None
        self.expiry = None
        self.lock = threading.Lock()

    def get_token(self) -> str:
        """Get valid access token, refresh if needed"""
        with self.lock:
            if self.token is None or datetime.now() >= self.expiry:
                self._refresh_token()
            return self.token

    def _refresh_token(self):
        """Refresh the OAuth token"""
        url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
        data = {
            'grant_type': 'client_credentials',
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
            'scope': 'https://graph.microsoft.com/.default'
        }

        try:
            response = requests.post(url, data=data, timeout=30)
            response.raise_for_status()
            token_data = response.json()

            self.token = token_data['access_token']
            # Token typically expires in 3600 seconds, refresh 5 minutes early
            self.expiry = datetime.now() + timedelta(seconds=token_data.get('expires_in', 3600) - 300)
            logger.info("Access token refreshed successfully")
        except Exception as e:
            logger.error(f"Failed to refresh token: {e}")
            raise


class VeevaToSharePointUploader:
    """Handles concurrent file uploads from Veeva to SharePoint"""

    def __init__(self, token_manager: TokenManager):
        print("entered init function")
        self.token_manager = token_manager

        # Configure session with larger connection pool to support concurrent workers
        self.session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=MAX_WORKERS,
            pool_maxsize=MAX_WORKERS * 2,
            max_retries=Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504]
            )
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

        self.veeva_headers = {
            'Authorization': VEEVA_TOKEN,
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        self.stats = {
            'total': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'none_spec_number': 0,
            'missing_both_versions': 0,
            'total_upload_time': 0.0,  # Total time spent uploading
            'total_download_time': 0.0,  # Total time spent downloading
            'min_upload_time': float('inf'),
            'max_upload_time': 0.0,
            'min_download_time': float('inf'),
            'max_download_time': 0.0
        }
        self.stats_lock = threading.Lock()
        self.failed_files = []
        self.missing_versions_files = []  # Track files missing both versions
        self.upload_times = []  # Track individual upload times for percentile calculations

    def fetch_documents(self, start: int, limit: int) -> List[Dict]:
        """Fetch documents from Veeva API"""
        url = f"{VEEVA_BASE_URL}/objects/documents"
        params = {'start': start, 'limit': limit}

        try:
            response = self.session.get(url, headers=self.veeva_headers, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()

            if data.get('responseStatus') == 'FAILURE':
                logger.error(f"Veeva API returned FAILURE status at start={start}")
                return []

            return data.get('documents', [])
        except Exception as e:
            logger.error(f"Error fetching documents at start={start}: {e}")
            return []

    def download_file(self, file_id: str) -> Tuple[Optional[bytes], float]:
        url = f"{VEEVA_BASE_URL}/objects/documents/{file_id}/file"
        download_start = time.time()

        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.get(url, headers=self.veeva_headers, timeout=120)
                response.raise_for_status()
                download_time = time.time() - download_start

                # Update download statistics
                with self.stats_lock:
                    self.stats['total_download_time'] += download_time
                    self.stats['min_download_time'] = min(self.stats['min_download_time'], download_time)
                    self.stats['max_download_time'] = max(self.stats['max_download_time'], download_time)

                return response.content, download_time
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                    logger.warning(f"Retry {attempt + 1}/{MAX_RETRIES} for downloading file {file_id}")
                else:
                    logger.error(f"Failed to download file {file_id} after {MAX_RETRIES} attempts: {e}")
                    return None, 0.0
        return None, 0.0
    
    def check_file_presents(self, filename: str) -> str:
        try:
            access_token = self.token_manager.get_token()
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }

            # Split filename into name and extension
            if '.' in filename:
                name_parts = filename.rsplit('.', 1)
                base_name = name_parts[0]
                extension = '.' + name_parts[1]
            else:
                base_name = filename
                extension = ''

            count = 0
            current_filename = filename

            # Check if file exists and find unique name
            while True:
                folder_encoded = FOLDER_NAME.replace(" ", "%20")
                filename_encoded = current_filename.replace(" ", "%20")
                check_url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/root:/{folder_encoded}/{filename_encoded}"

                try:
                    check_response = self.session.get(check_url, headers=headers, timeout=30)

                    if check_response.status_code == 404:
                        # File doesn't exist, we can use this name
                        logger.info(f"File '{current_filename}' is available for upload")
                        return current_filename
                    elif check_response.status_code == 200:
                        # File exists, try next count
                        count += 1
                        current_filename = f"{base_name}_({count}){extension}"
                        logger.info(f"File exists, trying new name: {current_filename}")
                    else:
                        # Unexpected status code, log and return original filename
                        logger.warning(f"Unexpected status {check_response.status_code} when checking file existence. Using original filename.")
                        return filename

                except Exception as e:
                    logger.warning(f"Error checking file existence: {e}. Using original filename.")
                    return filename

        except Exception as e:
            logger.error(f"Error in check_file_presents: {e}. Using original filename.")
            return filename

    def upload_to_sharepoint(self,file_content,items_to_upload) -> Tuple[bool, str, float]:
        """Upload file to SharePoint and update metadata, return success status, message, and upload time"""
        file_name = items_to_upload.get('filename',None)
        specification_version = items_to_upload.get('specification number',None)
        subtype = items_to_upload.get('subtype',None)
        lifecyclestate = items_to_upload.get('lifecyclestate',None)
        file_id = items_to_upload.get('file_id',None)
        specification_minor_version = items_to_upload.get('specification minor version',None)

        upload_start = time.time()

        for attempt in range(MAX_RETRIES):
            try:
                access_token = self.token_manager.get_token()
                sp_headers = {
                    'Authorization': f'Bearer {access_token}',
                    'Content-Type': 'application/octet-stream'
                }
                # Check if file exists and get unique filename if needed
                unique_filename = self.check_file_presents(file_name)

                # Upload file
                folder_encoded = FOLDER_NAME.replace(" ", "%20")
                filename_encoded = unique_filename.replace(" ", "%20")
                upload_url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/root:/{folder_encoded}/{filename_encoded}:/content"

                upload_response = self.session.put(upload_url, headers=sp_headers, data=file_content, timeout=120)

                if upload_response.status_code not in [200, 201]:
                    error_msg = f"Upload failed with status {upload_response.status_code}: {upload_response.text}"
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY * (attempt + 1))
                        continue
                    upload_time = time.time() - upload_start
                    return False, error_msg, upload_time

                # Update metadata
                uploaded_file_info = upload_response.json()
                item_id = uploaded_file_info["id"]
                if specification_version or specification_minor_version:
                    update_body = {
                        "specification_x002b_mainversion": specification_version,
                        'Subtype':subtype,
                        'Lifecyclestate':lifecyclestate,
                        "Documentdescription":items_to_upload.get('Documentdescription',None)
                    }

                    update_url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{item_id}/listItem/fields"
                    update_headers={
                            'Authorization': f'Bearer {access_token}',
                            'Content-Type': 'application/json'   # must be JSON
                        }
                    update_resp = self.session.patch(update_url, headers=update_headers, json=update_body, timeout=60)

                    upload_time = time.time() - upload_start

                    # Update upload statistics
                    with self.stats_lock:
                        self.stats['total_upload_time'] += upload_time
                        self.stats['min_upload_time'] = min(self.stats['min_upload_time'], upload_time)
                        self.stats['max_upload_time'] = max(self.stats['max_upload_time'], upload_time)
                        self.upload_times.append(upload_time)

                    if update_resp.status_code == 200:
                        logger.info(f"metadata for {file_name} and {id} updated")
                        return True, "Success", upload_time
                    else:
                        logger.info(f"metadata not updated for {file_name}")
                        return True, f"File uploaded but metadata update failed: {update_resp.text}", upload_time
                else:
                    upload_time = time.time() - upload_start
                    logger.info(f"Skipped upload — both specification_version and specification_minor_version are absent for {file_name} and {file_id}")
                    return False, "Missing both specification versions", upload_time
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                    logger.warning(f"Retry {attempt + 1}/{MAX_RETRIES} for uploading {file_name}")
                else:
                    upload_time = time.time() - upload_start
                    return False, str(e), upload_time
        upload_time = time.time() - upload_start
        return False, "Max retries exceeded", upload_time
    
    def get_lookup(self,specnumber:int):

        params = {
            "q":f"select specification_number__c from formulation__v where id = '{specnumber}'"
        }
        base_url = 'https://sb-envu-deployment.veevavault.com/api/v24.2/query'
        response = requests.post(url=base_url, headers = self.veeva_headers, params=params).json()
        return response['data'][0]['specification_number__c']
    

    def process_document(self, doc_fields: Dict) -> Tuple[bool, str, str]:
        """Process a single document: download from Veeva and upload to SharePoint"""
        try:
            document = doc_fields.get('document', {})
            filename = document.get('filename__v', 'unknown')

            # Skip documents without specification_main_version__c field
            if 'specification_main_version__c' not in document:
                error_msg = "Missing specification_main_version__c field"
                with self.stats_lock:
                    self.stats['skipped'] += 1
                    self.failed_files.append((filename, error_msg))
                return False, filename, error_msg

            actual_specnumber = self.get_lookup(document['specification_main_version__c'])
            root_spec = actual_specnumber.split('-')[0] if actual_specnumber else None

            # Skip documents with None specification numbers
            if root_spec is None:
                error_msg = "Specification number is None/empty"
                logger.warning(f"Skipping file {filename}: {error_msg}")
                with self.stats_lock:
                    self.stats['none_spec_number'] += 1
                    self.stats['skipped'] += 1
                    self.failed_files.append((filename, error_msg))
                return False, filename, error_msg

            # Get specification versions
            spec_main_version = actual_specnumber
            spec_minor_version = document.get('specification_minor_version__c', None)

            # Check if both versions are missing
            if not spec_main_version and not spec_minor_version:
                error_msg = "Missing both specification main and minor versions"
                logger.warning(f"File {filename} missing both versions: {error_msg}")
                with self.stats_lock:
                    self.stats['missing_both_versions'] += 1
                    self.stats['skipped'] += 1
                    self.missing_versions_files.append({
                        'filename': filename,
                        'file_id': document.get('id'),
                        'subtype': document.get('subtype__v', ''),
                        'status': document.get('status__v', ''),
                        'reason': error_msg
                    })
                return False, filename, error_msg

            items_to_upload = {
                'file_id': document.get('id'),
                'filename': filename,
                'lifecyclestate': document.get('status__v', ''),
                'subtype': document.get('subtype__v', ''),
                'specification number': spec_main_version,
                'specification minor version': spec_minor_version,
                'Documentdescription':filename,
                'root spec':root_spec
            }
            # Download from Veeva
            file_content, download_time = self.download_file(items_to_upload.get('file_id'))
            if file_content is None:
                return False, items_to_upload.get('filename'), f"Failed to download file {items_to_upload.get('file_id')}"

            # Upload to SharePoint
            success, message, upload_time = self.upload_to_sharepoint(file_content,items_to_upload)

            # Update stats
            with self.stats_lock:
                if success:
                    self.stats['successful'] += 1
                else:
                    self.stats['failed'] += 1
                    self.failed_files.append((items_to_upload.get('filename'), message))

            return success, items_to_upload.get('filename'), message

        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            filename = doc_fields.get('document', {}).get('filename__v', 'unknown')
            with self.stats_lock:
                self.stats['failed'] += 1
                self.failed_files.append((filename, error_msg))
            return False, filename, error_msg

    def run(self):
        """Main execution method with concurrent processing"""
        logger.info("Starting Veeva to SharePoint upload process...")
        start_time = time.time()

        start_index = 0

        # Fetch and process documents in batches
        while True:
            logger.info(f"Fetching documents from index {start_index}...")
            documents = self.fetch_documents(start_index, BATCH_SIZE)

            if not documents:
                logger.info("No more documents to process")
                break

            with self.stats_lock:
                self.stats['total'] += len(documents)

            # Process documents concurrently
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(self.process_document, doc): doc for doc in documents}

                for future in as_completed(futures):
                    success, file_name, message = future.result()
                    if success:
                        logger.info(f"[OK] Successfully uploaded: {file_name}")
                    else:
                        logger.error(f"[FAILED] Failed to upload: {file_name} - {message}")

                    # Print progress every 100 files
                    if (self.stats['successful'] + self.stats['failed']) % 100 == 0:
                        self._print_progress()

            # Move to next batch
            start_index += BATCH_SIZE
            time.sleep(0.5)

        # Final summary
        elapsed_time = time.time() - start_time
        self._print_final_summary(elapsed_time)

    def _print_progress(self):
        """Print current progress statistics"""
        with self.stats_lock:
            total_processed = self.stats['successful'] + self.stats['failed']
            logger.info(f"\n{'='*60}")
            logger.info(f"Progress: {total_processed}/{self.stats['total']} files processed")
            logger.info(f"Successful: {self.stats['successful']} | Failed: {self.stats['failed']}")
            logger.info(f"{'='*60}\n")

    def _print_final_summary(self, elapsed_time: float):
        """Print final upload summary"""
        logger.info("\n" + "="*80)
        logger.info("UPLOAD COMPLETE")
        logger.info("="*80)
        logger.info(f"Total files: {self.stats['total']}")
        logger.info(f"Successful: {self.stats['successful']}")
        logger.info(f"Failed: {self.stats['failed']}")
        logger.info(f"Skipped (missing specification_main_version__c): {self.stats['skipped']}")
        logger.info(f"Files with None specification number: {self.stats['none_spec_number']}")
        logger.info(f"Files missing both specification versions: {self.stats['missing_both_versions']}")
        logger.info("="*80)

        # Overall timing statistics
        logger.info("\nTIMING STATISTICS:")
        logger.info("-"*80)
        logger.info(f"Total time elapsed: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
        if elapsed_time/3600 >= 1:
            logger.info(f"                    ({elapsed_time/3600:.2f} hours)")
        logger.info(f"Average processing speed: {self.stats['total']/elapsed_time:.2f} files/second")

        # Download timing statistics
        if self.stats['successful'] > 0:
            avg_download = self.stats['total_download_time'] / self.stats['successful']
            logger.info(f"\nDownload Statistics:")
            logger.info(f"  Total download time: {self.stats['total_download_time']:.2f} seconds ({self.stats['total_download_time']/60:.2f} minutes)")
            logger.info(f"  Average download time per file: {avg_download:.2f} seconds")
            if self.stats['min_download_time'] != float('inf'):
                logger.info(f"  Fastest download: {self.stats['min_download_time']:.2f} seconds")
            logger.info(f"  Slowest download: {self.stats['max_download_time']:.2f} seconds")

        # Upload timing statistics
        if self.upload_times:
            sorted_times = sorted(self.upload_times)
            avg_upload = self.stats['total_upload_time'] / len(self.upload_times)
            median_upload = sorted_times[len(sorted_times)//2]
            p95_upload = sorted_times[int(len(sorted_times) * 0.95)] if len(sorted_times) > 1 else sorted_times[0]

            logger.info(f"\nUpload Statistics:")
            logger.info(f"  Total upload time: {self.stats['total_upload_time']:.2f} seconds ({self.stats['total_upload_time']/60:.2f} minutes)")
            logger.info(f"  Average upload time per file: {avg_upload:.2f} seconds")
            logger.info(f"  Median upload time: {median_upload:.2f} seconds")
            logger.info(f"  95th percentile upload time: {p95_upload:.2f} seconds")
            if self.stats['min_upload_time'] != float('inf'):
                logger.info(f"  Fastest upload: {self.stats['min_upload_time']:.2f} seconds")
            logger.info(f"  Slowest upload: {self.stats['max_upload_time']:.2f} seconds")

        # Time breakdown
        if self.stats['successful'] > 0:
            total_processing_time = self.stats['total_download_time'] + self.stats['total_upload_time']
            download_percentage = (self.stats['total_download_time'] / total_processing_time) * 100
            upload_percentage = (self.stats['total_upload_time'] / total_processing_time) * 100

            logger.info(f"\nTime Breakdown:")
            logger.info(f"  Download time: {download_percentage:.1f}% of processing time")
            logger.info(f"  Upload time: {upload_percentage:.1f}% of processing time")
            logger.info(f"  Overhead (API calls, metadata updates, etc.): {elapsed_time - total_processing_time:.2f} seconds")

        logger.info("="*80)

        if self.failed_files:
            logger.info("\nFailed Files:")
            for file_name, error in self.failed_files[:20]:  # Show first 20 failures
                logger.info(f"  - {file_name}: {error}")
            if len(self.failed_files) > 20:
                logger.info(f"  ... and {len(self.failed_files) - 20} more (check veeva_upload.log for full list)")

        # Report files missing both versions
        if self.missing_versions_files:
            logger.info("\n" + "-"*80)
            logger.info(f"FILES MISSING BOTH SPECIFICATION VERSIONS ({len(self.missing_versions_files)} files):")
            logger.info("-"*80)
            for file_info in self.missing_versions_files[:20]:  # Show first 20
                logger.info(f"  - {file_info['filename']}")
                logger.info(f"    File ID: {file_info['file_id']}")
                logger.info(f"    Subtype: {file_info['subtype']}")
                logger.info(f"    Status: {file_info['status']}")
            if len(self.missing_versions_files) > 20:
                logger.info(f"  ... and {len(self.missing_versions_files) - 20} more")
            logger.info("-"*80)

        # Save failed files to a separate file for retry
        if self.failed_files:
            with open('failed_uploads.json', 'w') as f:
                json.dump(self.failed_files, f, indent=2)
            logger.info("\nFailed uploads saved to 'failed_uploads.json' for review/retry")

        # Save files missing both versions to a separate file
        if self.missing_versions_files:
            with open('missing_versions_files.json', 'w') as f:
                json.dump(self.missing_versions_files, f, indent=2)
            logger.info("Files missing both versions saved to 'missing_versions_files.json' for review")


def main():
    """Main entry point"""
    try:
        # Initialize token manager
        token_manager = TokenManager()
        token_manager.get_token()  # Get initial token

        # Initialize and run uploader
        uploader = VeevaToSharePointUploader(token_manager)
        uploader.run()

    except KeyboardInterrupt:
        logger.info("\nUpload process interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)


if __name__ == "__main__":
    main()