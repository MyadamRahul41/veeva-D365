import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Tuple, Optional
import threading
import os
from dotenv import load_dotenv
from tqdm import tqdm

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

DRIVE_ID = os.getenv('sharepointProdDriveID')
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
            print("Access token refreshed successfully")
        except Exception as e:
            print(f"Failed to refresh token: {e}")
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
            'none_spec_number': 0
        }
        self.stats_lock = threading.Lock()
        self.failed_files = []

    def get_all_ids_with_next_page(self) -> List[str]:
        """Fetch all document IDs using pagination"""
        query = (
            "SELECT id, name__v, filename__v, specification_main_version__c, specification_minor_version__c, subtype__v, status__v "
            "FROM documents "
            "WHERE (specification_minor_version__c != null OR specification_main_version__c != null) "
            "AND (status__v = 'Approved' OR status__v = 'Obsolete') "
            "AND (subtype__v = 'Formulation Inert Specification' "
            "OR subtype__v = 'Composition of Formulation (CoF)' "
            "OR subtype__v = 'Specification Technical Material' "
            "OR subtype__v = 'Formulation Specification' "
            "OR subtype__v = 'Selection List')"
        )

        base_url = f'{VEEVA_BASE_URL}/query'
        params = {"q": query}
        print(f"Initial query params: {params}")

        all_ids = []

        # First request
        response = self.session.post(url=base_url, headers=self.veeva_headers, params=params, timeout=60).json()

        total_records = response.get('responseDetails', {}).get('total', 0)
        print(f"Total records to fetch: {total_records}")

        pbar = tqdm(total=total_records, desc="Fetching document IDs")

        while True:
            data = response.get('data', [])
            all_ids.extend([r['id'] for r in data if 'id' in r])
            pbar.update(len(data))

            # Check if next_page exists
            next_page = response.get('responseDetails', {}).get('next_page')
            if not next_page:
                break

            # Fetch next page
            response = self.session.get(url=f'{VEEVA_BASE_URL.split("/api")[0]}' + next_page,
                                    headers=self.veeva_headers, timeout=60).json()

        pbar.close()
        print(f"Total IDs collected: {len(all_ids)}")
        return all_ids

    def fetch_document_by_id(self, doc_id: str) -> Optional[Dict]:
        """Fetch a single document by ID from Veeva API"""
        url = f"{VEEVA_BASE_URL}/query"
        query = (
            "SELECT id, name__v, filename__v, specification_main_version__c, specification_minor_version__c, subtype__v, status__v "
            f"FROM documents WHERE id = '{doc_id}'"
        )
        params = {"q": query}

        try:
            response = self.session.get(url, headers=self.veeva_headers, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()

            if data.get('responseStatus') == 'FAILURE':
                print(f"Veeva API returned FAILURE status for doc_id={doc_id}, {data}")
                return None

            documents = data.get('data', [])
            return documents[0] if documents else None
        except Exception as e:
            print(f"Error fetching document with id={doc_id}: {e}")
            return None

    def download_file(self, file_id: str) -> Optional[bytes]:
        url = f"{VEEVA_BASE_URL}/objects/documents/{file_id}/file"

        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.get(url, headers=self.veeva_headers, timeout=120)
                response.raise_for_status()
                return response.content
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                    print(f"Retry {attempt + 1}/{MAX_RETRIES} for downloading file {file_id}")
                else:
                    print(f"Failed to download file {file_id} after {MAX_RETRIES} attempts: {e}")
                    return None
        return None
    
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
                check_url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/root:/{filename_encoded}"

                try:
                    check_response = self.session.get(check_url, headers=headers, timeout=30)

                    if check_response.status_code == 404:
                        # File doesn't exist, we can use this name
                        print(f"File '{current_filename}' is available for upload")
                        return current_filename
                    elif check_response.status_code == 200:
                        # File exists, try next count
                        count += 1
                        current_filename = f"{base_name}_({count}){extension}"
                        print(f"File exists, trying new name: {current_filename}")
                    else:
                                                # Unexpected status code, log and return original filename
                        print(f"Unexpected status {check_response.status_code} when checking file existence. Using original filename.current_filename:{current_filename}")
                        return filename

                except Exception as e:
                    print(f"Error checking file existence: {e}. Using original filename.")
                    return filename

        except Exception as e:
            print(f"Error in check_file_presents: {e}. Using original filename.")
            return filename

    def upload_to_sharepoint(self,file_content,items_to_upload) -> Tuple[bool, str]:
        documenttypemapping = {
            'Formulation Inert Specification' : 'ZSpec_IBDV_URL',
            'Composition of Formulation (CoF)' : 'ZSpec_IBDV_URL',
            'Specification Technical Material' : 'Zspec_TM_URL',
            'Formulation Specification' : 'Spec_URL',
            'Selection List' : 'ZSpec_SeL_URL'
            }
        statusmapping = {'Approved':'Approved',
                        'Released':'Approved',
                        'Obsolete':'Obsolete'}

        file_name = items_to_upload.get('FileName',None)
        specification_version = items_to_upload.get('SpecificationNumber',None)
        file_id = items_to_upload.get('file_id',None)
        specification_minor_version = items_to_upload.get('specification minor version',None)
        actual_filename = items_to_upload.get('actual_filename',None)

        for attempt in range(MAX_RETRIES):
            try:
                access_token = self.token_manager.get_token()
                sp_headers = {
                    'Authorization': f'Bearer {access_token}',
                    'Content-Type': 'application/octet-stream'
                }
                # Check if file exists and get unique filename if needed
                unique_filename = self.check_file_presents(actual_filename)

                # Upload file
                filename_encoded = unique_filename.replace(" ", "%20")
                upload_url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/root:/{filename_encoded}:/content"

                upload_response = self.session.put(upload_url, headers=sp_headers, data=file_content, timeout=120)

                if upload_response.status_code not in [200, 201]:
                    error_msg = f"Upload failed with status {upload_response.status_code}: {upload_response.text}"
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY * (attempt + 1))
                        continue
                    return False, error_msg


                # Update metadata
                uploaded_file_info = upload_response.json()
                item_id = uploaded_file_info["id"]
                if specification_version or specification_minor_version:
                    update_body = {
                        "specificationmainversion":items_to_upload.get('specificationmainversion',None),
                        "SpecificationNumber":items_to_upload.get('SpecificationNumber',None),
                        "FileName":items_to_upload.get('FileName',None),
                        "DocumentType":documenttypemapping.get(items_to_upload.get('DocumentType',None),None),
                        "DocumentDescription":items_to_upload.get('FileName',None),
                        "DocId":items_to_upload.get('file_id',None),
                        "DocumentStatus":statusmapping.get(items_to_upload.get('DocumentStatus',None),None),
                        "specificationminorversion":items_to_upload.get('specificationminorversion',None)
                    }

                    update_url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{item_id}/listItem/fields"
                    update_headers={
                            'Authorization': f'Bearer {access_token}',
                            'Content-Type': 'application/json'   # must be JSON
                        }
                    update_resp = self.session.patch(update_url, headers=update_headers, json=update_body, timeout=60)

                    if update_resp.status_code == 200:
                        print(f"metadata for {file_name} and {id} updated")
                        return True, "Success"
                    else:
                        print(f"metadata not updated for {file_name} {update_resp.text} and records is :{update_body}")
                        return True, f"File uploaded but metadata update failed: {update_resp.text}"
                else:
                    print(f"Skipped upload — both specification_version and specification_minor_version are absent for {file_name} and {file_id}")
                    return False, "Missing both specification versions"
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                    print(f"Retry {attempt + 1}/{MAX_RETRIES} for uploading {file_name}")
                else:
                    return False, str(e)
        return False, "Max retries exceeded"
    
    def get_lookup(self,specnumber:int):
        if specnumber:
            try:
                params = {
                    "q":f"select specification_number__c from formulation__v where id = '{specnumber}'"
                }
                base_url = f'{VEEVA_BASE_URL}/query'
                response = self.session.get(url=base_url, headers=self.veeva_headers, params=params, timeout=60)
                response.raise_for_status()
                response_data = response.json()

                # Accept both SUCCESS and WARNING statuses if data is present
                if response_data.get('responseStatus') in ['SUCCESS', 'WARNING'] and response_data.get('data'):
                    return response_data['data'][0].get('specification_number__c')
                else:
                    print(f"Failed to lookup specification for {specnumber}: {response_data}")
                    return None
            except Exception as e:
                print(f"Error in get_lookup for {specnumber}: {e}")
                return None
        else:
            return None
    

    def process_document(self, doc_fields: Dict) -> Tuple[bool, str, str]:
        """Process a single document: download from Veeva and upload to SharePoint"""
        try:
            filename = doc_fields.get('name__v', 'unknown')

            # Handle specification_main_version__c which might be a list or string
            spec_main_ref = doc_fields['specification_main_version__c']
            if isinstance(spec_main_ref, list):
                spec_main_ref = spec_main_ref[0] if spec_main_ref else None

            actual_specnumber = self.get_lookup(spec_main_ref) if spec_main_ref else None
            root_spec = actual_specnumber.split('-')[0] if actual_specnumber else None

            # Skip doc_fieldss with None specification numbers
            if root_spec is None:
                error_msg = "Specification number is None/empty"
                print(f"Skipping file {filename}: {error_msg}")
                with self.stats_lock:
                    self.stats['none_spec_number'] += 1
                    self.stats['skipped'] += 1
                    self.failed_files.append((filename, error_msg))
                return False, filename, error_msg

            # Get specification versions
            spec_main_version = actual_specnumber
            spec_minor_version = doc_fields.get('specification_minor_version__c', None)

            # Check if both versions are missing
            if not spec_main_version and not spec_minor_version:
                error_msg = "Missing both specification main and minor versions"
                print(f"File {filename} missing both versions: {error_msg}")
                with self.stats_lock:
                    self.stats['skipped'] += 1
                    self.failed_files.append((filename, error_msg))
                return False, filename, error_msg
            
            if doc_fields.get('specification_minor_version__c'):
                filename = doc_fields.get('name__v')
                doc_fields['specification_minor_version__c'] = doc_fields['specification_minor_version__c'][0]

                
            items_to_upload = {
                'specificationmainversion':actual_specnumber,
                'actual_filename':doc_fields.get('filename__v', '').strip(),
                'file_id': doc_fields.get('id'),
                'SpecificationNumber':root_spec,
                'FileName': filename,
                'DocumentType': doc_fields.get('subtype__v', ''),
                'Documentdescription':filename,
                'DocumentStatus': doc_fields.get('status__v', ''),
                'specificationminorversion':doc_fields.get('specification_minor_version__c','')
            }
            record = {k: (v.strip() if isinstance(v, str) else v) for k, v in items_to_upload.items()}
            # Download from Veeva
            file_content = self.download_file(record.get('file_id'))
            if file_content is None:
                return False, record.get('FileName'), f"Failed to download file {record.get('file_id')}"

            # Upload to SharePoint
            success, message = self.upload_to_sharepoint(file_content,record)

            # Update stats
            with self.stats_lock:
                if success:
                    self.stats['successful'] += 1
                else:
                    self.failed_files.append((record.get('FileName'), message))

            return success, record.get('FileName'), message

        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            filename = doc_fields.get('filename__v', 'unknown')
            with self.stats_lock:
                self.stats['failed'] += 1
                self.failed_files.append((filename, error_msg))
            return False, filename, error_msg

    def run(self, max_documents=None):
        """Main execution method with concurrent processing"""
        print("Starting Veeva to SharePoint upload process...")
        print("\n" + "="*80)
        print("STEP 1: Fetching all document IDs")
        print("="*80)

        # Fetch all document IDs first
        all_doc_ids = self.get_all_ids_with_next_page()

        if not all_doc_ids:
            print("No documents found to process")
            return

        # Limit documents if max_documents is specified
        if max_documents:
            print(f"\nLimiting upload to {max_documents} documents out of {len(all_doc_ids)} total")
            all_doc_ids = all_doc_ids[:max_documents]

        print("\n" + "="*80)
        print(f"STEP 2: Processing {len(all_doc_ids)} documents")
        print("="*80 + "\n")

        with self.stats_lock:
            self.stats['total'] = len(all_doc_ids)

        # Process documents in batches
        batch_size = BATCH_SIZE
        for i in range(0, len(all_doc_ids), batch_size):
            batch_ids = all_doc_ids[i:i + batch_size]
            print(f"\nProcessing batch {i//batch_size + 1}/{(len(all_doc_ids) + batch_size - 1)//batch_size} ({len(batch_ids)} documents)...")

            # Fetch full document details for this batch
            documents = []
            for doc_id in batch_ids:
                doc = self.fetch_document_by_id(doc_id)
                if doc:
                    documents.append(doc)
                else:
                    with self.stats_lock:
                        self.stats['skipped'] += 1
                        self.failed_files.append((doc_id, "Failed to fetch document details"))

            if not documents:
                print("No valid documents in this batch")
                continue

            # Process documents concurrently
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(self.process_document, doc): doc for doc in documents}

                for future in as_completed(futures):
                    success, file_name, message = future.result()
                    if success:
                        print(f"[OK] Successfully uploaded: {file_name}")
                    else:
                        print(f"[FAILED] Failed to upload: {file_name} - {message}")

                    # Print progress every 100 files
                    if (self.stats['successful'] + self.stats['skipped']) % 100 == 0:
                        self._print_progress()

            time.sleep(0.5)

        # Final summary
        self._print_final_summary()

    def _print_progress(self):
        """Print current progress statistics"""
        with self.stats_lock:
            print(f"\n{'='*60}")
            print(f"Progress: {self.stats['successful']}/{self.stats['total']} files processed")
            print(f"Successful: {self.stats['successful']} | Skipped: {self.stats['skipped']}")
            print(f"{'='*60}\n")

    def _print_final_summary(self):
        """Print final upload summary"""
        print("\n" + "="*80)
        print("UPLOAD COMPLETE")
        print("="*80)
        print(f"Total files: {self.stats['total']}")
        print(f"Successful: {self.stats['successful']}")
        print(f"Skipped: {self.stats['skipped']}")
        print(f"Files with specification_main_version__c field but null: {self.stats['none_spec_number']}")
        print("="*80)

        # Save all skipped files to failed_uploads.json
        if self.failed_files:
            import json
            with open('failed_uploads.json', 'w') as f:
                json.dump(self.failed_files, f, indent=2)
            print(f"\nSkipped files saved to 'failed_uploads.json' ({len(self.failed_files)} files)")


def main():
    """Main entry point"""
    try:
        # Initialize token manager
        token_manager = TokenManager()
        token_manager.get_token()  # Get initial token

        # Initialize and run uploader (limited to 20 documents)
        uploader = VeevaToSharePointUploader(token_manager)
        uploader.run(max_documents=20)

    except KeyboardInterrupt:
        print("\nUpload process interrupted by user")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")


if __name__ == "__main__":
    main()