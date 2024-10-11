import os
import pysolr
import csv
import requests
import json
import logging
from typing import Dict, List, Union
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SolrClient:
    """A class to handle interactions with Apache Solr."""

    def __init__(self, base_url: str):
        """
        Initialize the SolrClient.

        Args:
            base_url (str): The base URL for the Solr instance.
        """
        self.base_url = base_url

    def check_connection(self) -> bool:
        """Check the connection to Solr."""
        try:
            response = requests.get(f"{self.base_url}/admin/cores?action=STATUS&wt=json")
            response.raise_for_status()
            logger.info(f"Solr connection successful. Response: {response.text}")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Error connecting to Solr: {str(e)}")
            if hasattr(e, 'response'):
                logger.error(f"Response status code: {e.response.status_code}")
                logger.error(f"Response content: {e.response.text}")
            return False

    def check_core_exists(self, core_name: str) -> bool:
        """Check if a Solr core exists."""
        try:
            response = requests.get(f"{self.base_url}/admin/cores?action=STATUS&core={core_name}&wt=json")
            response.raise_for_status()
            cores = response.json()['status']
            exists = core_name in cores
            logger.info(f"Core {core_name} exists: {exists}")
            if not exists:
                logger.info(f"Available cores: {list(cores.keys())}")
            return exists
        except requests.exceptions.RequestException as e:
            logger.error(f"Error checking core {core_name}: {str(e)}")
            if hasattr(e, 'response'):
                logger.error(f"Response status code: {e.response.status_code}")
                logger.error(f"Response content: {e.response.text}")
            return False

    def create_core(self, core_name: str) -> str:
        """Create a new Solr core."""
        if self.check_core_exists(core_name):
            return f"Core {core_name} already exists."
        
        params = {
            'action': 'CREATE',
            'name': core_name,
            'instanceDir': core_name,
            'configSet': '_default'
        }
        try:
            response = requests.get(f"{self.base_url}/admin/cores", params=params)
            response.raise_for_status()
            logger.info(f"Core creation response: {response.text}")
            if 'success' in response.json():
                return f"Core {core_name} created successfully."
            else:
                return f"Failed to create core {core_name}. Response: {response.text}"
        except requests.exceptions.RequestException as e:
            logger.error(f"Error creating core {core_name}: {str(e)}")
            if hasattr(e, 'response'):
                logger.error(f"Response status code: {e.response.status_code}")
                logger.error(f"Response content: {e.response.text}")
            return f"Failed to create core {core_name}. Error: {str(e)}"

    def index_data(self, core_name: str, exclude_column: str, csv_file: str) -> str:
        """Index data from a CSV file into a Solr core."""
        try:
            solr = pysolr.Solr(f'{self.base_url}/{core_name}/', always_commit=True)
            with open(csv_file, 'r') as file:
                reader = csv.DictReader(file)
                documents = [{k: v for k, v in row.items() if k != exclude_column} for row in reader]
            logger.info(f"Attempting to index {len(documents)} documents into {core_name}")
            solr.add(documents)
            return f"Data indexed into {core_name}, excluding column {exclude_column}"
        except FileNotFoundError:
            logger.error(f"Error: {csv_file} file not found in the current directory.")
            return f"Failed to index data: CSV file {csv_file} not found"
        except Exception as e:
            logger.error(f"Error indexing data: {str(e)}")
            return f"Failed to index data: {str(e)}"

    def search_by_column(self, core_name: str, column_name: str, column_value: str) -> List[Dict]:
        """Search for documents in a Solr core based on a column value."""
        try:
            solr = pysolr.Solr(f'{self.base_url}/{core_name}/', always_commit=True)
            query = f'{column_name}:"{column_value}"'
            results = solr.search(query)
            return list(results)
        except Exception as e:
            logger.error(f"Search error details: {str(e)}")
            return []

    def get_employee_count(self, core_name: str) -> Union[int, str]:
        """Get the total number of documents (employees) in a Solr core."""
        try:
            solr = pysolr.Solr(f'{self.base_url}/{core_name}/', always_commit=True)
            results = solr.search('*:*', rows=0)
            return results.hits
        except Exception as e:
            logger.error(f"Error getting employee count: {str(e)}")
            return f"Failed to get employee count: {str(e)}"

    def delete_employee_by_id(self, core_name: str, employee_id: str) -> str:
        """Delete an employee document from a Solr core by ID."""
        try:
            solr = pysolr.Solr(f'{self.base_url}/{core_name}/', always_commit=True)
            solr.delete(id=employee_id)
            return f"Employee with ID {employee_id} deleted from {core_name}"
        except Exception as e:
            logger.error(f"Error deleting employee: {str(e)}")
            return f"Failed to delete employee: {str(e)}"

    def get_department_facet(self, core_name: str) -> Dict[str, int]:
        """Get a facet count of departments in a Solr core."""
        try:
            solr = pysolr.Solr(f'{self.base_url}/{core_name}/', always_commit=True)
            results = solr.search('*:*', facet='on', facet_field='Department')
            facet_counts = results.facets['facet_fields']['Department']
            return dict(zip(facet_counts[::2], facet_counts[1::2]))
        except Exception as e:
            logger.error(f"Error getting department facet: {str(e)}")
            return {}

def setup_cores(client: SolrClient, core_names: List[str]) -> None:
    """Set up Solr cores."""
    for core_name in core_names:
        logger.info(f"Creating core: {core_name}")
        result = client.create_core(core_name)
        logger.info(result)

def index_data_to_cores(client: SolrClient, core_configs: Dict[str, str], csv_file: str) -> None:
    """Index data into Solr cores."""
    for core_name, exclude_column in core_configs.items():
        logger.info(f"Indexing data into {core_name}")
        result = client.index_data(core_name, exclude_column, csv_file)
        logger.info(result)

def perform_operations(client: SolrClient, core_name: str) -> None:
    """Perform various operations on a Solr core."""
    # Delete an employee
    del_result = client.delete_employee_by_id(core_name, os.getenv('EMPLOYEE_ID_TO_DELETE', 'E02003'))
    logger.info(del_result)

    # Get employee count
    emp_count = client.get_employee_count(core_name)
    logger.info(f"Employee count in {core_name}: {emp_count}")

    # Search by column
    search_department = os.getenv('SEARCH_DEPARTMENT', 'IT')
    logger.info(f"Employees in {search_department} department ({core_name}):")
    search_result = client.search_by_column(core_name, 'Department', search_department)
    logger.info(search_result)

    # Get department facet
    logger.info(f"Department facet for {core_name}:")
    facet_result = client.get_department_facet(core_name)
    logger.info(facet_result)

def main():
    """Main execution function."""
    client = SolrClient(os.getenv('SOLR_URL', 'http://localhost:8983/solr'))

    if not client.check_connection():
        logger.error("Unable to connect to Solr. Please check the Solr URL and ensure Solr is running.")
        return

    core_names = os.getenv('CORE_NAMES', 'Hash_YourName,Hash_1234').split(',')
    setup_cores(client, core_names)

    core_configs = {
        core_name: os.getenv(f'{core_name.upper()}_EXCLUDE_COLUMN', 'Department')
        for core_name in core_names
    }
    csv_file = os.getenv('CSV_FILE', 'employee_data.csv')
    index_data_to_cores(client, core_configs, csv_file)

    for core_name in core_names:
        perform_operations(client, core_name)

if __name__ == "__main__":
    main()
