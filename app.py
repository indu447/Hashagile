import pysolr
import csv
import requests
import json

SOLR_URL = 'http://localhost:8983/solr'



# def test_core_connection(core_name):
#     try:
#         response = requests.get(f"{SOLR_URL}/{core_name}/admin/ping")
#         response.raise_for_status()
#         print(f"Successfully connected to core: {core_name}")
#         return True
#     except requests.exceptions.RequestException as e:
#         print(f"Error connecting to core {core_name}: {str(e)}")
#         return False

# # Test connections
# test_core_connection("Hash_YourName")
# test_core_connection("Hash_1234")


# def test_indexing(core_name):
#     solr = pysolr.Solr(f'{SOLR_URL}/{core_name}/', always_commit=True)
#     try:
#         solr.add([{"id": "test_doc", "title": "Test Document"}])
#         print(f"Successfully indexed a document to core: {core_name}")
#         return True
#     except Exception as e:
#         print(f"Error indexing to core {core_name}: {str(e)}")
#         return False

# # Test indexing
# test_indexing("Hash_YourName")
# test_indexing("Hash_1234")

def check_solr_connection():
    try:
        response = requests.get(f"{SOLR_URL}/admin/cores?action=STATUS&wt=json")
        response.raise_for_status()
        print(f"Solr connection successful. Response: {response.text}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to Solr: {str(e)}")
        if hasattr(e, 'response'):
            print(f"Response status code: {e.response.status_code}")
            print(f"Response content: {e.response.text}")
        return False

def check_core_exists(core_name):
    try:
        response = requests.get(f"{SOLR_URL}/admin/cores?action=STATUS&core={core_name}&wt=json")
        response.raise_for_status()
        cores = response.json()['status']
        exists = core_name in cores
        print(f"Core {core_name} exists: {exists}")
        if not exists:
            print(f"Available cores: {list(cores.keys())}")
        return exists
    except requests.exceptions.RequestException as e:
        print(f"Error checking core {core_name}: {str(e)}")
        if hasattr(e, 'response'):
            print(f"Response status code: {e.response.status_code}")
            print(f"Response content: {e.response.text}")
        return False

def createCore(core_name):
    if check_core_exists(core_name):
        print(f"Core {core_name} already exists.")
        return f"Core {core_name} already exists."
    
    params = {
        'action': 'CREATE',
        'name': core_name,
        'instanceDir': core_name,
        'configSet': '_default'
    }
    try:
        response = requests.get(f"{SOLR_URL}/admin/cores", params=params)
        response.raise_for_status()
        print(f"Core creation response: {response.text}")
        if 'success' in response.json():
            return f"Core {core_name} created successfully."
        else:
            return f"Failed to create core {core_name}. Response: {response.text}"
    except requests.exceptions.RequestException as e:
        print(f"Error creating core {core_name}: {str(e)}")
        if hasattr(e, 'response'):
            print(f"Response status code: {e.response.status_code}")
            print(f"Response content: {e.response.text}")
        return f"Failed to create core {core_name}. Error: {str(e)}"

def indexData(core_name, exclude_column):
    try:
        solr = pysolr.Solr(f'{SOLR_URL}/{core_name}/', always_commit=True)
        with open('employee_data.csv', 'r') as file:
            reader = csv.DictReader(file)
            documents = []
            for row in reader:
                doc = {k: v for k, v in row.items() if k != exclude_column}
                documents.append(doc)
            print(f"Attempting to index {len(documents)} documents into {core_name}")
            solr.add(documents)
        return f"Data indexed into {core_name}, excluding column {exclude_column}"
    except FileNotFoundError:
        print("Error: employee_data.csv file not found in the current directory.")
        return "Failed to index data: CSV file not found"
    

def searchByColumn(core_name, column_name, column_value):
    try:
        solr = pysolr.Solr(f'{SOLR_URL}/{core_name}/', always_commit=True)
        query = f'{column_name}:"{column_value}"'
        results = solr.search(query)
        return list(results)
    except Exception as e:
        print(f"Search error details: {str(e)}")
        return f"Failed to search: {str(e)}"

def getEmpCount(core_name):
    try:
        solr = pysolr.Solr(f'{SOLR_URL}/{core_name}/', always_commit=True)
        results = solr.search('*:*', rows=0)
        return results.hits
    except Exception as e:
        print(f"Error getting employee count: {str(e)}")
        return f"Failed to get employee count: {str(e)}"

def delEmpById(core_name, employee_id):
    try:
        solr = pysolr.Solr(f'{SOLR_URL}/{core_name}/', always_commit=True)
        solr.delete(id=employee_id)
        return f"Employee with ID {employee_id} deleted from {core_name}"
    except Exception as e:
        print(f"Error deleting employee: {str(e)}")
        return f"Failed to delete employee: {str(e)}"

def getDepFacet(core_name):
    try:
        solr = pysolr.Solr(f'{SOLR_URL}/{core_name}/', always_commit=True)
        results = solr.search('*:*', facet='on', facet_field='Department')
        facet_counts = results.facets['facet_fields']['Department']
        return dict(zip(facet_counts[::2], facet_counts[1::2]))
    except Exception as e:
        print(f"Error getting department facet: {str(e)}")
        return f"Failed to get department facet: {str(e)}"

def main():
    if not check_solr_connection():
        print("Unable to connect to Solr. Please check the Solr URL and ensure Solr is running.")
        return

    v_nameCore = 'Hash_YourName'
    v_phoneCore = 'Hash_1234'

    print("1. Create Cores:")
    print(createCore(v_nameCore))
    print(createCore(v_phoneCore))

    print("\n2. Get Employee Count (before indexing):")
    emp_count = getEmpCount(v_nameCore)
    print(f"Employee count in {v_nameCore}: {emp_count}")
    if isinstance(emp_count, str) and "Failed" in emp_count:
        print("Error getting employee count. Please check Solr connection and core existence.")

    print("\n3. Index Data:")
    index_result1 = indexData(v_nameCore, 'Department')
    print(index_result1)
    if "Failed" in index_result1:
        print("Error indexing data. Please check the CSV file and Solr core.")
    
    index_result2 = indexData(v_phoneCore, 'Gender')
    print(index_result2)
    if "Failed" in index_result2:
        print("Error indexing data. Please check the CSV file and Solr core.")

    print("\n4. Delete Employee:")
    del_result = delEmpById(v_nameCore, 'E02003')
    print(del_result)
    if "Failed" in del_result:
        print("Error deleting employee. Please check if the employee exists and the core is accessible.")

    print("\n5. Get Employee Count (after indexing and deletion):")
    emp_count = getEmpCount(v_nameCore)
    print(f"Employee count in {v_nameCore}: {emp_count}")
    if isinstance(emp_count, str) and "Failed" in emp_count:
        print("Error getting employee count. Please check Solr connection and core existence.")

    print("\n6. Search by Column:")
    print(f"Employees in IT department ({v_nameCore}):")
    search_result1 = searchByColumn(v_nameCore, 'Department', 'IT')
    print(search_result1)
    if isinstance(search_result1, str) and "Failed" in search_result1:
        print("Error searching. Please check Solr connection and core existence.")

    print(f"\nMale employees ({v_nameCore}):")
    search_result2 = searchByColumn(v_nameCore, 'Gender', 'Male')
    print(search_result2)
    if isinstance(search_result2, str) and "Failed" in search_result2:
        print("Error searching. Please check Solr connection and core existence.")

    print(f"\nEmployees in IT department ({v_phoneCore}):")
    search_result3 = searchByColumn(v_phoneCore, 'Department', 'IT')
    print(search_result3)
    if isinstance(search_result3, str) and "Failed" in search_result3:
        print("Error searching. Please check Solr connection and core existence.")

    print("\n7. Get Department Facet:")
    print(f"Department facet for {v_nameCore}:")
    facet_result1 = getDepFacet(v_nameCore)
    print(facet_result1)
    if isinstance(facet_result1, str) and "Failed" in facet_result1:
        print("Error getting department facet. Please check Solr connection and core existence.")

    print(f"\nDepartment facet for {v_phoneCore}:")
    facet_result2 = getDepFacet(v_phoneCore)
    print(facet_result2)
    if isinstance(facet_result2, str) and "Failed" in facet_result2:
        print("Error getting department facet. Please check Solr connection and core existence.")

if __name__ == "__main__":
    main()
