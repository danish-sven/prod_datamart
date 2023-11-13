import re
import logging
from typing import List, Tuple
from google.cloud import bigquery
from google.cloud.bigquery import AccessEntry

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bqva.analyzer")

# Regular expression pattern to match table references in SQL
SQL_TABLE_PATTERN = re.compile(
    r"(?:(?:FROM|JOIN)\s+?)[\x60\[]?(?:(?P<project>[\w][-\w]+?)\x60?[\:\.])?\x60?(?P<dataset>[\w]+?)\x60?\.\x60?(?P<table>[\w]+)[\x60\]]?(?:\s|$)", 
    re.IGNORECASE | re.MULTILINE
)

# Regular expression pattern to remove comments from SQL
COMMENTS_PATTERN = re.compile(r"(\/\*(.|[\r\n])*?\*\/)|(--.*)")

def find_direct_dependencies(query: str, default_project: str) -> List[Tuple[str, str, str]]:
    """Find direct dataset and table dependencies in a SQL query."""
    # Remove comments from query to avoid picking up tables from commented out SQL code
    query_without_comments = re.sub(COMMENTS_PATTERN, "", query)
    return [(m.group('project') or default_project, m.group('dataset'), m.group('table')) for m in SQL_TABLE_PATTERN.finditer(query_without_comments)]

def apply_permissions(client: bigquery.Client, project_id: str, dataset_id: str, view_id: str):
    """Apply permissions to datasets directly referenced in a view's SQL query."""
    view_ref = bigquery.DatasetReference(project_id, dataset_id).table(view_id)
    view = client.get_table(view_ref)
    
    if view.table_type != 'VIEW':
        raise ValueError(f"The specified table {view_id} is not a view.")
    
    dependencies = find_direct_dependencies(view.view_query, view.project)
    view_access_entry = AccessEntry(None, 'view', view.reference.to_api_repr())
    
    for dep_project_id, dataset_id, _ in set(dependencies):
        dataset_ref = bigquery.DatasetReference(dep_project_id, dataset_id)
        dataset = client.get_dataset(dataset_ref)
        access_entries = list(dataset.access_entries)
        
        if view_access_entry not in access_entries:
            access_entries.append(view_access_entry)
            dataset.access_entries = access_entries
            client.update_dataset(dataset, ['access_entries'])
            log.info(f"Permissions applied to dataset '{dataset_id}' in project '{dep_project_id}' for view '{view_id}'.")