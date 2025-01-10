import asyncio
from fabric import SemanticModelClient
from tabulate import tabulate

async def execute_and_print(semantic_client, dax_query, description, impersonated_user=None):
    """
    Executes a DAX query and prints the results in a tabulated format with a description.

    Args:
        semantic_client (SemanticModelClient): The client to execute queries.
        dax_query (str): The DAX query to execute.
        description (str): Description of the query for better understanding of results.
        impersonated_user (str, optional): The UPN of a user to impersonate.
    """
    print(f"\nExecuting Query: {description}")
    result = await semantic_client.execute_dax_query(dax_query, impersonated_user)
    if result is not None:
        if not result:
            print("No results found.")
        else:
            # Extract headers from the first row's keys
            headers = result[0].keys()
            # Extract rows as lists
            rows = [list(row.values()) for row in result]
            # Print the table using tabulate
            table = tabulate(rows, headers=headers, tablefmt="grid")
            print(table)
    else:
        print("Failed to retrieve data for this query.")

async def main():
    """
    Main function to execute multiple DAX queries against the DimProduct table.
    """
    # Initialize the SemanticModelClient
    semantic_client = SemanticModelClient()
    
    try:
        # Define a list of queries with descriptions
        queries = [
            {
                "description": "Summarize Product Classes",
                "query": """
                EVALUATE 
                SUMMARIZECOLUMNS(
                    DimProduct[ClassName]
                )
                """
            },
            {
                "description": "List of Products in Economy Class",
                "query": """
                EVALUATE 
                FILTER(
                    DimProduct,
                    DimProduct[ClassName] = "Economy"
                )
                """
            }
        ]

        # Optionally, specify an impersonated user if required
        impersonated_user = None  # Replace with "user@domain.com" if impersonation is needed

        # Execute each query and print results
        for q in queries:
            await execute_and_print(
                semantic_client,
                q["query"],
                q["description"],
                impersonated_user
            )
    finally:
        # Ensure that the client is properly closed
        await semantic_client.close()

if __name__ == "__main__":
    asyncio.run(main())
