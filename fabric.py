import logging
import os
import json
import asyncio
from dotenv import load_dotenv
from azure.identity.aio import ClientSecretCredential
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SemanticModelClient:
    """
    SemanticModelClient connects to Power BI Premium and executes DAX queries.

    It authenticates using a service principal and interacts with the Power BI REST API.
    """

    def __init__(self):
        """
        Initializes the Semantic Model client with credentials and configuration.
        """
        self.tenant_id = os.getenv("FABRIC_TENANT_ID")
        self.client_id = os.getenv("FABRIC_SP_CLIENT_ID")
        self.client_secret = os.getenv("FABRIC_SP_CLIENT_SECRET")
        self.premium_dataset_id = os.getenv("FABRIC_DATASET_ID")
        self.myorg = os.getenv("FABRIC_ORG_NAME", "myorg")  # Default to 'myorg' if not set
        self.api_gateway_uri = f"https://api.powerbi.com/v1.0/{self.myorg}"
        self.scope = "https://analysis.windows.net/powerbi/api/.default"

        # Validate that all necessary environment variables are set
        if not all([self.tenant_id, self.client_id, self.client_secret, self.premium_dataset_id]):
            logger.error("One or more required environment variables are missing.")
            raise ValueError("Missing environment variables for SemanticModelClient.")

        # Initialize the credential
        self.credential = ClientSecretCredential(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret
        )

    async def close(self):
        """
        Closes the credential to free resources.
        """
        await self.credential.close()

    async def get_access_token(self):
        """
        Obtains an access token for the Power BI API.

        Returns:
            str: Access token.
        """
        try:
            token = await self.credential.get_token(self.scope)
            logger.info("Access token acquired successfully.")
            return token.token
        except Exception as e:
            logger.error(f"Failed to obtain access token: {e}")
            raise

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(aiohttp.ClientError)
    )
    async def execute_dax_query(self, dax_query: str, impersonated_user: str = None) -> list:
        """
        Executes a DAX query against the Premium Power BI dataset with retry logic.

        Args:
            dax_query (str): The DAX query to execute.
            impersonated_user (str, optional): The UPN of a user to impersonate.

        Returns:
            list: The rows returned by the query, or None if an error occurred.
        """
        access_token = await self.get_access_token()
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}"
        }
        url = f"{self.api_gateway_uri}/datasets/{self.premium_dataset_id}/executeQueries"
        body = {
            "queries": [
                {"query": dax_query}
            ],
            "serializerSettings": {
                "includeNulls": True
            }
        }

        if impersonated_user:
            body["impersonatedUserName"] = impersonated_user

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, headers=headers, json=body) as response:
                    if response.status == 200:
                        response_json = await response.json()
                        results = response_json.get('results', [])
                        if not results:
                            logger.warning("No results found in the response.")
                            return []
                        tables = results[0].get('tables', [])
                        if not tables:
                            logger.warning("No tables found in the response.")
                            return []
                        rows = tables[0].get('rows', [])
                        logger.info("DAX query executed successfully.")
                        return rows
                    elif response.status == 429:  # Too Many Requests
                        retry_after = response.headers.get("Retry-After")
                        wait_time = int(retry_after) if retry_after else None
                        if wait_time:
                            logger.warning(f"Rate limited. Retrying after {wait_time} seconds.")
                            await asyncio.sleep(wait_time)
                        else:
                            logger.warning("Rate limited. Retrying with exponential backoff.")
                        raise aiohttp.ClientError("Rate limited")
                    elif 400 <= response.status < 500:
                        error_message = await response.text()
                        logger.error(f"Client error executing DAX query. Status: {response.status}, Message: {error_message}")
                        return None
                    else:
                        error_message = await response.text()
                        logger.error(f"Server error executing DAX query. Status: {response.status}, Message: {error_message}")
                        raise aiohttp.ClientError(f"Server error: {response.status}")
            except aiohttp.ClientError as e:
                logger.warning(f"Client error occurred: {e}. Retrying...")
                raise
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                return None
