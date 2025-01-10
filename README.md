```markdown
# MY_REPO

MY_REPO is a Python-based solution to execute DAX queries against a Power BI Premium dataset using a service principal for authentication. This repository includes the necessary code, requirements, and a test program to validate the functionality.

## Features
- **Execute DAX Queries**: Perform queries on Power BI datasets using REST API.
- **Retry Logic**: Robust error handling with retry for rate limits and transient errors.
- **Logging**: Detailed logging for better debugging and monitoring.
- **Async Support**: Leverages Python's asynchronous capabilities for optimal performance.

## Setup

### Prerequisites
1. **Python 3.9+**: Ensure Python is installed on your machine.
2. **Azure Active Directory Service Principal**: Create a service principal with the required permissions to access Power BI datasets.
3. **Environment Variables**: Configure the `.env` file with your Azure credentials and dataset details.

### Installation
1. Clone this repository:
   ```bash
   git clone https://github.com/your-username/MY_REPO.git
   cd MY_REPO
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Create a `.env` file:
   ```bash
   cp .env.template .env
   ```
   Fill in the required details in the `.env` file.

### Usage
To run the test program:
```bash
python test_fabric.py
```

### Sample Result
Here is an example of a successful query execution:

```plaintext
INFO:fabric:DAX query executed successfully.
+-------------------------+
| DimProduct[ClassName]   |
+=========================+
| Economy                 |
+-------------------------+
| Regular                 |
+-------------------------+
| Deluxe                  |
+-------------------------+
```

## Repository Structure

```
MY_REPO/
├── fabric.py             # Main library for executing DAX queries
├── test_fabric.py        # Test script to validate functionality
├── requirements.txt      # Python dependencies
├── .env.template         # Environment variable template
└── README.md             # Project documentation
```

## Environment Variables
| Variable               | Description                              |
|------------------------|------------------------------------------|
| `FABRIC_TENANT_ID`     | Azure AD Tenant ID                      |
| `FABRIC_SP_CLIENT_ID`  | Service Principal Client ID             |
| `FABRIC_SP_CLIENT_SECRET` | Service Principal Client Secret        |
| `FABRIC_ORG_NAME`      | Organization name for Power BI API      |
| `FABRIC_DATASET_ID`    | Dataset ID to query                     |

## Logging
Logs are output at the `INFO` level and provide details about query execution, errors, and retries.

## Contribution
Contributions are welcome! Feel free to submit issues or pull requests for improvements and bug fixes.

## License
This project is licensed under the MIT License. See the `LICENSE` file for details.
```