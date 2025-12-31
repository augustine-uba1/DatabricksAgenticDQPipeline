# Prompt user for Databricks host URL
$hostUrl = Read-Host "Enter the Databricks host URL (e.g., https://your-workspace.cloud.databricks.com)"

# Run databricks auth login with the provided host
& databricks auth login --host $hostUrl

# Check if login was successful
if ($LASTEXITCODE -eq 0) {
    # Print the auth profiles
    Write-Host "Auth profiles:"
    & databricks auth profiles

    # Print initialization message
    Write-Host "initialising databricks asset bundle"

    # Run databricks bundle init
    & databricks bundle init
} else {
    Write-Host "Authentication failed. Please check your credentials and try again."
}