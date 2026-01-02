# Validate the Databricks Asset Bundle for the dev target
Write-Host "Validating Databricks Asset Bundle for dev target..."
& databricks bundle validate -t dev

# Check if validation was successful
if ($LASTEXITCODE -eq 0) {
    Write-Host "Validation successful. Deploying Databricks Asset Bundle for dev target..."
    & databricks bundle deploy -t dev

    # Check if deployment was successful
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Deployment successful. Running ingest_autoloader_job for dev target with specified parameters..."
        & databricks bundle run -t dev ingest_autoloader_job --params "source_key=stock,source_path=abfss://raw-stock-data@stadevm77kkznmognla.dfs.core.windows.net/stock_raw,raw_table=dev_raw.stock.stock,format=parquet,schema_location=abfss://raw-stock-data@stadevm77kkznmognla.dfs.core.windows.net/stock_raw/_schemas/stock,checkpoint_location=abfss://raw-stock-data@stadevm77kkznmognla.dfs.core.windows.net/stock_raw/_checkpoints/stock,available_now=true,processing_time=1 minute"

        if ($LASTEXITCODE -eq 0) {
            Write-Host "Job run completed successfully."
        } else {
            Write-Host "Job run failed. Please check the output above for details."
        }
    } else {
        Write-Host "Deployment failed. Please check the output above for details."
    }
} else {
    Write-Host "Validation failed. Please check the output above for details."
}