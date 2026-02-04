{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "9ae97b1a-f7d2-47cd-8d62-c41483f10b3d",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "%pip install databricks-feature-engineering"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "285240f6-e1a8-4f92-b61c-27bdac2afbe2",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "%restart_python"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "6fc60b4f-21dc-4d8a-a9e5-52564d846f0e",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "%pip install databricks-feature-engineering\n",
    "from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup\n",
    "from mlflow.tracking.client import MlflowClient\n",
    "from pyspark.sql.functions import current_timestamp, lit"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "a647c0d9-6898-4b55-a3d5-5301a49008da",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Load data from Delta table\n",
    "wine_df = spark.table(\"jpmc_group_catalog.mlops.wine_data_poc_dabs\")\n",
    "\n",
    "# Prepare inference data with entity key and raw acidity columns for UDF computation \n",
    "inference_df = wine_df.select(\"wine_id\", \"citric_acid\", \"fixed_acidity\", \"volatile_acidity\")\n",
    "\n",
    "# Prepare actual labels for comparison (optional for evaluation)\n",
    "label_df = wine_df.select(\"wine_id\", \"quality\")\n",
    "\n",
    "# Add timestamp for monitoring and tracking predictions\n",
    "inference_df = inference_df.withColumn(\"timestamp\", current_timestamp())\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "7a0f69bc-158f-40be-a8ef-ee02331d3d86",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "\n",
    "\n",
    "model_name = \"jpmc_group_catalog.mlops.wine_rf_model_dabs\"\n",
    "mlflow_client = MlflowClient()\n",
    "def get_latest_model_version (model_name):\n",
    "    latest_version = 1\n",
    "    for mv in mlflow_client.search_model_versions(f\"name='{model_name}'\"):\n",
    "        version_int = int(mv.version)\n",
    "        if version_int > latest_version:\n",
    "           latest_version = version_int\n",
    "    return latest_version\n",
    "\n",
    "latest_model_version = get_latest_model_version(model_name)\n",
    "print(f\"Latest model version: {latest_model_version}\")\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "2800e41f-d929-4397-8696-1bf25ccc7055",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "\n",
    "\n",
    "fe = FeatureEngineeringClient()\n",
    "\n",
    "# Score batch using registered model\n",
    "predictions_df = fe.score_batch(\n",
    "     model_uri=f\"models:/{model_name}/{latest_model_version}\",\n",
    "     df=inference_df\n",
    ")\n",
    "\n",
    "# Add model version for lineage\n",
    "predictions_df = predictions_df.withColumn(\"model_id\", lit(latest_model_version)) \n",
    "predictions_df = predictions_df.join(label_df, on=\"wine_id\", how=\"left\")\n",
    "\n",
    "\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "50785e80-f931-4943-bca8-7a528d717402",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "\n",
    "# Define output table path\n",
    "\n",
    "predictions_table = \"jpmc_group_catalog.mlops.wine_predictions_v2_dabs\"\n",
    "\n",
    "# Save predictions to Delta table (schema evolution enabled)\n",
    "predictions_df.write.format(\"delta\").mode(\"overwrite\").option(\"mergeschema\", \"true\").saveAsTable(predictions_table)\n",
    "\n",
    "# Display for verification\n",
    "display(predictions_df)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "02f41caa-d4fe-48cf-b71d-f33495bb8c70",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "%sql\n",
    "\n",
    "-- Run in a SQL cell\n",
    "ALTER TABLE `jpmc_group_catalog`.`mlops`.`wine_predictions_v2_dabs`\n",
    "SET TBLPROPERTIES (delta.enableChangeDataFeed = true);\n"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "4"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 4
   },
   "notebookName": "5_model_inference",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
