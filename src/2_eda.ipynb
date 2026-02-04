{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "a01b006a-d6c4-40e0-a7fd-ecd81ef46993",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "display(spark.sql(\"\"\"\n",
    "    SELECT quality, COUNT(*) as count\n",
    "    FROM jpmc_group_catalog.mlops.wine_data_poc_dabs\n",
    "    GROUP BY quality\n",
    "    ORDER BY quality\n",
    "\"\"\"))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "57c94e0c-bba1-4315-b6bc-1f0c11b07a13",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "\n",
    "# Import required libraries\n",
    "import pandas as pd\n",
    "import matplotlib.pyplot as plt\n",
    "import seaborn as sns\n",
    "\n",
    "# Load data from Delta table into Spark DataFrame\n",
    "wine_df = spark.table(\"jpmc_group_catalog.mlops.wine_data_poc_dabs\")\n",
    "\n",
    "# Convert to Pandas DataFrame for EDA\n",
    "pdf = wine_df.toPandas()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "7865d6be-4f81-46ff-b9cc-c47c738731f8",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Show first few rows\n",
    "display (wine_df)\n",
    "\n",
    "# Show schema\n",
    "wine_df.printSchema()\n",
    "\n",
    "# Summary statistics\n",
    "display (wine_df.describe())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "17684bc2-4de2-4124-8553-4f9697b9bb72",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "\n",
    "# Check for missing values\n",
    "missing = pdf.isnull().sum()\n",
    "print(\"Missing values per column:\\n\", missing)\n",
    "\n",
    "# Data types\n",
    "print(\"Data types: \\n\", pdf.dtypes)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "ba396dcb-5526-42f8-9f6f-a33cc2adf93e",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Distribution of wine quality\n",
    "plt.figure(figsize=(8,5))\n",
    "sns.countplot(x=\"quality\", data=pdf, palette=\"viridis\")\n",
    "plt.title(\"Distribution of wine Quality\")\n",
    "plt.xlabel(\"Quality\")\n",
    "plt.ylabel(\"Count\")\n",
    "plt.show()\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "545fbe85-dcca-48c7-b01a-873b1225726c",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Plot distributions for key features\n",
    "features = [\n",
    "    \"fixed_acidity\", \"volatile_acidity\", \"citric_acid\",\n",
    "    \"residual_sugar\",\n",
    "    \"chlorides\", \"free_sulfur_dioxide\",\n",
    "    \"total_sulfur_dioxide\",\n",
    "    \"density\", \"pH\", \"sulphates\", \"alcohol\"\n",
    "]\n",
    "\n",
    "plt.figure(figsize=(16,12))\n",
    "for i, feature in enumerate(features):\n",
    "    plt.subplot(4, 3, i+1)\n",
    "    sns.histplot(pdf[feature], kde=True, color=\"skyblue\")\n",
    "    plt.title(f\"{feature} Distribution\")\n",
    "plt.tight_layout() \n",
    "plt.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "f8f758fb-6bed-42d8-a015-46c98fd21e31",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Correlation matrix\n",
    "corr = pdf [features + [\"quality\"]].corr()\n",
    "\n",
    "plt.figure(figsize=(10,8))\n",
    "sns.heatmap(corr, annot=True, cmap=\"coolwarm\") \n",
    "plt.title(\"Feature Correlation Matrix\")\n",
    "plt.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "a7a8d78c-8a7f-4694-ab47-fdf31812d2dc",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Boxplots of features vs. quality \n",
    "plt.figure(figsize=(16,12))\n",
    "for i, feature in enumerate (features):\n",
    "    plt.subplot(4, 3, i+1)\n",
    "    sns.boxplot(x=\"quality\", y=feature, data=pdf,\n",
    "    palette=\"Set2\")\n",
    "    plt.title(f\"{feature} vs Quality\")\n",
    "plt.tight_layout()\n",
    "plt.show()\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "ed7582db-f67d-4d6f-8485-ccdf1eff677e",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Outlier detection using boxplots\n",
    "plt.figure(figsize=(16,12))\n",
    "for i, feature in enumerate (features):\n",
    "    plt.subplot(4, 3, i+1)\n",
    "    sns.boxplot(y=pdf [feature], color=\"orange\")   \n",
    "    plt.title(f\"Outliers in {feature}\")\n",
    "\n",
    "    plt.tight_layout()\n",
    "plt.show()\n"
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
   "notebookName": "2_eda",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
