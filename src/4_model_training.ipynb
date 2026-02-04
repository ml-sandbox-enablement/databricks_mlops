{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "1791ac3a-0548-406e-927e-30cbe3bcb599",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# install requried packages\n",
    "\n",
    "\n",
    "%pip install databricks-feature-engineering optuna\n",
    "import pandas as pd\n",
    "from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup,FeatureFunction\n",
    "import mlflow\n",
    "import mlflow.sklearn\n",
    "import optuna\n",
    "from sklearn.ensemble import RandomForestRegressor\n",
    "from sklearn.model_selection import train_test_split\n",
    "from sklearn.metrics import mean_squared_error, r2_score\n",
    "from pyspark.sql.functions import monotonically_increasing_id"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "401533ec-b4ed-4677-be3b-f3f421dae5e1",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "\n",
    "#load intergarted data\n",
    "features_df = spark.table(\"jpmc_group_catalog.mlops.wine_features_engineered_dabs\") \n",
    "\n",
    "# Load or jpmc_group_catalog.mlops.winequality_dataiginal table with target\n",
    "labels_df = spark.table(\"jpmc_group_catalog.mlops.wine_data_poc_dabs\").select(\"quality\", * [col for col in spark.table(\"jpmc_group_catalog.mlops.wine_data_poc_dabs\").columns if col != \"quality\"])\n",
    "\n",
    "# Check if wine_id exists, if not, add it\n",
    "\n",
    "if \"wine_id\" not in features_df.columns:\n",
    "    features_df = features_df.withColumn(\"wine_id\", monotonically_increasing_id())\n",
    "    features_df.write.format(\"delta\").mode(\"overwrite\").option(\"mergeschema\", \"true\").saveAsTable(\"jpmc_group_catalog.mlops.wine_features_engineered_dabs\")\n",
    "\n",
    "if \"wine_id\" not in labels_df.columns:\n",
    "    labels_df = labels_df.withColumn(\"wine_id\", monotonically_increasing_id())\n",
    "    labels_df.write.format(\"delta\").mode(\"overwrite\").option(\"mergeschema\", \"true\").saveAsTable(\"jpmc_group_catalog.mlops.wine_data_poc_dabs\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "ac0fa864-dda0-4716-9deb-6c7f525342be",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "\n",
    "\n",
    "# Load engineered features from the feature table\n",
    "features_df = spark.table(\"jpmc_group_catalog.mlops.wine_features_engineered_dabs\")\n",
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
     "nuid": "132f8c1f-a69e-4031-9a74-dcc7ec9410ac",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Load labels (wine quality) with wine_id for joining\n",
    "labels_df = spark.table(\"jpmc_group_catalog.mlops.wine_data_poc_dabs\").select(\"wine_id\", \"quality\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "908af6d1-eb04-4ce6-b365-60878ec4e35a",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Load labels with additional acidity columns for on-demand feature computation \n",
    "labels_feature_df = spark.table(\"jpmc_group_catalog.mlops.wine_data_poc_dabs\").select (\"wine_id\", \"quality\", \"citric_acid\", \"fixed_acidity\", \"volatile_acidity\")\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "0eb97a51-5915-4ed7-ba3d-6891ae64e7d1",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Initialize Feature Engineering Client\n",
    "fe = FeatureEngineeringClient()\n",
    "\n",
    "# Define feature lookups: pre-computed features and on-demand UDF computation \n",
    "model_feature_lookups = [\n",
    "\n",
    "         # Lookup pre-engineered features from the feature table\n",
    "         FeatureLookup(\n",
    "\n",
    "            table_name=\"jpmc_group_catalog.mlops.wine_features_engineered_dabs\",\n",
    "            lookup_key=\"wine_id\"\n",
    "         ),\n",
    "\n",
    "# Compute average acidity on-demand using registered UDF\n",
    "FeatureFunction(\n",
    "           udf_name = \"jpmc_group_catalog.mlops.avg_acidity_udf_fn_dabs\", \n",
    "           input_bindings = {\"fixed_acidity\":\"fixed_acidity\",\n",
    "           \"volatile_acidity\":\"volatile_acidity\", \"citric_acid\":\"citric_acid\"},\n",
    " \n",
    "           output_name = \"acidity_avg_output\"\n",
    "),]\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "d3c55d10-94dd-4fa7-b7f0-e5a4501827f5",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Create training set by joining labels with features\n",
    "\n",
    "training_set = fe.create_training_set(\n",
    "   df=labels_feature_df,\n",
    "   feature_lookups=model_feature_lookups,\n",
    "   label=\"quality\",\n",
    "   exclude_columns=\"wine_id\"\n",
    "   )\n",
    "\n",
    "# Load the training DataFrame\n",
    "training_df = training_set.load_df() \n",
    "training_df.display()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "159bbdf4-f38c-4a4d-ab69-7dee4460113e",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "print(\"Training columns:\", training_df.columns)\n",
    "training_df.display(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "57f67c94-6b79-4424-9621-7b86a96445a4",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "training_pd = training_df.toPandas() \n",
    "print(training_pd.head())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "59874de6-0f7e-4e5c-aa3f-5041dde19e7a",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "\n",
    "# Drop wine_id and quality from features\n",
    "\n",
    "X = training_pd.drop([\"quality\"], axis=1)\n",
    "y = training_pd[\"quality\"]\n",
    "# Train/test split\n",
    "x_train, x_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "e4fc194b-1312-4b66-9f58-28a5016b0198",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "\n",
    "def objective(trial):\n",
    "   n_estimators = trial.suggest_int(\"n_estimators\", 50, 300) \n",
    "   max_depth = trial.suggest_int(\"max_depth\", 3, 20)\n",
    "   min_samples_split = trial.suggest_int(\"min_samples_split\", 2, 10) \n",
    "   min_samples_leaf = trial.suggest_int(\"min_samples_leaf\", 1, 10)\n",
    "\n",
    "\n",
    "   rf = RandomForestRegressor(\n",
    "   n_estimators=n_estimators,\n",
    "   max_depth=max_depth,\n",
    "   min_samples_split=min_samples_split,\n",
    "   min_samples_leaf=min_samples_leaf,\n",
    "   random_state=42\n",
    "   )    \n",
    "\n",
    "   rf.fit(x_train, y_train)\n",
    "   y_pred = rf.predict(x_test)\n",
    "   mse = mean_squared_error(y_test, y_pred)\n",
    "   \n",
    "   return mse\n",
    "\n",
    "study = optuna.create_study (direction=\"minimize\") \n",
    "study.optimize(objective, n_trials=30) \n",
    "best_params = study.best_params\n",
    "print(\"Best hyperparameters: \", best_params)\n",
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
     "nuid": "53d77b31-ed95-4a9a-a72b-6d71b3e6a8d4",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "\n",
    "rf_best = RandomForestRegressor(\n",
    "       n_estimators=best_params[\"n_estimators\"],\n",
    "       max_depth=best_params[\"max_depth\"],\n",
    "       min_samples_split=best_params[\"min_samples_split\"], \n",
    "       min_samples_leaf=best_params[\"min_samples_leaf\"],\n",
    "       random_state=42\n",
    ")\n",
    "\n",
    "rf_best.fit(x_train, y_train) \n",
    "y_pred = rf_best.predict(x_test)\n",
    "mse = mean_squared_error(y_test, y_pred) \n",
    "r2 = r2_score(y_test, y_pred)\n",
    "print(f\"Test MSE: {mse:.4f}, Test R2: {r2:.4f}\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "362ac687-2962-4079-b091-a8810e4df625",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "mlflow.set_registry_uri(\"databricks-uc\")\n",
    "model_name = \"jpmc_group_catalog.mlops.wine_rf_model_dabs\"\n",
    "\n",
    "\n",
    "mlflow.sklearn.autolog (log_models=False)\n",
    "fe = FeatureEngineeringClient()\n",
    "\n",
    "with mlflow.start_run() as run:\n",
    "   mlflow.log_params (best_params)\n",
    "   mlflow.log_metric(\"test_mse\", mse) \n",
    "   mlflow.log_metric (\"test_r2_score\", r2)\n",
    "   fe.log_model(\n",
    "       model=rf_best,\n",
    "       artifact_path=\"wine_rf_model\",\n",
    "       flavor=mlflow.sklearn,\n",
    "       training_set=training_set,\n",
    "       registered_model_name=model_name,\n",
    "    )   "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "fe667209-8d8a-415d-aa80-084b556867d4",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "import joblib\n",
    "import os\n",
    "\n",
    "volume_path = \"/108173_ctg_dev/m14hr/Volumes/wine_rf_model_artifacts_dabs\" \n",
    "os.makedirs(volume_path, exist_ok=True)\n",
    "joblib.dump(rf_best, f\"{volume_path}/rf_best_model.joblib\") \n",
    "print(f\"Model saved to (volume_path}/rf_best_model.joblib\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "565aa79b-5696-4bc4-8d3d-55102818158c",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": []
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
   "notebookName": "4_model_training",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
