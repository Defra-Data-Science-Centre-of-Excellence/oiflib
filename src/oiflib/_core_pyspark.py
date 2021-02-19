# """Core functions used across all OIF modules."""

# # standard library
# import os
# from io import BytesIO
# from typing import Any, Dict, List

# # third-party
# import requests
# from azure.identity import DefaultAzureCredential
# from azure.keyvault.secrets import SecretClient
# from azure.storage.blob import BlobClient, BlobServiceClient
# from pyspark.sql import DataFrame, SparkSession

# spark: SparkSession = SparkSession.builder.enableHiveSupport().getOrCreate()


# def melt(
#     df: DataFrame, id_vars: str, var_name: str = "variable", value_name: str = "value"
# ) -> DataFrame:
#     """Unpivots a DataFrame from wide to long format.

#     Args:
#         df: A Spark DataFrame.
#         id_vars: Column(s) to use as identifier variables.
#         var_name: Name to use for the variable column. Defaults to 'variable'.
#         value_name: Name to use for the value column. Defaults to 'value'.

#     Returns:
#         DataFrame: An unpivoted Spark DataFrame.

#     Raises:
#         Not yet implemented.
#     """
#     var_columns: List[str] = [col for col in df.columns if col not in id_vars]
#     expression: str = ", ".join(
#         [", ".join(["'" + x + "'", "`" + x + "`"]) for x in var_columns]
#     )
#     return df.selectExpr(
#         id_vars, f"stack({len(var_columns)},{expression}) as ({var_name}, {value_name})"  # noqa: B950
#     ).orderBy(var_name, id_vars)


# # def read_input(input_path: str, input_file: str) -> DataFrame:
# #     """TODO docstring."""
# #     return spark.read.parquet(f"{input_path}/{input_file}")


# # def write_output(df: DataFrame, output_path: str, output_file: str) -> bool:
# #     """TODO docstring."""
# #     df.write.mode("overwrite").parquet(f"{output_path}/{output_file}")
# #     return True


# def url_to_blob(
#     url: str,
#     url_headers: Dict[str, str] = None,
#     blob_connection_string: str = None,
#     blob_container_name: str = "oif-got",
#     blob_name: str = None,
#     blob_overwrite: bool = True,
# ) -> Dict[str, Any]:
#     """Reads a file from URL, then writes to Blob storage.

#     Given the URL of a file, this function reads the content of a
#     requests.Response object into an in-memory binary stream, then uploads it
#     to Azure Blob storage.

#     Args:
#         url (str): the URL of the file to upload.
#         url_headers (Dict[str, str], optional): HTTP headers to be added to the GET
#             request. If None, a User-Agent header is set using the environmental
#             variable USER_AGENT_STRING. Defaults to None.
#         blob-connection-string (str, optional): Blob storage connection string. If None,  # noqa: B950
#             the value of the environmental variable AZURE_STORAGE_CONNECTION_STRING
#             is used. Defaults to None.
#         blob_container_name (str, optional): Blob storage container name. Defaults
#             to "oif-got".
#         blob_name (str, optional): Blob name. If None, the name of the file is
#             extracted from the URL. Defaults to None.
#         blob_overwrite (bool, optional): Whether the blob to be uploaded should
#             overwrite the current data. If True, upload_blob will overwrite the existing  # noqa: B950
#             data. If set to False, the operation will fail with ResourceExistsError.
#             Defaults to True.

#     Returns:
#         Dict[str, Any]: Blob-updated property dict (Etag and last modified).
#     """

#     if url_headers is None:
#         url_headers: Dict[str, str] = {"User-Agent": os.getenv("USER_AGENT_STRING")}

#     if blob_connection_string is None:
#         vault_name: str = os.getenv("AZURE_KEY_VAULT_NAME")
#         vault_url: str = f"https://{ vault_name }.vault.azure.net"

#         credential: DefaultAzureCredential = DefaultAzureCredential()

#         secret_client: SecretClient = SecretClient(
#             vault_url=vault_url,
#             credential=credential,
#         )

#         blob_connection_string: str = secret_client.get_secret(
#             "oif-blob-connection-string"
#         )

#     if blob_name is None:
#         blob_name: str = url.split("/")[-1]

#     blob_service_client: BlobServiceClient = BlobServiceClient.from_connection_string(
#         blob_connection_string,
#     )

#     blob: BlobClient = blob_service_client.get_blob_client(
#         blob_container_name,
#         blob_name,
#     )

#     return blob.upload_blob(
#         data=BytesIO(
#             requests.get(
#                 url=url,
#                 headers=url_headers,
#             ).content,
#         ),
#         overwrite=blob_overwrite,
#     )
