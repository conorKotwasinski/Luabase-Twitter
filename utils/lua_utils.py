from google.cloud import secretmanager

# Import the Secret Manager client library.
from google.cloud import secretmanager

# Create the Secret Manager client.
secret_client = secretmanager.SecretManagerServiceClient()

# Build the resource name of the secret version.
def get_secret(secret_id, version_id='latest'):
    project_id = 'luabase'
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = secret_client.access_secret_version(request={"name": name})

    # Verify payload checksum.
    # crc32c = google_crc32c.Checksum()
    # crc32c.update(response.payload.data)
    # if response.payload.data_crc32c != int(crc32c.hexdigest(), 16):
    #     print("Data corruption detected.")
    #     return response

    # Print the secret payload.
    #
    # WARNING: Do not print the secret in a production environment - this
    # snippet is showing how to access the secret material.
    payload = response.payload.data.decode("UTF-8")
    print("Plaintext: {}".format(payload))
    return payload
    
