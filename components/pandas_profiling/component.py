from kfp.components import InputPath, OutputPath, create_component_from_func


def pandas_profiling_components(
        input_path: str,
        mlpipeline_ui_metadata_path: OutputPath(),
):
    import json
    import pandas as pd
    from pandas_profiling import ProfileReport
    from google.cloud import storage
    import io

    _GCS_PREFIX = "gs://"

    def _read_from_gcs(gcs_path: str):
        storage_client = storage.Client()
        if not gcs_path.startswith(_GCS_PREFIX):
            raise ValueError(
                f"Path must start with 'gs://', found: {gcs_path}")

        bucket_name = gcs_path.split("/")[2]
        file_path = '/'.join(gcs_path.split("/")[3:])
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.get_blob(file_path)
        if not blob:
            raise ValueError(f"Path {gcs_path} do not exists.")

        result = blob.download_as_string()
        return io.BytesIO(result)

    data = _read_from_gcs(input_path)
    df = pd.read_csv(data, index_col=0)
    profile = ProfileReport(df, title="Profiling Report", explorative=True)

    metadata = {
        'outputs': [{
            'type': 'web-app',
            'storage': 'inline',
            'source': profile.to_html(),
        }]
    }
    with open(mlpipeline_ui_metadata_path, 'w') as metadata_file:
        json.dump(metadata, metadata_file)
    return metadata


if __name__ == "__main__":
    create_component_from_func(pandas_profiling_components,
                               output_component_file='component.yml',
                               base_image='python:3.8',
                               packages_to_install=[
                                   'pandas-profiling',
                                   'google-cloud-storage==1.29.0'
                               ])
