"""
Reference: https://github.com/kubeflow/pipelines/blob/master/components/great-expectations/validate/CSV/component.py

"""
from kfp.components import InputPath, OutputPath, create_component_from_func

def validate_csv_using_greatexpectations(
    input_data: InputPath(),
    expectation_suite: InputPath(),
    mlpipeline_ui_metadata_path: OutputPath(),
):
    import json
    import great_expectations as ge
    from great_expectations.render import DefaultJinjaPageView
    from great_expectations.render.renderer import ValidationResultsPageRenderer
    from google.cloud import storage
    import io

    _JSON = "json"
    _BYTES = "bytes"
    _GCS_PREFIX = "gs://"

    def _read_from_gcs(gcs_path: str, return_as=_JSON):
        storage_client = storage.Client()
        if not gcs_path.startswith(_GCS_PREFIX):
            raise ValueError(f"Path must start with 'gs://', found: {gcs_path}")
        
        bucket_name = gcs_path.split("/")[2]
        file_path = '/'.join(gcs_path.split("/")[3:])
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.get_blob(file_path)
        if not blob:
            raise ValueError(f"Path {gcs_path} do not exists.")
            
        result = blob.download_as_string()
        
        if return_as == _JSON:
            return json.loads(result)
        elif return_as == _BYTES:
            return io.BytesIO(result)
        return result
        
    expectation_suite = _read_from_gcs(expectation_suite_path, return_as=_JSON)
    data = _read_from_gcs(csv_path, return_as=_BYTES)
    df = ge.read_csv(data, expectation_suite=expectation_suite)
    result = df.validate()

    document_model = ValidationResultsPageRenderer().render(result)
    
    metadata = {
        'outputs' : [{
          'type': 'web-app',
          'storage': 'inline',
          'source': DefaultJinjaPageView().render(document_model),
        }]
    }
    with open(mlpipeline_ui_metadata_path, 'w') as metadata_file:
        json.dump(metadata, metadata_file)
    return metadata

if __name__ == "__main__":
    create_component_from_func(
        validate_csv_using_greatexpectations,
        output_component_file='component.yml',
        base_image='python:3.8',
        packages_to_install=['great-expectations==0.13.11', 'google-cloud-storage==1.29.0']
    )