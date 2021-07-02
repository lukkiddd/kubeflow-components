# Name

Profiling CSV file using pandas profiling

# Labels

Pandas, Google Cloud Storage, Pandas Profiling, Data exploration

# Summary

A Kubeflow Pipeline component to profiling csv data using `pandas-profiling`.  Then save the output as an HTML to display on kubeflow pipeline UI.

# Details

## Runtime arguments

| Argument    | Description | Optional | Data type | Accepted values | Default |
|-------------|-------------|----------|-----------|-----------------|---------|
| input_path  | Input path  | No | String |  gs://bucket/path/to/data.csv| |


## Output

| Name | Description | Type |
|------|-------------|------|
| mlpipeline_ui_metadata_path | Local ML pipeline ui metadata path | Path



### How to use

#### 1. Load the component using KFP SDK

```python
from kfp.components import load_component_from_uri
component_uri = "https://raw.githubusercontent.com/lukkiddd/kubeflow-components/main/components/pandas_profiling/CSV/component.yml"
profiling_op = load_component_from_url(component_uri)
```

#### 2. Set up the pipeline

```python
from kfp.dsl import pipeline

@pipeline(name='my-pipeline')
def my_pipeline(
    input_path="gs://bucket/mydata.csv"
):
    profiling = profiling_op(input_path, expectation_suite_path)
```

#### 3. Compile the pipeline


```python
pipeline_func = pipeline
pipeline_filename = pipeline_func.__name__ + '.zip'
import kfp.compiler as compiler
compiler.Compiler().compile(pipeline_func, pipeline_filename)
```

#### 4. Submit the pipeline for execution

```python
EXPERIMENT_NAME = "my_experiment"

import kfp
client = kfp.Client()
experiment = client.create_experiment(EXPERIMENT_NAME)

#Submit a pipeline run
run_name = pipeline_func.__name__ + ' run'
run_result = client.run_pipeline(experiment.id, run_name, pipeline_filename, arguments)
```