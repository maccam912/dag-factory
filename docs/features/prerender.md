# Prerender DAG code

DAG Factory can generate Python source files from YAML configurations. Set an `output_dir` when building DAGs, and the resulting code is saved for inspection.

```title="example_prerender.yml"
--8<-- "dev/dags/prerender/example_prerender.yml"
```

Running the following will create `example_prerender.py` in the chosen directory:

```python
from dagfactory import DagFactory

DagFactory(config_filepath="dev/dags/prerender/example_prerender.yml", output_dir="dev/dags/prerender").build_dags()
```

The generated code resembles:

```title="example_prerender.py"
--8<-- "dev/dags/prerender/example_prerender.py"
```
