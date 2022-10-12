import os
import json
import tempfile

from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook


class ExampleDataToGCSOperator(BaseOperator):
    """Operator that creates example JSON data and writes it to GCS.
    Args:
        task_id: (templated) sensor data left bound
        run_date: (templated) sensor data right bound
        gcp_conn_id: Airflow connection for the GCP service account
        gcs_bucket: name of the target GCS bucket
    """
    template_fields = ('run_date', )

    @apply_defaults
    def __init__(
        self,
        run_date: str,
        gcp_conn_id: str,
        gcs_bucket: str,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.run_date = run_date
        self.gcp_conn_id = gcp_conn_id
        self.gcs_bucket = gcs_bucket

    def execute(self, context):
        """Create an example JSON and write it to a GCS bucket. """
        example_data = {'run_date': self.run_date, 'example_data': 12345}
        gcs_file_path = f"example_data_{context['ds_nodash']}.json"

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = os.path.join(tmp_dir, gcs_file_path)
            self.log.info(f"Writing example data to {tmp_path}.")
            with open(tmp_path, 'w') as handle:
                self.log.info(f"Writing example data to {tmp_path}.")
                json.dump(example_data, handle)

            gcs_hook = GCSHook(self.gcp_conn_id)
            gcs_hook.upload(
               bucket_name=self.gcs_bucket,
               object_name=gcs_file_path,
               filename=tmp_path,
            )