from __future__ import annotations

from io import BytesIO
import json

import boto3
import pandas as pd


class R2Storage:
    def __init__(
        self,
        endpoint_url: str | None,
        access_key_id: str | None,
        secret_access_key: str | None,
        bucket_evidence: str | None,
        bucket_data: str | None,
        region_name: str = "auto",
    ):
        self.bucket_evidence = bucket_evidence
        self.bucket_data = bucket_data
        self.client = None
        if endpoint_url and access_key_id and secret_access_key:
            self.client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
                region_name=region_name,
            )

    def available(self) -> bool:
        return self.client is not None

    def put_text(self, key: str, text: str, evidence: bool = False) -> None:
        if not self.available():
            return
        bucket = self.bucket_evidence if evidence else self.bucket_data
        if not bucket:
            return
        self.client.put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"), ContentType="text/plain")

    def put_json(self, key: str, payload: dict, evidence: bool = False) -> None:
        self.put_text(key, json.dumps(payload, ensure_ascii=False, indent=2), evidence=evidence)

    def put_parquet(self, key: str, df: pd.DataFrame, evidence: bool = False) -> None:
        if not self.available():
            return
        bucket = self.bucket_evidence if evidence else self.bucket_data
        if not bucket:
            return
        bio = BytesIO()
        df.to_parquet(bio, index=False)
        bio.seek(0)
        self.client.put_object(Bucket=bucket, Key=key, Body=bio.getvalue(), ContentType="application/octet-stream")
