from typing import Optional

from pydantic import BaseModel, Extra


class NewPendingOperationRequest(BaseModel, extra=Extra.forbid):
    operationType: str
    datasetName: str
    description: str


class UpdatePendingOperationRequest(BaseModel, extra=Extra.forbid):
    releaseStatus: str
    operationType: Optional[str]


class RemovePendingOperationRequest(BaseModel, extra=Extra.forbid):
    datasetName: str


class ApplyBumpManifestoRequest(BaseModel, extra=Extra.forbid):
    ...
