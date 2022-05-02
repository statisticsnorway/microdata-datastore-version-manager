from typing import Optional

from pydantic import BaseModel, Extra


class NewPendingOperationRequest(BaseModel, extra=Extra.forbid):
    operationType: str
    datasetName: str
    description: str


class UpdatePendingOperationRequest(BaseModel, extra=Extra.forbid):
    releaseStatus: str
    operationType: Optional[str]


class PendingOperation(BaseModel, extra=Extra.forbid):
    name: str
    description: str
    operation: str
    releaseStatus: str


class ApplyBumpManifestoRequest(BaseModel, extra=Extra.forbid):
    pendingOperations: list[PendingOperation]
    description: str
