from typing import Optional

from pydantic import BaseModel, Extra


class NewPendingOperationRequest(BaseModel, extra=Extra.forbid):
    operation_type: str
    dataset_name: str
    description: str


class UpdatePendingOperationRequest(BaseModel, extra=Extra.forbid):
    release_status: str
    operation_type: Optional[str]


class RemovePendingOperationRequest(BaseModel, extra=Extra.forbid):
    dataset_name: str


class ApplyBumpManifestoRequest(BaseModel, extra=Extra.forbid):
    ...
