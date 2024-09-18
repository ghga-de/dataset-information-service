# Copyright 2021 - 2024 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Models for internal representation"""

from pydantic import BaseModel, Field, PositiveInt


class FileInformation(BaseModel):
    """Public information container for files registered with the Internal File
    Registry service.
    """

    file_id: str = Field(
        ...,
        description="Public identifier of the file associated with the given information",
    )
    size: PositiveInt = Field(..., description="Size of the unencrypted file in bytes.")
    sha256_hash: str = Field(
        ...,
        description="SHA256 hash of the unencrypted file content encoded as hexadecimal "
        " values as produced by hashlib.hexdigest().",
    )


class DatasetFileIDs(BaseModel):
    """Contains ID of a dataset and its contained files."""

    dataset_id: str = Field(..., description="Public accesion of a dataset.")
    file_ids: list[str] = Field(
        ...,
        description="Public accesions for all files included in the corresponding dataset.",
    )


class DatasetInformation(BaseModel):
    """Container bundling public information for a dataset."""

    dataset_id: str = Field(..., description="Public accession of a dataset.")
    file_information: list[FileInformation] = Field(
        ..., description="Public information on all files belonging to a dataset."
    )
