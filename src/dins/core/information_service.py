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
"""Contains logic for public file information storage, retrieval and deletion."""

import logging

import ghga_event_schemas.pydantic_ as event_schemas
from hexkit.protocols.dao import ResourceNotFoundError

from dins.adapters.inbound.dao import DatasetDaoPort, FileInformationDaoPort
from dins.core.models import (
    DatasetFileAccessions,
    DatasetFileInformation,
    FileAccession,
    FileInformation,
)
from dins.ports.inbound.information_service import InformationServicePort

log = logging.getLogger(__name__)


class InformationService(InformationServicePort):
    """A service that handles storage and deletion of relevant metadata for files
    registered with the Internal File Registry service.
    """

    def __init__(
        self,
        *,
        dataset_dao: DatasetDaoPort,
        file_information_dao: FileInformationDaoPort,
    ):
        self._dataset_dao = dataset_dao
        self._file_information_dao = file_information_dao

    async def delete_dataset_information(self, dataset_id: str):
        """Delete dataset to file ID mapping when the corresponding dataset is deleted."""
        try:
            await self._dataset_dao.get_by_id(id_=dataset_id)
        except ResourceNotFoundError:
            log.info(f"Mapping for dataset with id {dataset_id} does not exist.")
            return

        await self._dataset_dao.delete(id_=dataset_id)
        log.info(f"Successfully deleted mapping for dataset with id {
                 dataset_id}.")

    async def deletion_requested(self, file_id: str):
        """Handle deletion requests for information associated with the given file ID."""
        try:
            await self._file_information_dao.get_by_id(id_=file_id)
        except ResourceNotFoundError:
            log.info(f"Information for file with id {file_id} does not exist.")
            return

        await self._file_information_dao.delete(id_=file_id)
        log.info(f"Successfully deleted entries for file with id {file_id}.")

    async def register_dataset_information(
        self, metadata_dataset: event_schemas.MetadataDatasetOverview
    ):
        """Extract dataset to file ID mapping and store it."""
        dataset_accession = metadata_dataset.accession
        file_accessions = [file.accession for file in metadata_dataset.files]

        dataset_accessions = DatasetFileAccessions(
            accession=dataset_accession, file_accessions=file_accessions
        )

        # inverted logic due to raw pymongo exception exposed by hexkit
        try:
            existing_mapping = await self._dataset_dao.get_by_id(id_=dataset_accession)
            log.debug(f"Found existing information for dataset {dataset_accession}")
            # Only log if information to be inserted is a mismatch
            if existing_mapping != dataset_accessions:
                information_exists = self.MismatchingDatasetAlreadyRegistered(
                    dataset_id=dataset_accession
                )
                log.error(information_exists)
        except ResourceNotFoundError:
            await self._dataset_dao.insert(dataset_accessions)
            log.debug(f"Successfully inserted file accession mapping for dataset {
                      dataset_accession}.")

    async def register_file_information(
        self, file_registered: event_schemas.FileInternallyRegistered
    ):
        """Store information for a file newly registered with the Internal File Registry."""
        file_information = FileInformation(
            accession=file_registered.file_id,
            size=file_registered.decrypted_size,
            sha256_hash=file_registered.decrypted_sha256,
        )
        file_id = file_information.accession

        # inverted logic due to raw pymongo exception exposed by hexkit
        try:
            existing_information = await self._file_information_dao.get_by_id(
                id_=file_id
            )
            log.debug(f"Found existing information for file {file_id}")
            # Only log if information to be inserted is a mismatch
            if existing_information != file_information:
                information_exists = self.MismatchingInformationAlreadyRegistered(
                    file_id=file_id
                )
                log.error(information_exists)
        except ResourceNotFoundError:
            await self._file_information_dao.insert(file_information)
            log.debug(f"Successfully inserted information for file {file_id} ")

    async def batch_serve_information(
        self, dataset_accession: str
    ) -> DatasetFileInformation:
        """Retrieve stored public information for the given dataset ID to be served by the API."""
        try:
            dataset = await self._dataset_dao.get_by_id(dataset_accession)
            log.debug(f"Found mapping for dataset {dataset_accession}.")
        except ResourceNotFoundError as error:
            dataset_mapping_not_found = self.DatasetNotFoundError(
                dataset_accession=dataset_accession
            )
            log.warning(dataset_mapping_not_found)
            raise dataset_mapping_not_found from error

        file_information: list[FileAccession | FileInformation] = []

        for file_accession in dataset.file_accessions:
            try:
                current_file_information = await self.serve_information(file_accession)
                file_information.append(current_file_information)
            except self.InformationNotFoundError:
                file_information.append(FileAccession(accession=file_accession))
        return DatasetFileInformation(
            accession=dataset_accession, file_information=file_information
        )

    async def serve_information(self, file_id: str) -> FileInformation:
        """Retrieve stored public information for the given file ID to be served by the API."""
        try:
            file_information = await self._file_information_dao.get_by_id(file_id)
            log.debug(f"Found information for file {file_id}.")
        except ResourceNotFoundError as error:
            information_not_found = self.InformationNotFoundError(file_id=file_id)
            log.warning(information_not_found)
            raise information_not_found from error

        return file_information
