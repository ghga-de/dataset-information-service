# Copyright 2021 - 2026 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
from hexkit.protocols.dao import NoHitsFoundError, ResourceNotFoundError

from dins.adapters.outbound.dao import (
    DatasetDaoPort,
    FileAccessionMapDaoPort,
    FileInformationDaoPort,
    PendingFileInfoDaoPort,
)
from dins.core.models import (
    DatasetFileAccessions,
    DatasetFileInformation,
    FileAccession,
    FileAccessionMap,
    FileInformation,
    FileInternallyRegistered,
    PendingFileInfo,
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
        accession_map_dao: FileAccessionMapDaoPort,
        dataset_dao: DatasetDaoPort,
        file_information_dao: FileInformationDaoPort,
        pending_file_info_dao: PendingFileInfoDaoPort,
    ):
        self._accession_map_dao = accession_map_dao
        self._dataset_dao = dataset_dao
        self._file_information_dao = file_information_dao
        self._pending_file_info_dao = pending_file_info_dao

    async def delete_dataset_information(self, dataset_id: str):
        """Delete dataset to file accession mapping when the corresponding dataset is deleted."""
        try:
            await self._dataset_dao.get_by_id(dataset_id)
        except ResourceNotFoundError:
            log.info("Mapping for dataset with id %s does not exist.", dataset_id)
            return

        await self._dataset_dao.delete(dataset_id)
        log.info("Successfully deleted mapping for dataset with id %s.", dataset_id)

    async def delete_file_information(self, accession: str):
        """Delete FileInformation for the given accession and clean up any related pending record.

        If no FileInformation exists for the accession, logs and returns early.
        """
        try:
            await self._file_information_dao.delete(accession)
        except ResourceNotFoundError:
            log.info(
                "Information for file with accession %s does not exist.", accession
            )
        else:
            log.info(
                "Successfully deleted entries for file with accession %s.", accession
            )

    async def register_dataset_information(
        self, dataset: event_schemas.MetadataDatasetOverview
    ):
        """Extract dataset to file ID mapping and store it."""
        dataset_accession = dataset.accession
        file_accessions = [file.accession for file in dataset.files]

        dataset_file_accessions = DatasetFileAccessions(
            accession=dataset_accession, file_accessions=file_accessions
        )

        await self._dataset_dao.upsert(dataset_file_accessions)

    async def register_file_information(self, file_information: FileInformation):
        """Store information for a file newly registered with the Internal File Registry."""
        accession = file_information.accession

        # inverted logic due to raw pymongo exception exposed by hexkit
        try:
            existing_information = await self._file_information_dao.get_by_id(accession)
        except ResourceNotFoundError:
            await self._file_information_dao.insert(file_information)
            log.debug("Successfully inserted information for file %s.", accession)
        else:
            log.debug("Found existing information for file %s.", accession)
            # Only log if information to be inserted is a mismatch
            if existing_information.model_dump() != file_information.model_dump():
                information_exists = self.MismatchingFileInformationAlreadyRegistered(
                    accession=accession
                )
                log.error(information_exists)
                raise information_exists

    async def serve_dataset_information(
        self, dataset_id: str
    ) -> DatasetFileInformation:
        """Retrieve stored public information for the given dataset ID to be served by the API."""
        try:
            dataset = await self._dataset_dao.get_by_id(dataset_id)
            log.debug("Found mapping for dataset %s.", dataset_id)
        except ResourceNotFoundError as error:
            dataset_not_found = self.DatasetNotFoundError(dataset_accession=dataset_id)
            log.debug(dataset_not_found)
            raise dataset_not_found from error

        file_ids_mapping = {"accession": {"$in": dataset.file_accessions}}

        file_informations = [
            single_file_information
            async for single_file_information in self._file_information_dao.find_all(
                mapping=file_ids_mapping
            )
        ]

        matched_accessions = {
            file_information.accession for file_information in file_informations
        }
        missing_accessions = set(dataset.file_accessions) - matched_accessions

        log.debug(
            f"{dataset_id}: File information found: [{matched_accessions}]; Missing: [{missing_accessions}]"
        )

        file_accessions = [
            FileAccession(accession=accession) for accession in missing_accessions
        ]

        combined = sorted(
            file_informations + file_accessions, key=lambda x: x.accession
        )

        return DatasetFileInformation(accession=dataset_id, file_information=combined)

    async def serve_file_information(self, accession: str) -> FileInformation:
        """Retrieve stored public information for the given file accession to be served by the API."""
        try:
            file_information = await self._file_information_dao.get_by_id(accession)
            log.debug("Found information for file %s.", accession)
        except ResourceNotFoundError as error:
            information_not_found = self.InformationNotFoundError(accession=accession)
            log.debug(information_not_found)
            raise information_not_found from error

        return file_information

    async def _store_pending_file_info(self, *, file: FileInternallyRegistered) -> None:
        """Store essential file registration fields as a PendingFileInfo record.

        No-ops on exact duplicates. Logs an error if differing data is already stored.
        """
        pending = PendingFileInfo(
            file_id=file.file_id,
            decrypted_size=file.decrypted_size,
            decrypted_sha256=file.decrypted_sha256,
            storage_alias=file.storage_alias,
        )
        try:
            existing_pending = await self._pending_file_info_dao.get_by_id(file.file_id)
        except ResourceNotFoundError:
            await self._pending_file_info_dao.insert(pending)
            log.debug("Stored pending file info for file_id %s.", file.file_id)
        else:
            if existing_pending.model_dump() == pending.model_dump():
                log.info(
                    "Duplicate pending file info received for file_id %s, skipping.",
                    file.file_id,
                )
            else:
                mismatch = self.MismatchingPendingFileInfoExists(file_id=file.file_id)
                log.error(mismatch)
                raise mismatch

    async def handle_file_internally_registered(
        self, *, file: FileInternallyRegistered
    ) -> None:
        """Decide how to handle a new file registration.

        If a corresponding FileAccessionMap already exists, merge and store FileInformation.
        If not, temporarily store the essential fields as a PendingFileInfo instance.
        """
        try:
            accession_map = await self._accession_map_dao.get_by_id(file.file_id)
        except ResourceNotFoundError:
            await self._store_pending_file_info(file=file)
        else:
            file_information = FileInformation(
                accession=accession_map.accession,
                size=file.decrypted_size,
                sha256_hash=file.decrypted_sha256,
                storage_alias=file.storage_alias,
            )
            await self.register_file_information(file_information=file_information)

    async def store_accession_map(self, *, accession_map: FileAccessionMap) -> None:
        """Store an accession map in the database.

        Triggers an error if a FileInformation record already exists for this accession.
        Otherwise upserts freely, then checks for a pending file info to merge.
        """
        try:
            await self._file_information_dao.get_by_id(accession_map.accession)
        except ResourceNotFoundError:
            pass
        else:
            log.error(
                "Accession map update rejected for accession %s: FileInformation already registered.",
                accession_map.accession,
            )
            return

        try:
            existing_map = await self._accession_map_dao.get_by_id(
                str(accession_map.file_id)
            )
        except ResourceNotFoundError:
            pass
        else:
            if existing_map == accession_map:
                log.info(
                    "Duplicate accession map received for accession %s, file_id %s, skipping.",
                    accession_map.accession,
                    accession_map.file_id,
                )
                return

        await self._accession_map_dao.upsert(accession_map)
        log.info(
            "Upserted accession map for accession %s, file ID %s.",
            accession_map.accession,
            accession_map.file_id,
        )

        try:
            pending = await self._pending_file_info_dao.get_by_id(
                str(accession_map.file_id)
            )
        except ResourceNotFoundError:
            return

        file_information = FileInformation(
            accession=accession_map.accession,
            size=pending.decrypted_size,
            sha256_hash=pending.decrypted_sha256,
            storage_alias=pending.storage_alias,
        )
        await self.register_file_information(file_information=file_information)
        await self._pending_file_info_dao.delete(str(accession_map.file_id))
        log.info(
            "Merged pending file info for file_id %s with accession %s.",
            accession_map.file_id,
            accession_map.accession,
        )

    async def delete_accession_map(self, *, accession: str) -> None:
        """Delete the accession map entry identified by the given accession.

        Looks up the map by accession to retrieve the file_id, then deletes by file_id
        (the primary key). Logs and returns early if no entry exists for the accession.
        """
        try:
            accession_map = await self._accession_map_dao.find_one(
                mapping={"accession": accession}
            )
        except NoHitsFoundError:
            log.info("Accession map for accession %s does not exist.", accession)
            return

        await self._accession_map_dao.delete(str(accession_map.file_id))
        log.info("Accession mapping deleted for accession %s", accession)
