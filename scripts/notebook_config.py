"""
This file contains the configuration for the notebook as a class.
It is used to set the current project, location and datapull as a class object with the
corresponding methods to set and get the values.

Use this at the top of your notebook to the configuration for the datapull you want to run
processing on.

Date: 2025-06-11
Author: @wis
"""

from pathlib import Path

import scripts.storage_services as storage_services


# VARIABLES

# Set these by year and project
FILE_STRUCTURE = {
    'Kahuku': 'STANDARD',
    'Pattern': 'STANDARD',
    'West_OSC': 'STANDARD',
}

WIND_TYPE = {
    'Kahuku': 'Onshore',
    'Pattern': 'Onshore',
    'West_OSC': 'Onshore'
}


CURRENT_YEAR = '2025'


INPUT_STORAGE_SERVICE = {
    'Kahuku': 'local',
    'Pattern': 'local',
    'West_OSC': 'azure'
}

# CLASS
class NotebookConfig:
    def __init__(self, project: str, location: str, datapull: str, container_url: str, token:str,
                 camera_id: str = None, file_ext: str = 'mkv'):
        self.project = project
        self.location = location
        self.datapull = datapull
        self.camera_id = camera_id
        self.file_ext = file_ext

        # Set the notebook configuration based on the project, location, and datapull
        self.set_core_paths()

        if INPUT_STORAGE_SERVICE[self.project] == 'local':
            self.data_access = storage_services.LocalStorageService(self.video_base_path,
                                                                    self.datapull,
                                                                    self.get_video_paths(),
                                                                    self.get_xml_paths()
                                                                    )
        elif INPUT_STORAGE_SERVICE[self.project] == 'azure':
            datapull_path = f"{CURRENT_YEAR}/{self.location}/{self.datapull}"
            self.data_access = storage_services.AzureBlobStorageService(container_url, token,
                                                                         datapull_path)
    
    def set_core_paths(self):
        """
        This sets the common configurations for the notebook based on the project, location, and 
        datapull. It sets the data base path, processing parameters path, and mkv base path.
        """            
        # Set wildcard count for paths
        file_structure = FILE_STRUCTURE[self.project]
        if file_structure == 'FLAT':
            self.wildcard_structure = ''
        else:
            self.wildcard_structure = '**/**/**/**'

    # STANDARD PATHS
    def get_video_paths(self) -> str:
        base = self.video_base_path
        camera_id = f"{self.camera_id}/" if self.camera_id is not None else "*"
        return Path(base, self.datapull, camera_id, self.wildcard_structure, 
                    f"**.{self.file_ext}").as_posix()

    def get_xml_paths(self) -> str:
        base = self.video_base_path
        camera_id = f"{self.camera_id}/" if self.camera_id is not None else "*"
        return Path(base, self.datapull, camera_id, self.wildcard_structure, '**.xml').as_posix()