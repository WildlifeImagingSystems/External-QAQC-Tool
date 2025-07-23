"""
This file contains the configuration for the notebook as a class.
It is used to set the current project, location and datapull as a class object with the
corresponding methods to set and get the values.

Use this at the top of your notebook to the configuration for the datapull you want to run
processing on.

Date: 2025-06-11
Author: @jonah
"""

from pathlib import Path
import glob
import os
import numpy as np

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

LOCATIONS = {
    'Pattern': ['Clines_Corners', 'Duran_Mesa', 'Gulf_Wind', 'Hatchet_Ridge','Henvey_Inlet',
                'Lanfine', 'Logans_Gap', 'Red_Cloud', 'Santa_Isabel', 'Stillwater', 'Tecolote'],
    'West_OSC': ['Cardinal_Point', 'Bright_Stalk', 'Wildhorse_Mountain']
}

CURRENT_YEAR = '2025'

DATA_BASE_SHORTCUTS = {
    'Core': '.shortcut-targets-by-id/1vqGfsolivsfb3uonHK-cV02VRW_pS7DX',
}

PROJECT_RANGES = {
    'Kahuku': (150, 170),
    'Pattern': (150, 170),
    'West_OSC': (150, 170),
}

INPUT_STORAGE_SERVICE = {
    'Kahuku': 'local',
    'Pattern': 'local',
    'West_OSC': 'azure'
}

#}

DRIVE_LETTER = 'H:/'

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
        # Set database path based on project and year
        
        self.data_base_path = Path(DRIVE_LETTER, DATA_BASE_SHORTCUTS['Core'], 
                                       f'{CURRENT_YEAR} Data/').as_posix()
            
        # Set the video base path
        if self.project == 'Pattern':
            self.video_base_path = Path(DRIVE_LETTER, f'.shortcut-targets-by-id/**/' \
                                        f'{self.location}/').as_posix()
        else:
            self.video_base_path = Path(DRIVE_LETTER, 'Shared Drives', self.project).as_posix()

        # Set wildcard count for paths
        file_structure = FILE_STRUCTURE[self.project]
        if file_structure == 'FLAT':
            self.wildcard_structure = ''
        else:
            self.wildcard_structure = '**/**/**/**'

    # STANDARD PATHS

    def get_cloud_out_path(self) -> str:
        return Path(DRIVE_LETTER, 'Shared Drives/Cloud_Output', self.project,
                    self.location, self.datapull).as_posix()

    def get_data_output_path(self) -> str:
        data_output_path = None
        if self.project in ['Pattern', 'West_OSC']:
            data_output_path = Path(self.data_base_path, self.project, self.location).as_posix()
        elif self.project == 'HKZ':
            data_output_path = Path(self.data_base_path).as_posix()
        else:
            data_output_path = Path(self.data_base_path, self.project).as_posix()
        return data_output_path

    def get_video_paths(self) -> str:
        base = self.video_base_path
        camera_id = f"{self.camera_id}/" if self.camera_id is not None else "*"
        return Path(base, self.datapull, camera_id, self.wildcard_structure, 
                    f"**.{self.file_ext}").as_posix()

    def get_xml_paths(self) -> str:
        base = self.video_base_path
        camera_id = f"{self.camera_id}/" if self.camera_id is not None else "*"
        return Path(base, self.datapull, camera_id, self.wildcard_structure, '**.xml').as_posix()

    def get_file_structure(self) -> str:
        return FILE_STRUCTURE.get(self.project, 'STANDARD')
    
    def get_wind_type(self) -> str:
        return WIND_TYPE.get(self.project, 'Onshore')
    
    def get_split_video_length(self) -> int:
        if FILE_STRUCTURE[self.project] == 'FLAT':
            return -2
        else:
            return -6
        
    def get_database_split_parts(self) -> list:
        if self.project == 'HKZ':
            return [2, -1]
        else:
            return [1, -1]
        
    def get_range(self) -> tuple:
        return PROJECT_RANGES.get(self.project, (150, 170))

    # DATA OUTPUT PATHS

    def get_detections_path(self) -> str:
        return Path(self.get_data_output_path(), 'detections', self.datapull).as_posix()
    
    def get_tracks_path(self) -> str:
        return Path(self.get_data_output_path(), 'tracks', self.datapull).as_posix()

    def get_classified_path(self) -> str:
        return Path(self.get_data_output_path(), 'classified', self.datapull).as_posix()
    
    def get_excluded_path(self) -> str:
        return Path(self.get_data_output_path(), 'excluded', self.datapull).as_posix()
    
    def get_nights_path(self) -> str:
        return Path(self.get_data_output_path(), 'night', self.datapull).as_posix()
    
    def get_finetuning_path(self) -> str:
        return Path(self.get_data_output_path(), 'finetuning', self.datapull).as_posix()
    
    def get_interesting_path(self) -> str:
        return Path(self.get_data_output_path(), 'interesting', self.datapull).as_posix()
    
    def get_threshold_path(self) -> str:
        return Path(self.get_data_output_path(), 'thresh').as_posix()
    
    def get_QAQC_path(self) -> str:
        return Path(self.get_data_output_path(), 'QAQC', self.datapull).as_posix()

    # USING CLOUD PATHS

    def get_processed_detections_db_path(self):
        return Path(self.get_cloud_out_path(), 'detections', self.datapull, '**.db').as_posix()

    def get_processed_tracks_db_path(self):
        return Path(self.get_cloud_out_path(), 'tracks', self.datapull, '**.db').as_posix()

    def get_processed_summary_images_path(self):
        return Path(self.get_cloud_out_path(), 'detections', self.datapull, '**.jpg').as_posix()
    
    def get_processed_classified_db_path(self):
        return Path(self.get_cloud_out_path(), 'classified', self.datapull, '**.db').as_posix()

    def get_processed_classified_images_path(self):
        return Path(self.get_cloud_out_path(), 'classified', self.datapull, '**.jpg').as_posix()
    
    # USING DATA OUTPUT PATHS

    def get_detections_db_path(self):
        return Path(self.get_detections_path(), '**', '**_Detections.db').as_posix()

    def get_tracks_db_path(self):
        return Path(self.get_tracks_path(), '**', '**_Tracks.db').as_posix()

    def get_summary_images_path(self):
        return Path(self.get_detections_path(), '**', '**_Summary.jpg').as_posix()

    def get_classified_dbs_path(self, turbine='**'):
        return Path(self.get_classified_path(), turbine, '**_Classified.db').as_posix()

    def get_classified_images_path(self):
        return Path(self.get_classified_path(), '**', '**_Class.jpg').as_posix()

    def get_excluded_images_path(self):
        return Path(self.get_excluded_path(), '**', '**.jpg').as_posix()

    def get_night_dbs_path(self):
        return Path(self.get_nights_path(), '**', '**.db').as_posix()

    def get_finetuning_images_path(self):
        return Path(self.get_finetuning_path(), '**', '**_Class.jpg').as_posix()
    
    def get_threshold_csv_path(self):
        return Path(self.get_threshold_path(), '**_Thresh.csv').as_posix()

    def get_QAQC_folder_path(self):
        return Path(self.get_QAQC_path(), '**').as_posix()