from abc import ABC, abstractmethod
import os
from pathlib import PurePath
import json

from azure.storage.blob import ContainerClient
from glob import glob

AZURE_LOCAL_DIRECTORY = "temp_azure_storage/"


class InputBase(ABC):
    """
    Abstract class which defines required arguments for an InputBase object.
    """
    @abstractmethod
    def list_cam_ids(self) -> list:
        """Returns a list of camera ids."""
        pass

    @abstractmethod
    def list_video_paths(self, camera_id: str=None, file_ext:str='.mkv') -> list:
        """Returns a list of video paths. If no camera id is provided, it returns all 
        video paths.
        """
        pass

    @abstractmethod
    def list_xml_paths(self, camera_id: str=None) -> list:
        """Returns a list of XML paths. If no camera id is provided, it returns all XML paths."""
        pass

    @abstractmethod
    def list_mac_addresses(self) -> dict:
        """Returns a dictionary where the mac address for each camera is stored for each day of the 
        datapull. 
        
        Example: mac_addresses[camera_id][date] = mac_address
        """
        pass

    @abstractmethod
    def get_file(self, path:str) -> str:
        """Downloads file from its storage location to the local machine and returns a path to a 
        local copy of a file. This is needed when you need to actually work with the file and 
        performance of this will vary between different services.
        """
        pass

    @abstractmethod
    def get_file_size(self, path:str) -> int:
        """Returns the size of the file at the specified path."""
        pass


class AzureBlobStorageService(InputBase):
    container_url = None
    token = None
    datapull_folder = None
    azure_save_path_base = None

    def __init__(self, container_url:str, token:str, datapull_folder:str, 
                 azure_save_path_base:str=AZURE_LOCAL_DIRECTORY):
        """
        Initializes an interface to interact with an Azure storage blob.
        
        Args:
            secret_path: a path to a config file with the container and token information to connect
                to an azure blob.
            datapull_folder: path from the container to a specific datapull (includes location)
            azure_save_path_base: a path that is used to locally store any videos downloaded from 
                azure
        """
        self.datapull_folder = datapull_folder
        self.azure_save_path_base = azure_save_path_base

        self.blob_storage_client = self.get_azure_blob_storage_client(container_url, token)

        # initializes video and xml lists so that it only needs to be called once
        self.video_list = None
        self.xml_list = None


    """Azure Specific Method"""
    def get_azure_blob_storage_client(self, container_url, token):
        container_url_with_token = container_url + '?' + token
        azure_blob_storage_client = ContainerClient.from_container_url(container_url_with_token)
        return azure_blob_storage_client

    def download_file(self, file_path):
        """Downloads a blob from the container."""

        blob_client = self.blob_storage_client.get_blob_client(file_path)
        blob_data = blob_client.download_blob()

        # raise error if download failed
        if blob_data == None:
            raise ValueError(f"No data downloaded from blob. File path: {file_path}")

        return blob_data      
            
    """Data Access Methods"""
    def list_cam_ids(self):
        # check if video list is already defined        
        if self.video_list == None:
            self.video_list = self.get_video_paths()

        camera_ids = []
        for video_path in self.video_list:
            path_without_parent_folder = video_path.split(self.datapull_folder, 1)[1]
            camera_id = path_without_parent_folder.split("/")[1]
            camera_ids.append(camera_id)

        return sorted(set(camera_ids))

    def list_video_paths(self, camera_id=None, file_ext='.mkv'):
        # check if video list is already defined        
        if self.video_list == None:
            prefix = f"{self.datapull_folder}/"
            blobs = self.blob_storage_client.list_blobs(name_starts_with=prefix)
            self.video_list = [blob.name for blob in blobs if blob.name.endswith(file_ext)]
                            
        # selects only from camera id if given
        if camera_id is not None:
            return [path for path in self.video_list if camera_id in path]
        
        return self.video_list

    def list_xml_paths(self, camera_id=None):
        # check if xml list is already defined        
        if self.xml_list is None:
            if camera_id:
                prefix = f"{self.datapull_folder}/{camera_id}/"
            else:
                prefix = f"{self.datapull_folder}/"

            blobs = self.blob_storage_client.list_blobs(name_starts_with=prefix)
            self.xml_list = [blob.name for blob in blobs if blob.name.endswith('.xml')]
                            
        # selects only from camera id if given
        if camera_id is not None:
            return [path for path in self.xml_list if camera_id in path]
        
        return self.xml_list

    def list_mac_addresses(self):
        if self.video_list is None:
            self.video_list = self.get_video_paths()

        # clips all 
        mac_paths = ['/'.join(PurePath(path).parts[:-2]) for path in self.video_list]
        unique_mac_paths = set([str(PurePath(path)) for path in mac_paths])
        mac_addresses = {}
        for camera_id in self.list_cam_ids():
            mac_addresses[camera_id] = {} 
            mac_folders = [path for path in unique_mac_paths if camera_id in path]
            for folder in mac_folders:
                path_paths = PurePath(folder).parts

                # parse path for date
                date = path_paths[-3]

                # parse path for mac address
                mac_folder_name = path_paths[-1]
                mac_address = mac_folder_name.split('_')[-1]
                if len(mac_address) == 12: #  mac addresses are always 12 characters long
                    mac_addresses[camera_id][date] = mac_address

        return mac_addresses

    def get_file(self, path):
        # create directory to save the file locally
        save_path = f"{self.azure_save_path_base}/{path}"
        os.makedirs(os.path.dirname(save_path), exist_ok=True)

        # download and save the file
        data = self.download_file(path)
        with open(save_path, 'wb') as file:
            file.write(data.readall())

        # return the path to the file
        return f"{os.getcwd()}/{save_path}"

    def get_file_size(self, path):
        properties = self.blob_storage_client.get_blob_client(path).get_blob_properties()
        return properties.size  # Azure returns the size of a file in bytes


class LocalStorageService(InputBase):
    video_base_path = None
    datapull = None

    def __init__(self, video_base_path, datapull, video_search_path, xml_search_path):
        """
        Initializes an interface to interact with locally stored files.
        
        Args:
            video_base_path: the base path where all of this datapulls videos are stored.
            datapull: the datapull being accessed.
            video_search_path: a path with wildcards (**) which can be used to list all videos.
            xml_search_path: a path with wildcards (**) which can be used to list all xmls.
        """
        self.video_base_path = video_base_path
        self.datapull = datapull
        self.video_search_path = video_search_path
        self.xml_search_path = xml_search_path

        # initialize so that lists are only generated once
        self.video_list = None
        self.xml_list = None


    """Data Access Methods"""
    def list_cam_ids(self):
        camera_id_pattern = f"{self.video_base_path}/{self.datapull}/**"
        files = glob(camera_id_pattern)
        camera_ids = [os.path.basename(folder) for folder in files if os.path.isdir(folder)]

        # remove .ini files
        camera_id_list = [camera_id for camera_id in set(camera_ids) if 'ini' not in camera_id]
        return sorted(camera_id_list)

    def list_video_paths(self, camera_id=None, file_ext='.mkv'):
        if self.video_list is None:
            files = glob(self.video_search_path)

            self.video_list = [file for file in files if file.endswith(file_ext)]
        
        # only include videos from camera id if specified
        if camera_id:
            return [path for path in self.video_list if camera_id in path]

        return self.video_list

    def list_xml_paths(self,camera_id=None):
        if self.xml_list is None:

            files = glob(self.xml_search_path)

            self.xml_list = [file for file in files if file.endswith('.xml')]

        # only include xmls from camera id if specified
        if camera_id:
            return [path for path in self.xml_list if camera_id in path]

        return self.xml_list

    def list_mac_addresses(self):
        mac_addresses = {}
        for camera_id in self.list_cam_ids():
            mac_addresses[camera_id] = {} 
            mac_folders = glob(f"{self.video_base_path}/{self.datapull}/{camera_id}/**/**/**")
            for folder in mac_folders:
                if os.path.isdir(folder):
                    path_paths = PurePath(folder).parts

                    # parse path for date
                    date = path_paths[-3]

                    # parse path for mac address
                    mac_folder_name = path_paths[-1]
                    mac_address = mac_folder_name.split('_')[-1]
                    if len(mac_address) == 12: #  mac addresses are always 12 characters long
                        mac_addresses[camera_id][date] = mac_address

        return mac_addresses

    def get_file(self, path):
        return path

    def get_file_size(self, path):
        return os.stat(path).st_size # file size is returned in bytes
    

