import storage_service
import tqdm

class VideoTiming:

    def __init__(self, container_url:str, token:str, datapull_folder:str):
        datapull_folder = 
        storage_service = storage_service.AzureBlobStorageService(container_url, token, site, datapull)

    def get_all_file(): 
        zero_size_files = []
        file_sizes = []
        xml_count = 0
        mkv_count = 0

        for ind_turbine, turbine in tqdm(enumerate(turbine_list)):
            file_sizes = []
            for ind_date, date in  enumerate(date_list):
                date_str = datetime.strftime(date, "%Y%m%d")
                for datapull in cfg.config['datapulls']:
                    if FLAT_FILE_STRUCTURE:
                        search_path = f'{file_path}{location}/{datapull}/{turbine}/{date_str}**{file_ext}'
                        xml_search_path = ''
                    else:
                        search_path = f'{file_path}{location}/{datapull}/{turbine}/**/**/**/**/{date_str}**{file_ext}'
                        xml_search_path = f'{file_path}{location}/{datapull}/{turbine}/**/**/**/**/{date_str}**.xml'
                
                file_list = glob.glob(search_path)
                count_detect_files_array[ind_turbine,ind_date] = len(file_list)
                count_xmls[ind_turbine,ind_date] = len(glob.glob(xml_search_path))

                # Check files for size
                for file in file_list:
                    if os.stat(file).st_size == 0:
                        zero_size_files.append(file)
                    file_sizes.append(os.stat(file).st_size)
                
                # Count xml and mkv files to check for partial upload
                if not FLAT_FILE_STRUCTURE:
                    xml_list = glob.glob(xml_search_path)
                    xml_count += len(xml_list)
                    mkv_count += len(file_list)
                    if len(xml_list) != len(file_list):
                        print(f'turbine: {turbine_list[ind_turbine]}')
                        print(f'date: {date_list[ind_date]}'