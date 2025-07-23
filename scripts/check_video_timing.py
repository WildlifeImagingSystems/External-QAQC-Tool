from tqdm import tqdm
import numpy as np
from pathlib import PurePath
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt

def create_heatmap(count_detect_files_array, date_list, config, camera_ids):
    MIN_VAL = 0
    MAX_VAL = count_detect_files_array.max()
    fig = plt.figure(figsize=[12,4],facecolor='gray')
    ax = fig.add_subplot()
    im = ax.imshow(count_detect_files_array,cmap='inferno',vmin=MIN_VAL,vmax=MAX_VAL)
    ax.set_xticks(np.arange(len(date_list)))
    ax.set_xticklabels(date_list)
    plt.xlabel('Date')
    location, project, datapull = config.location, config.project, config.datapull
    plt.title(f'{project},{location}.{datapull}\nNumber of MKV Files for Each Date ({MAX_VAL} maximum)')
    plt.setp(ax.get_xticklabels(), rotation=-90, ha="left",
            rotation_mode="anchor",fontsize='xx-small')

    ax.set_yticks(np.arange(len(camera_ids)))
    ax.set_yticklabels(camera_ids)
    ax.set_ylim((len(camera_ids)-0.5, -0.5))
    # ax.set_position([0.02,0.2,0.95,0.8])
    plt.ylabel('Turbine')
    ##Rotate the tick labels and set their alignment.
    plt.setp(ax.get_yticklabels(),ha="right",fontsize='xx-small')

    fig.colorbar(im, orientation="vertical", pad = 0.05,label='# Files For Each Date')
    plt.tight_layout()
    plt.show()


def create_video_timing_card(config):
    zero_size_files = []
    xml_count = 0
    video_count = 0

    all_files = config.data_access.list_video_paths()
    camera_id_list = config.data_access.list_cam_ids()

    # Extract datetimes from filenames
    dts = np.array([pd.to_datetime(' '.join(PurePath(path).parts[-1].split('_')[0:2])) for path in all_files])
    start_date = dts.min().date()
    end_date = dts.max().date()
    date_list = []

    # Generate the list of dates using pandas date_range
    date_list = list(pd.date_range(start=start_date, end=end_date).date)

    # Initialize arrays
    count_detect_files_array = np.zeros((len(camera_id_list), len(date_list)))
    count_xmls = np.zeros((len(camera_id_list), len(date_list)))
    with tqdm(total=len(camera_id_list) * len(date_list), desc = "Camera Nights", position=0) as pbar:
        for ind_camera_id, camera_id in enumerate(camera_id_list):
            camera_video_files = config.data_access.list_video_paths(camera_id)
            camera_xml_files = config.data_access.list_xml_paths(camera_id)
            for ind_date, date in enumerate(date_list):
                date_str = datetime.strftime(date, "%Y%m%d")

                file_list = [path for path in camera_video_files if date_str in path]
                count_detect_files_array[ind_camera_id,ind_date] = len(file_list)
                xml_list = [path for path in camera_xml_files if date_str in path]
                count_xmls[ind_camera_id,ind_date] = len(xml_list)

                # Check files for size
                for file in file_list:
                    file_size = config.data_access.get_file_size(file)
                    if file_size == 0:
                        zero_size_files.append(file)

                xml_count += len(xml_list)
                video_count += len(file_list)
                if len(xml_list) != len(file_list):
                    print(f'camera_id: {camera_id_list[ind_camera_id]}')
                    print(f'date: {date_list[ind_date]}')
                pbar.update(1)

    return count_detect_files_array, count_xmls, zero_size_files, video_count, xml_count, date_list