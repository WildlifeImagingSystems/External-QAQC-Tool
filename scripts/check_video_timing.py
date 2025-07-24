from tqdm import tqdm
import numpy as np
from pathlib import PurePath
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import plotly.express as px


def create_heatmap(count_detect_files_array, date_list, config, camera_ids):
    # Create a heatmap of the number of video files detected per camera and date
    heatmap_df = pd.DataFrame(
        count_detect_files_array,
        index=camera_ids,
        columns=[str(d.strftime('%Y - %m - %d')) for d in date_list]  # Convert dates to str for Plotly 
    )

    # drop last camera night if there are now videos
    if all(heatmap_df[heatmap_df.columns[-1]] == 0):
        heatmap_df.drop(columns=[heatmap_df.columns[-1]], inplace=True)


    MAX_VAL = count_detect_files_array.max()
    location, project, datapull = config.location, config.project, config.datapull

    heatmap = px.imshow(
        heatmap_df,
        labels=dict(x="Date", y="Camera ID", color="File Count"),
        aspect="auto",
        color_continuous_scale="Purples",
        zmin=0,
        zmax=count_detect_files_array.max()
    )
    heatmap.update_xaxes(
        tickfont=dict(size=12),
        side="bottom",
        tickangle=90
    )
    heatmap.update_yaxes(
        tickfont=dict(size=12)
    )

    # Add a title to the plot
    heatmap.update_layout(
        title_text=f"{project} - {location} - {datapull}<br><sup>Video Count per Day ({MAX_VAL} maximum)</sup>",
        title_font_size=20,
        title_subtitle_font_size=14,
        title_subtitle_font_color="gray"
    )
    heatmap.show()

def get_files_in_camera_night(paths, date):
    in_camera_night = []

    for path in paths:
    # parse files date
        date_str = ' '.join(PurePath(path).parts[-1].split('_')[:-1])
        try:
            path_date = datetime.strptime(date_str, "%Y%m%d %H%M%S")
            if path_date.date() == date:
                if path_date.hour > 12:
                    in_camera_night.append(path)
            elif path_date.date() == date + timedelta(days=1):
                if path_date.hour <= 12:
                    in_camera_night.append(path)
        except ValueError as e:
            pass

    return in_camera_night

def create_video_timing_card(config):
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
                file_list = get_files_in_camera_night(camera_video_files, date)
                count_detect_files_array[ind_camera_id,ind_date] = len(file_list)


                xml_list = get_files_in_camera_night(camera_xml_files, date)
                xml_list = [path for path in xml_list if 'recording' not in path]
                count_xmls[ind_camera_id,ind_date] = len(xml_list)

                xml_count = len(xml_list)
                video_count = len(file_list)
                if len(xml_list) != len(file_list):
                    print(f'date: {date_list[ind_date]}, camera_id: {camera_id_list[ind_camera_id]}: mismatched count of xmls and videos.')

                if xml_count == 0 and video_count != 0: 
                    print(f'date: {date_list[ind_date]}, camera_id: {camera_id_list[ind_camera_id]}: zero xmls found with videos, usually caused by an incomplete upload.')
                
                if xml_count != 0 and video_count == 0: 
                    print(f'date: {date_list[ind_date]}, camera_id: {camera_id_list[ind_camera_id]}: no videos found, usually cause by an incomplete upload.')
                pbar.update(1)

    return count_detect_files_array ,date_list