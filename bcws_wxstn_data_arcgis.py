# -*- coding: utf-8 -*-
"""
Created on Thur Apr 18 17:00:00 2024

@author: Gregory A. Greene
"""
__author__ = ['Gregory A. Greene, map.n.trowel@gmail.com']

import os
import calendar
import datetime as dt
import sys

from dateutil.rrule import *
from typing import Union
import pandas as pd
import requests
import ast
import arcpy
from arcpy import env


# Set ArcGIS Environment Parameters
env.overwriteOutput = True
env.workspace = 'in_memory'
env.outputCoordinateSystem = arcpy.SpatialReference(4326)

# Get paths to weather station and community point shapefiles
community_shp = os.path.join(os.path.dirname(__file__),
                             'Supplementary_Data',
                             'bc_communities_epsg4326.shp')

# Set base url
base_url = 'https://bcwsapi.nrs.gov.bc.ca/wfwx-datamart-api/v1'


def getWX(out_path: str,
          data_type: str,
          start_date: Union[int, str],
          end_date: Union[int, str],
          query_method: str,
          query_names: Union[list[str], None] = None,
          shp_path: Union[str, None] = None,
          search_radius: Union[float, None] = None) -> None:
    """
    Function to get BCWS weather station data through the weather station API
    :param out_path: path to save BCWS weather station data (will be stored in 'BCWS_WxStn_Downloads' folder)
    :param data_type: Type of data to download ('dailies' or 'hourlies')
    :param start_date: Start date of weather stream (formatted as yyyymmddhh);
        Days start at 12am (hh = 00) and end at 11pm (hh = 23). For Daily data, use 00 as the start time.
    :param end_date: End date of weather stream (formatted as yyyymmddhh);
        Days start at 12am (hh = 00) and end at 11pm (hh = 23). For Daily data, use 23 as the end time.
    :param query_method: Method to query weather data (by 'station', 'community', or 'shapefile')
    :param query_names: BC Wx Station name (STTN_NM; e.g., 'KNIFE'), or BC Community name (Name; e.g., '150 Mile House')
    :param shp_path: Path to shapefile - only used if query_method set to 'shapefile'
    :param search_radius: Distance (km) to search for stations around a point.
        Used when query_method == 'community',
        or when query_method == 'shapefile' and the shapefile is a Point geometry type.
    :return: None
    """
    # ### VERIFY INPUT PARAMETERS
    arcpy.AddMessage('Verifying input parameters')
    # query_method
    if query_method not in ['station', 'community', 'shapefile']:
        raise ValueError(f'Invalid "query_method": {query_method}')
    elif (query_method == 'community') & (search_radius is None):
        raise ValueError('Parameter "search_radius" is required when "query_method" is "community"')
    # data_type
    if data_type not in ['dailies', 'hourlies']:
        raise ValueError(f'Invalid "data_type": {data_type}')

    # ### CREATE FOLDERS AND MODIFY INPUT PARAMETERS
    # Add folder where downloads will go
    if not os.path.exists(out_path):
        os.makedirs(out_path)

    # Force formatting of start and end dates for daily weather data
    start_date = str(start_date)
    end_date = str(end_date)
    if data_type == 'dailies':
        start_date = start_date[:-2] + '00'
        end_date = end_date[:-2] + '23'

    # ### GENERATE WEATHER DATE LIST
    arcpy.AddMessage('Generating weather dates list')
    # Get year, month, and day from start and end dates
    start_year = int(start_date[0:4])
    start_month = int(start_date[4:6])
    start_day = int(start_date[6:8])
    end_year = int(end_date[0:4])
    end_month = int(end_date[4:6])
    end_day = int(end_date[6:8])

    # Create datetime objects from start and end dates
    date_start = dt.date(start_year, start_month, start_day)
    date_end = dt.date(end_year, end_month, end_day)

    # Generate list of months between start and end dates
    month_list = [month.isoformat() for month in rrule(MONTHLY, dtstart=date_start, until=date_end)]

    # Create start date list contianing dates for the first day of each month
    start_date_list = [date[:10].replace('-', '') + '00' for date in month_list]
    # Replace first start date with the user provided start_date
    start_date_list[0] = start_date

    # Create end date list contianing dates for the last day of each month
    end_date_list = [date[:10].replace('-', '') + '23' for date in month_list]
    end_date_list = [date[0:6] +
                     date[6:8].replace('01',
                                       str(calendar.monthrange(int(date[0:4]), int(date[4:6]))[1]).zfill(2)) +
                     date[8:]
                     for date in end_date_list]
    # Replace last end date with the use provided end_date
    end_date_list[-1] = end_date

    # Zip the start and end dates together into a weather date list
    wx_dates = list(zip(start_date_list, end_date_list))

    # ### GENERATE URLS BY QUERY METHOD
    temp_path = ''
    arcpy.AddMessage('Generating request URLs')
    if query_method == 'station':
        # Construct URL for station data request
        url_list = []
        for name in query_names:
            # Construct URL for data request
            station = name.replace(' ', '%20')
            for date in wx_dates:
                url_list.extend(
                    [f'{base_url}/{data_type}?stationName={station}&from={date[0]}&to={date[1]}']
                )

    elif query_method == 'community':
        # Get list of coordinates from point shapefile
        coord_list = [row[0] for row in arcpy.da.SearchCursor(community_shp, ['SHAPE@XY', 'Name'])
                      if row[1] in query_names]

        # Construct URL for community data request
        url_list = []
        for coord in coord_list:
            com_string = f'point={coord[0]},{coord[1]}&distance={search_radius}'
            for date in wx_dates:
                url_list.extend(
                    [f'{base_url}/{data_type}?{com_string}&from={date[0]}&to={date[1]}']
                )

    else:   # query_method == 'shapefile'
        # Check if shapefile exists
        if not os.path.exists(shp_path):
            raise ValueError(f'Shapefile does not exist at {shp_path}')

        # Get the name of the shapefile
        shp_name = shp_path.split('\\')[-1].split('.')[0]

        # Open shapefile with Fiona
        shp_desc = arcpy.Describe(shp_path)
        shp_proj = shp_desc.spatialReference.factoryCode

        # Verify projection is WGS84 (EPSG:4326)
        if shp_proj != 4326:
            # Add temp folder to store reprojected shapefile
            temp_path = os.path.join(out_path, 'temp')
            if not os.path.exists(temp_path):
                os.makedirs(temp_path)
            # Reproject shapefile to WGS84
            proj_path = os.path.join(temp_path, shp_name + '_EPSG4326.shp')
            arcpy.Project_management(shp_path, new_crs=4326, out_path=proj_path)
            shp_path = proj_path

        # Get shapefile geometry type
        shp_type = arcpy.Describe(shp_path).shapeType

        # Ensure geometry type is point or polygon
        if shp_type not in ['Point', 'Polygon']:
            raise TypeError(f'Shapefile is not a point or polygon geometry type: {shp_type}')
        elif shp_type == 'Point':
            # Get list of coordinates from point shapefile
            coord_list = [row[0] for row in arcpy.da.SearchCursor(shp_path, ['SHAPE@XY'])]

            # Construct URLs for point shapefile data request
            url_list = []
            for coord in coord_list:
                point_string = f'point={coord[0]},{coord[1]}'
                for date in wx_dates:
                    url_list.extend(
                        [f'{base_url}/{data_type}?{point_string}&distance={search_radius}&from={date[0]}&to={date[1]}']
                    )
        else:
            # Get list of bounding box (extent) coordinates from polygons
            bbox_list = [(row[0].extent.XMin, row[0].extent.YMin, row[0].extent.XMax, row[0].extent.YMax)
                         for row in arcpy.da.SearchCursor(shp_path, ['SHAPE@'])]

            # Construct URLs for polygon shapefile data request
            url_list = []
            for bbox in bbox_list:
                poly_string = f'boundingBox={bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}'
                for date in wx_dates:
                    url_list.extend(
                        [f'{base_url}/{data_type}?{poly_string}&from={date[0]}&to={date[1]}']
                    )

    # ### GET REQUESTED DATA
    arcpy.AddMessage('Processing data request...')
    # Create request header
    headers = {
        'Cookie': 'ROUTEID=.3',
        'Connection': 'keep-alive',
        'Content-Type': 'applications/json'
    }
    # Create list to store responses
    responses = []
    for url in url_list:
        # Verify the request is valid
        res = requests.get(url, headers=headers)
        if res.status_code >= 400:
            msg_template = ast.literal_eval(res.text)['messages'][0]['messageTemplate']
            msg_args = ast.literal_eval(res.text)['messages'][0]['messageArguments']
            raise ValueError(
                f"""Search URL is invalid: {url}\n
                ERROR MESSAGE: {msg_template}\n
                The arguments you provided: {msg_args}""")

        # Get page count from initial url request
        page_count = res.json()['totalPageCount']

        if page_count >= 1:
            # Cycle through each page, request page data, convert response to dataframe, store in responses list
            for i in range(1, page_count + 1):
                page_url = f'{url}&pageNumber={i}&pageRowCount=100'
                responses.append(pd.DataFrame(requests.get(page_url, headers=headers).json()['collection']))

    # Generate data_df from list of responses
    data_df = pd.concat(responses)

    # Remove unnecessary data from data_df
    data_df = data_df.iloc[:, 2:]
    data_df = data_df.drop(columns='geometry')
    data_df = data_df.drop_duplicates()

    # Sort data_df by stationName and weatherTimestamp
    data_df.sort_values(by=['stationName', 'weatherTimestamp'],
                        ascending=[True, True],
                        inplace=True)

    # Save data to out_path folder
    if query_method == 'station':
        if len(query_names) > 1:
            out_name = 'MultipleStations'
        else:
            out_name = query_names[0].replace(' ', '_')
        data_df.to_csv(
            f'{out_path}//{data_type}_{out_name}_{start_date}_to_{end_date}.csv',
            index=False)
    elif query_method == 'community':
        if len(query_names) > 1:
            out_name = 'MultipleCommunities'
        else:
            out_name = query_names[0].replace(' ', '_')
        data_df.to_csv(
            f'{out_path}//{data_type}_{out_name}_{search_radius}kmBuffer_{start_date}_to_{end_date}.csv',
            index=False)
    else:   # query_method == 'shapefile'
        if shp_type == 'Point':
            data_df.to_csv(
                f'{out_path}//{data_type}_{shp_name}_{search_radius}kmBuffer_{start_date}_to_{end_date}.csv',
                index=False)
        else:   # shp_type == 'polygon'
            data_df.to_csv(
                f'{out_path}//{data_type}_{shp_name}_{start_date}_to_{end_date}.csv',
                index=False)
        # Delete temporary
        if temp_path:
            if os.path.exists(shp_path):
                os.remove(shp_path)
            if os.path.exists(temp_path):
                os.remove(temp_path)

    arcpy.AddMessage('Data saved!')


if __name__ == '__main__':
    if len(sys.argv[1:]) != 8:
        print('Three parameters are required to test the WindNinja CLI: [num_threads, vegetation, out_path]')
        sys.exit(1)

    (out_path_, data_type_,
     start_date_, end_date_,
     query_method_, query_names_,
     shp_path_, search_radius_) = sys.argv[1:]

    getWX(
        out_path=out_path_,
        data_type=data_type_,
        start_date=start_date_,
        end_date=end_date_,
        query_method=query_method_,
        query_names=query_names_,
        shp_path=shp_path_,
        search_radius=search_radius_
    )
