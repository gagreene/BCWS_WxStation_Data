# -*- coding: utf-8 -*-
"""
Created on Thur Apr 18 17:00:00 2024

@author: Gregory A. Greene
"""
__author__ = ['Gregory A. Greene, map.n.trowel@gmail.com']

import os
import arcpy
from arcpy import env
import pandas as pd
import requests


# Set base url
base_url = 'https://bcwsapi.nrs.gov.bc.ca/wfwx-datamart-api/v1'


# Set ArcGIS Environment Parameters
env.parallelProcessingFactor = '75%'
env.overwriteOutput = True
env.workspace = 'in_memory'
env.

def getWX(out_path: str,
          data_type: str,
          start_date: int,
          end_date: int,
          query_method: str,
          query_name: str = None,
          shp_path: str = None,
          search_radius: float = None) -> None:
    """
    Function to get BCWS weather station data through the weather station API
    :param out_path: path to save BCWS weather station data (will be stored in 'BCWS_WxStn_Downloads' folder)
    :param data_type: Type of data to download ('dailies' or 'hourlies')
    :param start_date: Start date of weather stream (formatted as yyyymmdd);
        Days start at 12am (dd = 00) and end at 11pm (dd = 23). For Daily data, use 00 as the start time.
    :param end_date: End date of weather stream (formatted as yyyymmdd);
        Days start at 12am (dd = 00) and end at 11pm (dd = 23). For Daily data, use 23 as the end time.
    :param query_method: Method to query weather data (by 'station', 'community', or 'shapefile')
    :param query_name: BC Wx Station name (STTN_NM; e.g., 'KNIFE'), or BC Community name (Name; e.g., '150 Mile House')
    :param shp_path: Path to shapefile - only used if query_method set to 'shapefile'
    :param search_radius: Distance (km) to search for stations around a point.
        Used when query_method == 'community',
        or when query_method == 'shapefile' and the shapefile is a Point geometry type.
    :return: None
    """
    # Verify input parameters
    if query_method not in ['station', 'community', 'shapefile']:
        raise ValueError(f'Invalid "query_method": {query_method}')
    elif (query_method == 'community') & (search_radius is None):
        raise ValueError('Parameter "search_radius" is required when "query_method" is "community"')

    # Set station output path
    stn_out = os.path.join(out_path, 'BCWS_WxStn_Downloads')

    # Add folder where downloads will go
    if not os.path.exists(stn_out):
        os.makedirs(stn_out)

    # Format switches a bit
    start_date = str(start_date)
    end_date = str(end_date)
    if data_type == 'dailies':
        start_date = start_date[:-2] + '00'
        end_date = end_date[:-2] + '23'

    # Generate URLs by query method
    if query_method == 'station':
        # Construct URL for data request
        station = query_name.replace(' ', '%20')
        url = f'{base_url}/{data_type}?stationName={station}&from={start_date}&to={end_date}'

    elif query_method == 'community':
        # Construct URL for data request
        com_string = f'point={query_name}&distance={search_radius}'
        url = f'{base_url}/{data_type}?{com_string}&from={start_date}&to={end_date}'

    elif query_method == 'shapefile':
        # ### PROCESS SHAPEFILE DATA
        # Check if shapefile exists
        if not os.path.exists(shp_path):
            raise ValueError(f'Shapefile does not exist at {shp_path}')

        # Get the name of the shapefile
        shp_name = shp_path.split('\\')[-1].split('.')[0]

        # Open shapefile with Fiona
        shp_desc = arcpy.Describe(shp_path)
        shp_proj = shp_desc.spatialReference.projectionCode

        # Verify projection is WGS84 (EPSG:4326)
        temp_path = None
        if in_shp.crs != 4326:
            # Add temp folder to store reprojected shapefile
            temp_path = os.path.join(out_path, 'temp')
            if not os.path.exists(temp_path):
                os.makedirs(temp_path)
            # Reproject shapefile to WGS84
            shp_path = os.path.join(temp_path, shp_name + '_EPSG4326.shp')
            in_shp = projectShapefile(in_shp, new_crs=4326, out_path=shp_path)

        # Get shapefile geometry type
        shp_type = in_shp.schema['geometry']

        # Ensure geometry type is point or polygon
        if shp_type not in ['Point', 'Polygon']:
            raise TypeError(f'Shapefile is not a point or polygon geometry type: {shp_type}')
        elif shp_type == 'Point':
            # Get list of coordinates from point shapefile
            coord_list = []
            for feat in in_shp:
                coord_list.append(feat['geometry']['coordinates'])
            if len(coord_list) > 1:
                coord_list = coord_list[0]

            # Construct URL for data request
            point_string = f'point={coord_list[0]},{coord_list[1]}'
            url = f'{base_url}/{data_type}?{point_string}&distance={search_radius}&from={start_date}&to={end_date}'
        else:
            # Get the bounding box of all polygons in the shapefile
            bbox = []
            def explode(coords):
                for e in coords:
                    if isinstance(e, (float, int)):
                        yield coords
                        break
                    else:
                        for f in explode(e):
                            yield f
            for feat in in_shp:
                x, y = zip(*list(explode(feat['geometry']['coordinates'])))
                bbox.append(min(x), min(y), max(x), max(y))

            # ### CONSTRUCT URL FOR DATA REQUEST
            poly_string = f'boundingBox={bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}'
            url = f'{base_url}/{data_type}?{poly_string}&from={start_date}&to={end_date}'

    # Grab API data
    headers = {
        'Cookie': 'ROUTEID=.3',
        'Connection': 'keep-alive',
        'Content-Type': 'applications/json'
    }

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

    # Cycle through each page, request page data, convert response to dataframe, and store in responses list
    responses = []
    for i in range(1, page_count + 1):
        page_url = f'{url}&pageNumber={i}&pageRowCount=100'
        responses.append(pd.DataFrame(requests.get(page_url, headers=headers).json()['collection']))

    # Generate data_df from list of responses
    data_df = pd.concat(responses)

    # Remove unnecessary data from data_df
    data_df = data_df.iloc[:, 2:]
    data_df = data_df.drop(columns='geometry')

    # Save data to stn_out folder
    if query_method == 'station':
        query_name = query_name.replace(' ', '_')
        data_df.to_csv(
            f'{stn_out}//{data_type}_{query_name}_{start_date}_to_{end_date}.csv',
            index=False)
    elif query_method == 'community':
        query_name = query_name.replace(' ', '_')
        data_df.to_csv(
            f'{stn_out}//{data_type}_{query_name}_{search_radius}km_{start_date}_to_{end_date}.csv',
            index=False)
    else:   # query_method == 'shapefile'
        if shp_type == 'Point':
            data_df.to_csv(
                f'{stn_out}//{data_type}_{shp_name}_{search_radius}km_{start_date}_to_{end_date}.csv',
                index=False)
        else:   # shp_type == 'polygon'
            data_df.to_csv(
                f'{stn_out}//{data_type}_{shp_name}_{start_date}_to_{end_date}.csv',
                index=False)
        # Delete temporary
        if temp_path is not None:
            if os.path.exists(shp_path):
                os.remove(shp_path)
            if os.path.exists(temp_path):
                os.remove(temp_path)

    print('Data saved!')


if __name__ == '__main__':
    out_path_ = r'C:\Temp\BCWS_weatherTesting'
    data_type_ = 'dailies'
    start_date_ = 2022070100
    end_date_ = 2022073123
    query_method_ = 'station'
    query_name_ = 'KNIFE'
    search_radius_ = 15

    getWX(
        out_path=out_path_,
        data_type=data_type_,
        start_date=start_date_,
        end_date=end_date_,
        query_method=query_method_,
        query_name=query_name_,
        search_radius=search_radius_
    )
