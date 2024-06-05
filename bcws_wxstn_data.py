# -*- coding: utf-8 -*-
"""
Created on Thur Apr 18 17:00:00 2024

@author: Gregory A. Greene
"""
__author__ = ['Gregory A. Greene, map.n.trowel@gmail.com']

import os
import calendar
import sys
from typing import Union, Optional
import datetime as dt
from datetime import timedelta
from dateutil.rrule import *
import ast
import pandas as pd
import requests
import fiona as fio
from fiona.crs import CRS
from shapely.geometry import mapping, Point
from pyproj import Transformer


# Get paths to weather station and community point shapefiles
community_shp = os.path.join(os.path.dirname(__file__),
                             'Supplementary_Data',
                             'bc_communities_epsg4326.shp')

# Set base url
base_url = 'https://bcwsapi.nrs.gov.bc.ca/wfwx-datamart-api/v1'


def _getShapefile(in_path: str):
    """
    Function returns a fiona collection object representing the shapefile
    :param in_path: path to shapefile
    :return: fiona collection object in read mode
    """
    return fio.open(in_path, 'r')


def _projectShapefile(src: fio.Collection,
                      new_crs: int,
                      out_path: str):
    """
    Function returns a fiona collection object representing the shapefile
    :param src: fiona collection object
    :param new_crs: EPSG code for new projection
    :param out_path: output path to new shapefile
    :return: fiona collection object in read mode
    """
    src_crs = src.crs
    out_crs = CRS.from_epsg(new_crs)
    new_feats = []

    # Transform coordinates with new projection
    transformer = Transformer.from_crs(src_crs, out_crs)
    for feat in src:
        x, y = feat['geometry']['coordinates']
        x_, y_ = transformer.transform(x, y)
        new_feats.append({'geometry': mapping(Point(x_, y_)),
                          'properties': feat.properties})

    # Create new shapefile
    schema = src.schema
    with fio.open(out_path, mode='w', driver='ESRI Shapefile', schema=schema, crs=out_crs) as dst:
        for feat in new_feats:
            dst.write(feat)
    return fio.open(out_path, 'r')


def _getFilteredMonthDays(start_date_str: str,
                          end_date_str: str,
                          filter_month_days: bool) -> list[tuple]:
    """
    Function to generate the start and end dates for each month within a start and end date range
    :param start_date_str: Start date of weather stream (formatted as yyyymmddhh);
        Days start at 12am (hh = 00) and end at 11pm (hh = 23). For Daily data, use 00 as the start time.
    :param end_date_str: End date of weather stream (formatted as yyyymmddhh);
        Days start at 12am (hh = 00) and end at 11pm (hh = 23). For Daily data, use 23 as the end time.
    :param filter_month_days: Filter dates to be within the months and days provided for each year.
        If False, data from all dates within the start and end dates will be requested.
    :return: List containing tuple pairs of the start/end dates for each month
    """
    # Parse the input date strings
    start_date = dt.datetime.strptime(start_date_str, '%Y%m%d%H')
    end_date = dt.datetime.strptime(end_date_str, '%Y%m%d%H')
    mmdd_start = dt.datetime.strptime(start_date_str[4:8], '%m%d').date().replace(year=2020)
    mmdd_end = dt.datetime.strptime(end_date_str[4:8], '%m%d').date().replace(year=2020)

    # Create a list to store the first and last days of each filtered month
    month_boundaries = []

    # Set the current date to the start of the month for the start_date
    current_date = start_date.replace(day=1)

    while current_date <= end_date:
        # Get the first day of the current month
        first_day = current_date

        # Get the last day of the current month
        last_day = current_date.replace(day=calendar.monthrange(current_date.year, current_date.month)[1])

        # Ensure the last day does not exceed the end_date
        if last_day > end_date:
            last_day = end_date

        if filter_month_days:
            # Filter out months that do not fall within the specified MMDD range
            first_mmdd = first_day.date().replace(year=2020)
            last_mmdd = last_day.date().replace(year=2020)

            if mmdd_start <= first_mmdd <= mmdd_end or mmdd_start <= last_mmdd <= mmdd_end:
                # Adjust the first day to be within the range if it starts before mmdd_start
                if first_mmdd < mmdd_start:
                    first_day = first_day.replace(month=mmdd_start.month, day=mmdd_start.day)

                # Adjust the last day to be within the range if it ends after mmdd_end
                if last_mmdd > mmdd_end:
                    last_day = last_day.replace(month=mmdd_end.month, day=mmdd_end.day)

                # Convert dates to YYYYMMDDHH format
                first_day_str = first_day.replace(hour=0).strftime('%Y%m%d%H')
                last_day_str = last_day.replace(hour=23).strftime('%Y%m%d%H')

                # Add first and last day of the month to the month_boundaries list
                month_boundaries.append((first_day_str, last_day_str))
        else:
            # Assign first and last day values as strings
            first_day_str = first_day.strftime('%Y%m%d%H')
            last_day_str = last_day.strftime('%Y%m%d%H')

            # Add first and last day of the month to the month_boundaries list
            month_boundaries.append((first_day_str, last_day_str))

        # Move to the first day of the next month
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1, day=1)

    return month_boundaries


def _getFilteredHours(hourly_df: pd.DataFrame,
                      date_column: str,
                      start_date_str: str,
                      end_date_str: str) -> pd.DataFrame:
    """
    Function to remove rows from a Pandas dataframe where hours are outside a provided range
    :param hourly_df: Pandas dataframe containing hourly data
    :param date_column: Name of the hourly data column
    :param start_date_str: Start date of weather stream (formatted as yyyymmddhh);
        Days start at 12am (hh = 00) and end at 11pm (hh = 23).
    :param end_date_str: End date of weather stream (formatted as yyyymmddhh);
        Days start at 12am (hh = 00) and end at 11pm (hh = 23).
    :return: A filtered Pandas dataframe, with hours outside the start and end date hours removed
    """
    # Convert the date strings to datetime objects
    hourly_df['NewDates'] = pd.to_datetime(hourly_df[date_column], format='%Y%m%d%H')

    # Extract the hour component from the datetime objects
    hourly_df['Hour'] = hourly_df['NewDates'].dt.hour

    # Convert hh_start and hh_end to integers
    hh_start = int(start_date_str[8:])
    hh_end = int(end_date_str[8:])

    # Filter the rows based on the specified hour range
    filtered_df = hourly_df[(hourly_df['Hour'] >= hh_start) & (hourly_df['Hour'] <= hh_end)]

    # Drop the 'Hour' column as it is no longer needed
    filtered_df = filtered_df.drop(columns=['Hour', 'NewDates'])

    return filtered_df


def getWX(out_path: str,
          data_type: str,
          start_date: Union[int, str],
          end_date: Union[int, str],
          filter_month_days: bool,
          filter_hours: bool,
          query_method: str,
          query_names: Optional[list[str]] = None,
          shp_path: Optional[str] = None,
          search_radius: Optional[float] = None) -> None:
    """
    Function to get BCWS weather station data through the weather station API
    :param out_path: path to save BCWS weather station data (will be stored in 'BCWS_WxStn_Downloads' folder)
    :param data_type: Type of data to download ('dailies' or 'hourlies')
    :param start_date: Start date of weather stream (formatted as yyyymmddhh);
        Days start at 12am (hh = 00) and end at 11pm (hh = 23). For Daily data, use 00 as the start time.
    :param end_date: End date of weather stream (formatted as yyyymmddhh);
        Days start at 12am (hh = 00) and end at 11pm (hh = 23). For Daily data, use 23 as the end time.
    :param filter_month_days: Filter annual dates to be within the months and days provided by the start and end dates.
        If False, data from all dates within the start and end dates will be requested.
    :param filter_hours: Filter hourly data per day to be within the hours provided by the start and end dates.
        If False, all hours within the start and end dates will be requested.
    :param query_method: Method to query weather data (by 'station', 'community', or 'shapefile')
    :param query_names: BC Wx Station name (STTN_NM; e.g., 'KNIFE'), or BC Community name (Name; e.g., '150 Mile House')
    :param shp_path: Path to shapefile - only used if query_method set to 'shapefile'
    :param search_radius: Distance (km) to search for stations around a point.
        Used when query_method == 'community',
        or when query_method == 'shapefile' and the shapefile is a Point geometry type.
    :return: None
    """
    # ### VERIFY INPUT PARAMETERS
    print('Verifying input parameters')
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
    print('Generating weather dates list')
    # Generate list of dates between start and end date
    wx_dates = _getFilteredMonthDays(start_date, end_date, filter_month_days)

    # ### GENERATE URLS BY QUERY METHOD
    print('Generating request URLs')
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
        # Open community shapefile with Fiona
        in_shp = _getShapefile(community_shp)

        # Get list of coordinates from community shapefile
        coord_list = [feat['geometry']['coordinates'] for feat in in_shp
                      if feat['properties']['Name'] in query_names]

        # Construct URL for community data request
        url_list = []
        for coord in coord_list:
            com_string = f'point={coord[0]},{coord[1]}&distance={search_radius}'
            for date in wx_dates:
                url_list.extend(
                    [f'{base_url}/{data_type}?{com_string}&from={date[0]}&to={date[1]}']
                )

    else:  # query_method == 'shapefile':
        # Check if shapefile exists
        if not os.path.exists(shp_path):
            raise ValueError(f'Shapefile does not exist at {shp_path}')

        # Get the name of the shapefile
        shp_name = shp_path.split('\\')[-1].split('.')[0]

        # Open shapefile with Fiona
        in_shp = _getShapefile(shp_path)

        # Verify projection is WGS84 (EPSG:4326)
        temp_path = None
        if in_shp.crs != 4326:
            # Add temp folder to store reprojected shapefile
            temp_path = os.path.join(out_path, 'temp')
            if not os.path.exists(temp_path):
                os.makedirs(temp_path)
            # Reproject shapefile to WGS84
            shp_path = os.path.join(temp_path, shp_name + '_EPSG4326.shp')
            in_shp = _projectShapefile(in_shp, new_crs=4326, out_path=shp_path)

        # Get shapefile geometry type
        shp_type = in_shp.schema['geometry']

        # Ensure geometry type is point or polygon
        if shp_type not in ['Point', 'Polygon']:
            raise TypeError(f'Shapefile is not a point or polygon geometry type: {shp_type}')
        elif shp_type == 'Point':
            # Get list of coordinates from point shapefile
            coord_list = [feat['geometry']['coordinates'] for feat in in_shp]

            # Construct URLs for point shapefile data request
            url_list = []
            for coord in coord_list:
                point_string = f'point={coord[0]},{coord[1]}'
                for date in wx_dates:
                    url_list.extend(
                        [f'{base_url}/{data_type}?{point_string}&distance={search_radius}&from={date[0]}&to={date[1]}']
                    )
        else:
            # Get the bounding box of all polygons in the shapefile
            bbox_list = []

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
                bbox_list.append(min(x), min(y), max(x), max(y))

                # Construct URLs for polygon shapefile data request
                url_list = []
                for extent in bbox_list:
                    poly_string = f'boundingBox={extent[0]},{extent[1]},{extent[2]},{extent[3]}'
                    for date in wx_dates:
                        url_list.extend(
                            [f'{base_url}/{data_type}?{poly_string}&from={date[0]}&to={date[1]}']
                        )

    # ### GET REQUESTED DATA
    print('Submitting data request...')
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

    if len(responses) > 0:
        # Generate data_df from list of responses
        print('Processing data...')
        data_df = pd.concat(responses)

        # Remove unnecessary data from data_df
        data_df = data_df.iloc[:, 2:]
        data_df = data_df.drop(columns='geometry')
        data_df = data_df.drop_duplicates()

        # Sort data_df by stationName and weatherTimestamp
        data_df.sort_values(by=['stationName', 'weatherTimestamp'],
                            ascending=[True, True],
                            inplace=True)

        # Remove unrequested hours
        if filter_hours:
            data_df = _getFilteredHours(hourly_df=data_df,
                                        date_column='weatherTimestamp',
                                        start_date_str=start_date,
                                        end_date_str=end_date)

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
        else:  # query_method == 'shapefile'
            if shp_type == 'Point':
                data_df.to_csv(
                    f'{out_path}//{data_type}_{shp_name}_{search_radius}kmBuffer_{start_date}_to_{end_date}.csv',
                    index=False)
            else:  # shp_type == 'polygon'
                data_df.to_csv(
                    f'{out_path}//{data_type}_{shp_name}_{start_date}_to_{end_date}.csv',
                    index=False)
            # Delete temporary
            if temp_path is not None:
                if os.path.exists(shp_path):
                    os.remove(shp_path)
                if os.path.exists(temp_path):
                    os.remove(temp_path)

        print('Data saved!')
    else:
        print('No data was found for the dates provided.')


if __name__ == '__main__':
    # if len(sys.argv[1:]) != 9:
    #     print('Nine parameters are required: [out_path_, data_type_, start_date_, end_date_, '
    #           'filter_month_days_, filter_hours_, query_method_, query_names_, shp_path_, search_radius_]')
    #     sys.exit(1)
    #
    # (out_path_, data_type_,
    #  start_date_, end_date_,
    #  filter_month_days_, filter_hours_,
    #  query_method_, query_names_,
    #  shp_path_, search_radius_) = sys.argv[1:]

    # FOR TESTING
    (out_path_, data_type_,
     start_date_, end_date_,
     filter_month_days_, filter_hours_,
     query_method_, query_names_,
     shp_path_, search_radius_) = (
        os.path.join(r'F:\Temp\BCWS_WxStation_DataDownloader_Testing'),
        'hourlies',
        '2014051508',
        '2017083112',
        True,
        True,
        'station',
        ['KNIFE'],
        None,
        None
    )

    getWX(
        out_path=out_path_,
        data_type=data_type_,
        start_date=start_date_,
        end_date=end_date_,
        filter_month_days=filter_month_days_,
        filter_hours=filter_hours_,
        query_method=query_method_,
        query_names=query_names_,
        search_radius=search_radius_
    )
