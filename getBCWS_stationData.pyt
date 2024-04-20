# -*- coding: utf-8 -*-
"""
Created on Thur Apr 18 17:00:00 2024

@author: Gregory A. Greene
"""
__author__ = ['Gregory A. Greene, map.n.trowel@gmail.com']

import os
import arcpy
import downloadBCWS_stationData_arcgis as bcwx


community_shp = os.path.join(os.path.dirname(__file__),
                             'Supplementary_Data',
                             'bc_communities_epsg4326.shp')
station_shp = os.path.join(os.path.dirname(__file__),
                           'Supplementary_Data',
                           'bc_weather_stations_epsg4326.shp')


class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the .pyt file). """
        self.label = 'BCWS Weather Station Download Toolbox'
        self.alias = 'BCWS Weather Station Download Toolbox'

        # List of tool classes associated with this toolbox
        self.tools = [DownloadBCWS_wxStnData]


class DownloadBCWS_wxStnData(object):
    """
    METHOD:
        __init__():
        getParameterInfo():
        updateParameters():
        updateMessages():
        execute():
    """

    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = 'Download BCWS weather station data'
        self.description = """ArcGIS Pro Toolbox to download BCWS weather station data"""
        self.comm_names = sorted(list(set(list({name[0] for name in arcpy.da.SearchCursor(community_shp, 'Name')}))))
        self.stn_names = sorted(list(set(list({name[0] for name in arcpy.da.SearchCursor(station_shp, 'STTN_NM')}))))

    def isLicensed(self):
        """Allow the tool to execute."""

        return True

    def getParameterInfo(self):
        """Define parameter definitions."""
        #### Local Imports ####

        #### Define Parameters ####
        param0 = arcpy.Parameter(displayName='Output Folder',
                                 name='out_folder',
                                 datatype='DEFolder',
                                 parameterType='Required',
                                 direction='Input')

        param1 = arcpy.Parameter(displayName='Wx Data Type',
                                 name='data_type',
                                 datatype='GPString',
                                 parameterType='Required',
                                 direction='Input')
        param1.filter.type = 'ValueList'
        param1.filter.list = ['Daily', 'Hourly']
        param1.value = 'Daily'

        param2 = arcpy.Parameter(displayName='Start Day',
                                 name='start_date',
                                 datatype='GPDate',
                                 parameterType='Required',
                                 direction='Input')
        param2.value = '4/1/2023'

        param3 = arcpy.Parameter(displayName='Start Hour',
                                 name='start_hour',
                                 datatype='GPString',
                                 parameterType='Required',
                                 direction='Input')
        param3.filter.type = 'ValueList'
        param3.filter.list = [str(i).zfill(2) for i in range(0, 24)]
        param3.value = '00'

        param4 = arcpy.Parameter(displayName='End Day',
                                 name='end_date',
                                 datatype='GPDate',
                                 parameterType='Required',
                                 direction='Input')
        param4.value = '4/30/2023'

        param5 = arcpy.Parameter(displayName='End Hour',
                                 name='end_hour',
                                 datatype='GPString',
                                 parameterType='Required',
                                 direction='Input')
        param5.filter.type = 'ValueList'
        param5.filter.list = [str(i).zfill(2) for i in range(0, 24)]
        param5.value = '23'

        param6 = arcpy.Parameter(displayName='Query Method',
                                 name='query_method',
                                 datatype='GPString',
                                 parameterType='Required',
                                 direction='Input',
                                 multiValue=False)
        param6.filter.type = 'ValueList'
        param6.filter.list = ['station', 'community', 'shapefile']
        param6.value = 'station'

        param7 = arcpy.Parameter(displayName='Query Name',
                                 name='query_name',
                                 datatype='GPString',
                                 parameterType='Optional',
                                 direction='Input',
                                 multiValue=True)
        param7.filter.type = 'ValueList'
        param7.filter.list = []
        param7.value = ''

        param8 = arcpy.Parameter(displayName='Input Shapefile',
                                 name='in_shp_path',
                                 datatype=['DEShapefile', 'DEFeatureClass', 'GPFeatureLayer'],
                                 parameterType='Optional',
                                 direction='Input')

        param9 = arcpy.Parameter(displayName='Search Radius',
                                 name='search_radius',
                                 datatype='GPDouble',
                                 parameterType='Optional',
                                 direction='Input')
        param9.value = 0

        params = [param0, param1, param2, param3, param4,
                  param5, param6, param7, param8, param9]

        return params

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed. """

        # Disable parameters 6 and 7 initially
        parameters[3].enabled = False
        parameters[5].enabled = False
        parameters[8].enabled = False
        parameters[9].enabled = False

        if parameters[2].value:
            if parameters[1].valueAsText == 'Daily':
                parameters[3].value = '00'
                parameters[3].enabled = False
            else:
                parameters[3].enabled = True
        else:
            parameters[3].enabled = False

        if parameters[4].value:
            if parameters[1].valueAsText == 'Daily':
                parameters[5].value = '23'
                parameters[5].enabled = False
            else:
                parameters[5].enabled = True
        else:
            parameters[5].enabled = False

        if parameters[6].value:
            if not parameters[6].hasBeenValidated:
                parameters[7].value = ''
                parameters[7].filter.list = []
            if parameters[6].valueAsText == 'community':
                parameters[7].filter.list = self.comm_names
                parameters[7].enabled = True
                parameters[8].enabled = False
                parameters[9].enabled = True
            if parameters[6].valueAsText == 'station':
                parameters[7].filter.list = self.stn_names
                parameters[7].enabled = True
                parameters[8].enabled = False
            if parameters[6].valueAsText == 'shapefile':
                parameters[7].enabled = False
                parameters[8].enabled = True

        if parameters[8].enabled:
            if parameters[8].value:
                shp_type = arcpy.Describe(parameters[8].valueAsText).shapeType
                if shp_type == 'Point':
                    parameters[9].enabled = True
                else:
                    parameters[9].enabled = False

        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation. """

        if parameters[6].valueAsText == 'shapefile':
            if not parameters[8].value:
                parameters[8].setErrorMessage('Must select a shapefile')

        if parameters[8].value:
            shp_type = arcpy.Describe(parameters[8].valueAsText).shapeType
            if shp_type not in ['Point', 'Polygon']:
                parameters[9].setErrorMessage('Feature dataset must be either Point or Polygon geometry type')

        return

    def execute(self, parameters, messages):
        # Get input parameters from the Toolbox interface
        out_folder = parameters[0].valueAsText
        data_type = parameters[1].valueAsText
        start_day = parameters[2].valueAsText
        start_hour = parameters[3].valueAsText
        end_day = parameters[4].valueAsText
        end_hour = parameters[5].valueAsText
        query_method = parameters[6].valueAsText
        query_name = parameters[7].values
        shp_in = parameters[8].valueAsText
        search_radius = parameters[9].value

        # Generate start_date variable
        start_day_list = start_day.split('/')
        start_date = f'{start_day_list[2]}{start_day_list[1]}{start_day_list[0]}{start_hour}'
        # Generate end_date variable
        end_day_list = end_day.split('/')
        end_date = f'{end_day_list[2]}{end_day_list[1]}{end_day_list[0]}{end_hour}'

        if query_method == 'shapefile':
            shp_out = os.path.join(out_folder,
                                   arcpy.Describe(shp_in).name.split('.') + '.shp')
            arcpy.CopyFeatures_management(shp_in, shp_out)

        arcpy.AddMessage((out_folder,
                          data_type,
                          start_day, start_hour,
                          end_day, end_hour,
                          query_method, query_name,
                          shp_in,
                          search_radius,
                          arcpy.Describe(shp_in).dataType,
                          start_day_list, end_day_list))

        bcwx.getWX(out_path=out_folder,
                   data_type=data_type,
                   start_date=start_date,
                   end_date=end_date,
                   query_method=query_method,
                   query_name=query_name,
                   shp_path=shp_out,
                   search_radius=search_radius)

        return

