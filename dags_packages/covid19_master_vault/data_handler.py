import os
import csv
import requests
import shutil
import pickle

import pandas as pd
import boto3

class DataHandler():
    """
    Class to handle data transformation

    :param source_name: str / identifier to data sources name
    :param source_type: str / identifier to data source type (s3, html, big query, csv)
    :param transform_to_type: str / identifier to data it will be transformed
    :param file_name: str / file's name on storage point
    :param source_sql_string: str / sql string to data source
    """
    data_source_list = [
        {
            'id': 'JHU',
            'source_name': 'Johns Hopkins University',
            'source_type': 'Big Query',
            'sql_string': 'SELECT * FROM `bigquery-public-data.covid19_jhu_csse.summary`'
        },
        {
            'id': 'EDC',
            'source_name': 'Education Development Center',
            'source_type': 'Big Query',
            'sql_string':  'SELECT * FROM `bigquery-public-data.covid19_ecdc.covid_19_geographic_distribution_worldwide`'
        },
        {
            'id': 'OWID',
            'source_name': 'Our World in Data',
            'source_type': 'CSV'
        }
    ]
    absdirname = os.path.dirname(os.path.abspath(__file__))

    def __init__(self, source, file_name=None):
        self.id = source['id']
        self.source_name = source['source_name']
        self.source_type = source['source_type']
        self.sql_string = source.get('sql_string', None)
        self.file_name = file_name

    def __repr__(self):
        return '{} ({}) {}'.format(
            self.__class__.__name__,
            self.id,
            self.source_type
        )

    def __str__(self):
        return '<DataHandler(id={}, source_type={})>'.format(
            self.id,
            self.source_type
        )
    def extract_data(self, bqclient=None, bqstorageclient=None):
        """
        Extract data from differents source and write it on staging EXT area
        Known sources: CSV (OWD) n Big Query
        :return: panda dataframe with extracted data
        """
        if self.source_type == 'Big Query':
            dataframe = (
                    bqclient.query(self.sql_string)
                    .result()
                    .to_dataframe(bqstorage_client=bqstorageclient)
            )
            dataframe.to_csv('{}/EXT/RAW_{}_DATA.csv'.format(
                self.absdirname,
                self.id.upper(),
                sep='\t'
            ))
            pd.to_pickle(
                dataframe,
                '{}/EXT_PICKLE/RAW_{}_DATA.pkl'.format(self.absdirname, self.id))
            print('Raw {} succesfully extracted!'.format(self.id))
        elif self.source_type == 'CSV':
            url = 'https://covid.ourworldindata.org/data/owid-covid-data.csv'
            try:
                req = requests.get(url, verify=False, stream=True)
                req.raw.decode_content = True
                with open(
                    '{}/EXT/RAW_{}_DATA.csv'.format(self.absdirname, self.id.upper()), 'wb'
                ) as fb:
                    shutil.copyfileobj(req.raw, fb)
                with open(
                    '{}/EXT_PICKLE/RAW_{}_DATA.pkl'.format(self.absdirname, self.id.upper()), 'wb'
                ) as fb:
                    pickle.dump(req.text, fb) #options are .text .json() semnada
                print('Raw {} succesfully extracted!'.format(self.id))
            except requests.exceptions.RequestException as e:
                print('Failed to extract csv: exception {}'.format(e))


    def transform_data(self, input_format='csv'):
        """
        Aggregate data to date/country granularity, adds lat and long whether this data doenst exists
        :param ext_from: str/ csv or pickle, selecting wich data format to use and data input
        """
        country_latlong = '{}_country'.format(self.id)
        df_latlong = pd.read_csv(
            '{}/utils/dim_country.csv'.format(self.absdirname),
            usecols=[country_latlong,'latitude','longitude','iso_id']
        )
        df_latlong = df_latlong.rename(columns={country_latlong:'country'})
        if input_format == 'pickle':
            df = pd.read_pickle('{}/EXT_PICKLE/RAW_{}_DATA.pkl'.format(self.absdirname, self.id))
        else:
            df = pd.read_csv('{}/EXT/RAW_{}_DATA.csv'.format(self.absdirname, self.id))
        if self.id == 'JHU':
            country_map = 'country_region'
            deaths_map = 'deaths'
            cases_map = 'confirmed'
        elif self.id == 'EDC':
            country_map = 'countries_and_territories'
            deaths_map = 'deaths'
            cases_map = 'confirmed_cases'
        elif self.id == 'OWID':
            country_map = 'location'
            deaths_map = 'total_deaths'
            cases_map = 'total_cases'
        df_ren = df.rename(
            columns={
                country_map:'country',
                cases_map:'confirmed_cases',
                deaths_map:'deaths',
                'date':'date'
            }
        )
        df_agg = df_ren.groupby(['date','country']).agg(
            {'confirmed_cases':'sum','deaths':'sum'}
        )
        del df_ren
        df_agg = df_agg.reset_index(level=[0])#reset date as index
        print(df_agg.index)
        df_agg.to_csv('{}/TRA/{}_DATA_agg.csv'.format(
            self.absdirname,
            self.id.upper(),
            sep='\t'
        ))
        print('df_agg')
        print(df_agg.head())
        df_tra = pd.merge(df_agg, df_latlong, how='left', right_on='country', left_on='country')
        del df_agg
        print('df_tra')
        print(df_tra.head())
        df_tra.to_csv('{}/TRA/{}_DATA.csv'.format(
            self.absdirname,
            self.id.upper(),
            sep='\t'
        ))
        df_tra.to_json('{}/TRA/{}_DATA.json'.format(
            self.absdirname,
            self.id.upper(),
            orient='records'
        ))
        pd.to_pickle(df_tra, '{}/TRA_PICKLE/{}_DATA.pkl'.format(self.absdirname, self.id))
        del df_tra
        print('{} succesfully transformed!'.format(self.id))


    def upload_s3(
            self,
            bucket_name='covid19-datalake',
            input_extension='csv'):
        """
        Store transformed data to S3 providing proper file naming
        """
        s3 = boto3.resource('s3')
        for file_extension in ['JSON','CSV']:
            for data_state in ['RAW','TRANSFORMED']:
                file_path_s3 = '{0}/{1}/{2}/{3}_DATA.{4}'.format(
                    self.id,
                    data_state,
                    file_extension,
                    self.id,
                    str(file_extension).lower()
                )
                file_name = '{}/TRA/{}_DATA.{}'.format(self.absdirname, self.id, file_extension.lower())
                s3.meta.client.upload_file(
                    file_name,
                    bucket_name,
                    file_path_s3
                )
