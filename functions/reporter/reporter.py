import pandas as pd
import boto3
import json
import logging
from typing import Type
import os
logger = logging.getLogger()

logger.setLevel("INFO")

ReporterType = Type['Reporter']

class Reporter():
    def __init__(self):
        self.s3 = boto3.client('s3')
        self.census_df = None
        self.pr_df = None
        
        # Create tmp directory if it doesn't exist
        if not os.path.exists("/tmp"):
            os.makedirs("/tmp")


    def load_data(self, s3_bucket: str) -> ReporterType:
        logger.info(f"Loading data from S3 bucket {s3_bucket}")
        
        self.s3.download_file(s3_bucket, 'census_data.json', '/tmp/census_data.json')
        self.s3.download_file(s3_bucket, 'productivity_cost/2024-06-06/pr.data.0.Current', '/tmp/pr.data.0.Current')
        self.pr_df = pd.read_csv('/tmp/pr.data.0.Current', sep='\t')

        # Load the census JSON file into a dataframe.
        with open('/tmp/census_data.json') as f:
            data = json.load(f)

        data_to_convert = data['data']
        self.census_df = pd.DataFrame(data_to_convert)
        logger.info("Data loaded successfully")
        return self
    
    
    def clean_data(self) -> ReporterType:
        logger.info("Cleaning data")
        # Strip whitespace from column names and values
        self.pr_df.columns = self.pr_df.columns.str.strip()
        self.census_df.columns = self.census_df.columns.str.strip()
        self.pr_df = self.pr_df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        self.census_df = self.census_df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

        # Normalize Dates for 
        self.census_df['Year'] = pd.to_datetime(self.census_df['Year'], format='%Y')
        self.pr_df['year'] = pd.to_datetime(self.pr_df['year'], format='%Y')

        logger.info("Data cleaned successfully")
        return self


    def print_census_summary(self) -> None:
        logger.info("Printing census summary")
        start_year = pd.to_datetime('2013', format='%Y')
        end_year = pd.to_datetime('2018', format='%Y')

        filtered_census_df = self.census_df[(self.census_df['Year'] >= start_year) & (self.census_df['Year'] <= end_year)]

        population_mean = filtered_census_df['Population'].mean()
        population_std = filtered_census_df['Population'].std()

        logger.info(f"Mean: {population_mean}")
        logger.info(f"Standard Deviation: {population_std}")
        logger.info("Census summary printed successfully")

    
    def print_pr_summary(self) -> None:   
        logger.info("Printing Productivity and Costs summary")
        grouped = self.pr_df.groupby(['series_id', 'year'])['value'].sum()
        grouped = grouped.reset_index()
        best_years = grouped.loc[grouped.groupby('series_id')['value'].idxmax()]

        logger.info("Best years for each series:")
        logger.info(best_years.to_string())
        logger.info("Productivity and Costs summary printed successfully")


    def print_report(self) -> None:
        logger.info("Printing report for Analysis of Productivity and Costs for 2013-2018 for series_id PRS30006032 and period Q01")
        filtered_pr_df = self.pr_df[(self.pr_df['series_id'] == 'PRS30006032') & (self.pr_df['period'] == 'Q01')]

        merged_df = pd.merge(filtered_pr_df, self.census_df, left_on='year', right_on="Year", how='left')
        report_df = merged_df[['series_id', 'year', 'period', 'value', 'Population']]

        logger.info("Report:")
        logger.info(report_df.to_string())
        logger.info("Report printed successfully")
    