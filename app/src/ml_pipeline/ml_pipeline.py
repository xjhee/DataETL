from configparser import ConfigParser
from category_encoders import BinaryEncoder
from src.ml_pipeline.helper import to_timestamp
from src.ml_pipeline.algorithms import IsolationForests
from src.ml_pipeline.algorithms import LocalOutlierFactor
from src.ml_pipeline.algorithms import HistogramBasedOutlierScore
import logging

logging.basicConfig(level = logging.INFO)

class ml_pipeline:

    def __init__(self, df):

        self.df = df


    def encode_categorical_data(self):

        cols = [ 
        'timestamp',
        'pod_id', 
        'host', 
        'container_name', 
        'deploy_time', 
        'exception_message_detailed', 
        'level1_exception_message_filtered', 
        'level2_exception_classes', 
        'level3_exception_causes', 
        'exception_logger'      
       ]

        target_data = self.df[cols]
        target_data['timestamp'] = target_data['timestamp'].apply(lambda x: to_timestamp(x))
        target_data['deploy_time'] = target_data['deploy_time'].apply(lambda x: to_timestamp(x))
        target_data = target_data.fillna('unknown')

        encoder = BinaryEncoder()
        encoded_target_data = encoder.fit_transform(target_data) 

        return encoded_target_data

 
    def generate_outage_report(self, algorithm):

        encoded_data = self.encode_categorical_data()
        target_algorithm = algorithm
        detected_anomalies = None

        if target_algorithm == 'Histogram-Based-Outlier-Score':
            detected_anomalies = HistogramBasedOutlierScore(self.df, encoded_data).detection_hbos()
        elif target_algorithm == 'Local-Outlier-Factor':
            detected_anomalies = LocalOutlierFactor(self.df, encoded_data).detection_lof()
        elif target_algorithm == 'Isolation-Forest':
            detected_anomalies = IsolationForests(self.df, encoded_data).detection_iforest()

        return detected_anomalies

    




if __name__ == '__main__':
    """
    df: output data from real_time_outage_detection().clean_data()
    """
    ml_pipeline(df).generate_outage_report()