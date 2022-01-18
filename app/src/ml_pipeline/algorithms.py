from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
from pyod.models.hbos import HBOS


class HistogramBasedOutlierScore:
    def __init__(self, df, encoded_df):

        self.df = df
        self.encoded_df = encoded_df


    def detection_hbos(self, outliers_fraction = 0.005):

        clf = HBOS(contamination = outliers_fraction, n_neighbors = 20)
        clf.fit(self.encoded_df)
        pred = clf.predict(self.encoded_df)

        df_cp = self.df.copy()
        df_cp['isAnomaly'] = pred
        detected_anomalies_detail = df_cp[df_cp['isAnomaly'] == 1]

        return detected_anomalies_detail



class LocalOutlierFactor:
    def __init__(self, df, encoded_df):

        self.df = df
        self.encoded_df = encoded_df


    def detection_lof(self, outliers_fraction = 0.005):

        clf = LocalOutlierFactor(n_neighbors=20, contamination=outliers_fraction)
        pred = clf.fit_predict(self.encoded_df)

        df_cp = self.df.copy()
        df_cp['isAnomaly'] = pred
        detected_anomalies_detail = df_cp[df_cp['isAnomaly'] == -1]

        return detected_anomalies_detail



class IsolationForests:
    def __init__(self, df, encoded_df):

        self.df = df
        self.encoded_df = encoded_df

    def detection_iforest(self, outliers_fraction = 0.005):

        clf = IsolationForest(n_estimators = 100, max_samples = 'auto', \
                            max_features = 1.0, bootstrap = False, n_jobs = -1, \
                            contamination = outliers_fraction, random_state = 42, verbose = 0)
        clf.fit(self.encoded_df)
        pred = clf.predict(self.encoded_df)

        df_cp = self.df.copy()
        df_cp['isAnomaly'] = pred
        detected_anomalies_detail = df_cp[df_cp['isAnomaly'] == -1]

        return detected_anomalies_detail



