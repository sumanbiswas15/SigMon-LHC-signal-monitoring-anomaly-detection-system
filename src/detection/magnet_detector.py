import pandas as pd
import numpy as np


def threshold_check(voltage, resistance, temperature):
    if voltage > 0.005 or resistance > 1e-4 or temperature > 3.0:
        return True
    return False


def zscore_check(value, mean, std):
    if std == 0:
        return False
    z = (value - mean) / std
    return abs(z) > 3


class PersistenceDetector:
    def __init__(self, window=3):
        self.window = window
        self.history = []

    def update(self, is_anomaly):
        self.history.append(is_anomaly)
        if len(self.history) > self.window:
            self.history.pop(0)

        return all(self.history)


class MagnetQuenchDetector:
    def __init__(self, use_persistence: bool = False, persistence_window: int = 3):
        self.stats = {}
        self.persistence_detectors = {}
        self.use_persistence = use_persistence
        self.persistence_window = persistence_window
    
    def fit(self, df: pd.DataFrame):
        magnet_ids = df["magnet_id"].unique()
        
        for magnet_id in magnet_ids:
            magnet_df = df[df["magnet_id"] == magnet_id]
            self.stats[magnet_id] = {
                "v_mean": magnet_df["voltage_diff"].mean(),
                "v_std": magnet_df["voltage_diff"].std(),
                "r_mean": magnet_df["resistance"].mean(),
                "r_std": magnet_df["resistance"].std(),
                "t_mean": magnet_df["temperature"].mean(),
                "t_std": magnet_df["temperature"].std()
            }
            self.persistence_detectors[magnet_id] = PersistenceDetector(window=self.persistence_window)
    
    def detect_single(self, row: pd.Series) -> int:
        magnet_id = row["magnet_id"]
        
        if magnet_id not in self.stats or magnet_id not in self.persistence_detectors:
            return 0
        
        voltage = row["voltage_diff"]
        resistance = row["resistance"]
        temperature = row["temperature"]
        
        stats = self.stats[magnet_id]
        persistence_detector = self.persistence_detectors[magnet_id]
        
        threshold_flag = threshold_check(voltage, resistance, temperature)
        
        def zscore_high_only(value, mean, std):
            if std == 0:
                return False
            z = (value - mean) / std
            return z > 3
        
        z_voltage = zscore_high_only(voltage, stats["v_mean"], stats["v_std"])
        z_resistance = zscore_high_only(resistance, stats["r_mean"], stats["r_std"])
        z_temp = zscore_high_only(temperature, stats["t_mean"], stats["t_std"])
        
        statistical_flag = z_voltage or z_resistance or z_temp
        
        raw_anomaly = threshold_flag or statistical_flag
        
        if self.use_persistence:
            final_flag = persistence_detector.update(raw_anomaly)
        else:
            final_flag = raw_anomaly
        
        return 1 if final_flag else 0
    
    def detect_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        detected_flags = []
        
        for _, row in df.iterrows():
            detected_flags.append(self.detect_single(row))
        
        df["detected_quench_flag"] = detected_flags
        return df
