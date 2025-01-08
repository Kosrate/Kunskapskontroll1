import unittest
import pandas as pd
from data_pipeline import load_data, process_data

class TestDataPipeline(unittest.TestCase):
    def test_load_data(self):
        """Testa att data laddas korrekt."""
        df = load_data("test_data.csv")
        self.assertIsInstance(df, pd.DataFrame)

    def test_process_data(self):
        """Testa databehandling."""
        df = pd.DataFrame({"date": ["2023-01-01", "2023-01-02"]})
        processed_df = process_data(df)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(processed_df['date']))

if __name__ == "__main__":
    unittest.main()
