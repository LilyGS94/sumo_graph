from datetime import datetime

from ..base_code.base_classes import SumoApiQuery


class SumoApiQueryBasho(SumoApiQuery):
    def __init__(self):
        super().__init__()

    def generate_timestamps(self):
        current_year = datetime.now().year
        current_month = datetime.now().month
        timestamps = []
        for year in range(1958, current_year + 1):
            for month in ["01", "03", "05", "07", "09", "11"]:
                if year == current_year and int(month) > current_month:
                    break
                timestamps.append(f"{year}{month}")
        self.iters = timestamps


if __name__ == "__main__":
    # setup_logging()
    query = SumoApiQueryBasho()
    query.generate_timestamps()
    print(f"Generated {len(query.iters)} timestamps to query.")
    query.run_queries()
    print("Process completed.")
    # example command for running this module python -m code.downloaders.pulling_data
