import dask.dataframe as dd
from dask import delayed
import dask
from dask.diagnostics import ProgressBar
import timeit




# pbar = ProgressBar()
# pbar.register()

df = dd.read_csv("crime.csv", dtype=str, error_bad_lines=False, warn_bad_lines=False)
# df = df.map_partitions(lambda df, i: df.assign(record=i), meta=('record', 'i8'))
# df = df.compute()
# print(df.head())


start_time = timeit.default_timer()
print(df.count().compute())
# df.count().compute()
end_time = timeit.default_timer()
print('Elapsed Time:', end_time - start_time)
