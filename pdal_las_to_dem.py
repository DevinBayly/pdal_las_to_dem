import pdal
from pathlib import Path
import multiprocessing as mp
import time 


def process(fl,i):
    pj =f'''[
      "{fl}",
    {{
        "filename":"./dtm{i}_{fl.stem}.tif",
        "gdaldriver":"GTiff",
        "output_type":"all",
        "resolution":"2.0",
        "type": "writers.gdal"
    }}
]'''

    print(pj)
    pipeline = pdal.Pipeline(pj)
    count = pipeline.execute()
    arrays = pipeline.arrays
    etadata = pipeline.metadata
    log = pipeline.log

las_files = list(Path("/data/input/files/LIDAR/2015").glob("**/*.las"))

def runTest(i):
    print("starting")
    time.sleep(i)
    print(i)

with mp.Pool(processes = mp.cpu_count()) as pool:
    procs = [pool.apply_async(process,(fl,i)) for i,fl in enumerate(las_files)]
    [res.wait() for res in procs]
    [res.get() for res in procs]

print("done!")

