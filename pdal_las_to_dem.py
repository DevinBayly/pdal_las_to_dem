import pdal
from pathlib import Path
import multiprocessing as mp


def process(fl,i):
    pj = f'''[
          "{fl}",
        {
            "filename":"./dtm{i}_{fl.name}.tif",
            "gdaldriver":"GTiff",
            "output_type":"all",
            "resolution":"2.0",
            "type": "writers.gdal"
        }
        

    ]'''
    pipeline = pdal.Pipeline(pj)

    count = pipeline.execute()
    arrays = pipeline.arrays
    metadata = pipeline.metadata
    log = pipeline.log

las_files = list(Path("/data/input/files/LIDAR/2015").glob("**/*.las"))[:6]

with mp.Pool(processes = 6) as pool:
    procs = [pool.apply_async(process,(fl,i,)) for i,fl in enumerate(las_files)]
    pool.close()
    pool.join()

print("done!")

