import sys, os,logging, time, asyncio
import pandas as pd
from pyarrow import csv
from datetime import datetime


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)   
logger.addHandler(logging.StreamHandler(sys.stdout))

rulefile = os.environ.get("RULE_FILE_NAME", "rulesample.csv")


async def main():

  #pd.describe_option()
  #table = csv.read_csv("sampleRule.csv")
  #opt = csv.ReadOptions(column_names=["Name", "Creation-Time", "Metadata", "Tags"], skip_rows=1)
  
  isComplete = False

  while True:
    if not isComplete:
      logger.info("Start Parser" + " " + datetime.now().strftime("%m/%d/%Y, %H:%M:%S"))

      with csv.open_csv(rulefile) as reader:
        i = 1
        for next_chunk in reader:
          df = next_chunk.to_pandas()
          with pd.option_context('display.max_rows', None, 'display.max_colwidth', None, ):
            logger.info(df)
      
      logger.info("End Parser" + " " + datetime.now().strftime("%m/%d/%Y, %H:%M:%S"))

      isComplete = True
    else:
      time.sleep(60)
    
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

#df = table.to_pandas()

#df = pd.read_csv("rulenew.csv")
#par = df.to_parquet("large.parquet", compression=None)

#with pd.option_context('display.max_rows', None,):
#    print(df)

