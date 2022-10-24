import sys, os,logging, time, asyncio
import pandas as pd
from pyarrow import csv
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)   
logger.addHandler(logging.StreamHandler(sys.stdout))

rulefile = os.environ.get("RULE_FILE_NAME", "rulesample.csv")


async def main():  
  isComplete = False

  while True:
    if not isComplete:
      logger.info("Start Parser" + " " + datetime.now().strftime("%m/%d/%Y, %H:%M:%S"))

      with csv.open_csv(rulefile) as reader:
        i = 1
        for next_chunk in reader:        
          df = next_chunk.to_pandas()
          logger.info(df.to_string())
          time.sleep(3)

      logger.info("End Parser" + " " + datetime.now().strftime("%m/%d/%Y, %H:%M:%S"))
      isComplete = True
    else:
      time.sleep(60)
    
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
