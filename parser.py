import sys, os,logging, time, asyncio
import pandas as pd
from pyarrow import csv
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)   
logger.addHandler(logging.StreamHandler(sys.stdout))

rulefile = os.environ.get("RULE_FILE_NAME", "rulesamplenextday.csv")
current = datetime.now().date()
lastday = datetime.today() - timedelta(days=1)
fromdate = pd.to_datetime(str(lastday.year) + "-" + str(lastday.month) + "-" + str(lastday.day) + "T00:00:00Z")
todate = pd.to_datetime(str(current.year) + "-" + str(current.month) + "-" + str(current.day) + "T00:00:00Z")

isfirstrun = os.environ.get("IS_FIRST_RUN", "False").lower() in ('true', '1')

async def main():  
  isComplete = False

  while True:
    if not isComplete:
      logger.info("Start Parser" + " " + datetime.now().strftime("%m/%d/%Y, %H:%M:%S"))

      #with csv.open_csv(rulefile) as reader:
      #  i = 1
      #  for next_chunk in reader:     
      #    if i == 1:   
      #      df = next_chunk.to_pandas()
      #    else:
      #      df2 = next_chunk.to_pandas()
      #      df = df.append(df2)
      #    i = i + 1

      df = csv.read_csv(rulefile).to_pandas()

      if isfirstrun:
        df_filtered = df.loc[df['Creation-Time'] < fromdate]
      else:
        df_filtered = df.loc[(fromdate <= df['Creation-Time']) & (df['Creation-Time'] < todate)]        

      df_sorted = df_filtered.sort_values(by='Creation-Time')
      n = 1000  #chunk row size
      list_df = [df_sorted[i:i+n] for i in range(0,df_sorted.shape[0],n)]
      for chunk in list_df:
        logger.info(chunk.to_string())
        time.sleep(1) 

      logger.info("End Parser" + " " + datetime.now().strftime("%m/%d/%Y, %H:%M:%S"))
      isComplete = True
    else:
      time.sleep(60)
    
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
