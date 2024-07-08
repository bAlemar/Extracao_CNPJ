from src.pipeline.pipeline_bronze import Pipeline_Bronze
from src.pipeline.pipeline_silver import Pipeline_Silver
from src.pipeline.pipeline_silver_2 import Pipeline_Silver_2
from src.pipeline.pipeline_gold import PipeLineGold
import time
def main():
    # bronze = Pipeline_Bronze()
    # bronze.run()
    
    # silver_1 = Pipeline_Silver()
    # silver_1.run()
    
    # silver_2 = Pipeline_Silver_2()
    # silver_2.run()

    gold = PipeLineGold()
    gold.run()