import json
import os
import time


def main():
    while 1 == 1:
        os.system('python3 /home/suman/PycharmProjects/batch_lake_to_stage_with_dup/load_data_from_lake_to_stage.py')
        os.system('python3 /home/suman/PycharmProjects/data_load_from_raw_to_presentation/data_load_from_raw_to_presentation.py')
        print('start refreshing production table')
        os.system('python3 /home/suman/PycharmProjects/move_data_to_production/move_data_to_prod.py')
        print('waiting for 300 seconds')
        time.sleep(120)


if __name__ == "__main__":
    main()
