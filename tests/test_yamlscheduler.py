import os
import datetime
from datetime import timedelta
import logging
from yamlscheduler import YamlScheduler

import unittest   # The test framework

def testJob(myArgs):
    try:
        now = datetime.datetime.now()
        dt_string = now.strftime("%d-%m-%Y %H:%M:%S")
        logging.info(dt_string + ' job called with [' + ', '.join(myArgs) + ']')

    except Exception as e:
        logging.critical('test() Failed (' + e + ')')

class Test_YamlScheduler(unittest.TestCase):

    @classmethod 
    def setUpClass(self):
        f = open('test.yml', 'w')
        f.write('--- # Test Polling Schedule\n')
        f.write('weekdays:\n')
        f.write('  - weekday: # Test job one minute ahead, every 5 seconds\n')
        f.write('      name: "' + datetime.datetime.now().strftime("%A").lower() + '"\n')
        f.write('      start: "' + (datetime.datetime.now() + timedelta(minutes=1)).strftime('%H:%M') + '"\n')
        f.write('      end: "' + (datetime.datetime.now() + timedelta(minutes=2)).strftime('%H:%M') + '"\n')
        f.write('      seconds: 10\n')
        f.close()

    @classmethod
    def tearDownClass(self):
        os.remove('test.yml')

    def test_scheduler(self):
        try:
            logging.basicConfig(filename="yamlscheduler.log", filemode='w', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)        
            logging.info('===== START =====')
            sch = YamlScheduler()
            a = ['General', 'Kenobi']
            YamlScheduler.Initialise(logger=logging, jobToSchedule=testJob, jobArguments=a, config='test.yml')
            YamlScheduler.RunOnce(True)
            YamlScheduler.Wait()
            self.assertTrue(True)
            logging.info('===== END =====')

        except Exception as e:
            self.assertTrue(False, e)

if __name__ == '__main__':
    unittest.main()