import yaml
import schedule
import time

# TODO: Remove print statements and add proper logging

class YamlScheduler(object):
    """YamlScheduler: Singleton for in process scheduling
    Reads schedule.yml containing:
    
    List of weekdays that define a start-end window to run the task for at a desired interval
    Days need not be unique

        weekdays:
        - weekday:
            name: "tuesday"
            start: "21:00"
            end: "22:00"
            minutes: 5

    Interval period in YAML can be specified as seconds, minutes or hours
    """    
    _instance = None
    __signal = None

    def __new__(cls):
        """Singleton initialisation. Not much to see here. Move along. Move along.

        """        
        if cls._instance is None:            
            cls._instance = super(YamlScheduler, cls).__new__(cls)
            # Put any initialization here.
            __signal = False

        return cls._instance

    @classmethod
    def Wait(self):
        """Wait: blocks main execution thread waiting for next job.
        Might be an idea to set some jobs scheduled first.
        """        
        while not self.__signal:
            schedule.run_pending()
            time.sleep(1)

    @classmethod
    def Stop(self):
        """Stop: Stop the main execution loop.
        Something altogether more sophisticated required for multithreading
        """        
        self.__signal = True

    @classmethod
    def __StartWindow(self, job, interval,  intervalUnit, tag):
        """__StartWindow: Internal method that starts a window that the job will run in.
        Called by the scheduler at the start of the window.

        Args:
            job (fn)       : The python function to call
            interval (int) : How frequently to call the job during the time window. Seconds for development, minutes for final
            tag (string)   : What to tag this job with. Used to cancel the job at the end of the window
                             format is s_[weekday name]_hh:mm_hh:mm
        """
        print('Starting ' + tag + ' every ' + str(interval) + ' ' + intervalUnit + '...')
        if intervalUnit == 'seconds':
            schedule.every(interval).seconds.do(job).tag(tag)
        elif intervalUnit == 'minutes':
            schedule.every(interval).minutes.do(job).tag(tag)
        elif intervalUnit == 'hours':
            schedule.every(interval).hours.do(job).tag(tag)

    @classmethod
    def __StopWindow(self, tag):
        """__StopWindow: Internal method that finished a window that the job will run in.
        Called by the scheduler at the end of the window.

        Args:
            tag (string)   : The job tag to cancel. 
                             format is s_[weekday name]_hh:mm_hh:mm
        """
        print('Stopping ' + tag + '...')
        schedule.clear(tag)

    @classmethod
    def Initialise(self, jobToSchedule, jobArguments):
        """Initialise: Set up the scheduled jobs from the YAML file.

        Args:
            jobToSchedule (fn) : The job to run
        
        Todo: 
            Maybe extend to pass a {name,fn} dictionary so that can be referenced from the YAML file too.
        """
        try:

            fndict = {
                'monday': (schedule.every().monday.at),
                'tuesday': (schedule.every().tuesday.at),
                'wednesday': (schedule.every().wednesday.at),
                'thursday': (schedule.every().thursday.at),
                'friday': (schedule.every().friday.at),
                'saturday': (schedule.every().saturday.at),
                'sunday': (schedule.every().sunday.at)
            }
            
            #
            # The below works but I don't know why declaring a dictionary as above
            # and calling like this 
            #   fndict[dayname](windowStart).do(YamlScheduler.__StartWindow, job=jobToSchedule, interval=minutes, tag=jobTag)
            # doesn't work

            self.__signal = False
            with open("schedule.yml", "r") as ymlfile:
                cfg = yaml.load(ymlfile, yaml.FullLoader)
                weekdays = cfg['weekdays']
                if weekdays is not None:
                    for scheduleInstance in weekdays:
                        weekday = scheduleInstance['weekday']
                        dayname = weekday['name']
                        windowStart = weekday['start']
                        windowEnd = weekday['end']                        
                        jobTag = 's_' + dayname + '_' + windowStart + '_' + windowEnd
                        
                        # Get the interval
                        intervalNames = ['seconds', 'minutes', 'hours']
                        intervalName = None
                        for iName in intervalNames:
                            if iName in weekday:
                                intervalName = iName
                        
                        # Now we know the interval get its value
                        if intervalName is None:
                            raise('Interval for schedule must be given as seconds, minutes or hours')
                        intervalValue = int(weekday[intervalName])

                        if dayname == 'monday':
                            schedule.every().monday.at(windowStart).do(YamlScheduler.__StartWindow, job=jobToSchedule, interval=intervalValue, intervalUnit=intervalName, tag=jobTag)
                            schedule.every().monday.at(windowEnd).do(YamlScheduler.__StopWindow, tag=jobTag)
                        
                        if dayname == 'tuesday':
                            schedule.every().tuesday.at(windowStart).do(YamlScheduler.__StartWindow, job=jobToSchedule, interval=intervalValue, intervalUnit=intervalName, tag=jobTag)
                            schedule.every().tuesday.at(windowEnd).do(YamlScheduler.__StopWindow, tag=jobTag)
                        
                        if dayname == 'wednesday':
                            schedule.every().wednesday.at(windowStart).do(YamlScheduler.__StartWindow, job=jobToSchedule, interval=intervalValue, intervalUnit=intervalName, tag=jobTag)
                            schedule.every().wednesday.at(windowEnd).do(YamlScheduler.__StopWindow, tag=jobTag)
                        
                        if dayname == 'thursday':
                            schedule.every().thursday.at(windowStart).do(YamlScheduler.__StartWindow, job=jobToSchedule, interval=intervalValue, intervalUnit=intervalName, tag=jobTag)
                            schedule.every().thursday.at(windowEnd).do(YamlScheduler.__StopWindow, tag=jobTag)
                        
                        if dayname == 'friday':
                            schedule.every().friday.at(windowStart).do(YamlScheduler.__StartWindow, job=jobToSchedule, interval=intervalValue, intervalUnit=intervalName, tag=jobTag)
                            schedule.every().friday.at(windowEnd).do(YamlScheduler.__StopWindow, tag=jobTag)
                        
                        if dayname == 'saturday':
                            schedule.every().saturday.at(windowStart).do(YamlScheduler.__StartWindow, job=jobToSchedule, interval=intervalValue, intervalUnit=intervalName, tag=jobTag)
                            schedule.every().saturday.at(windowEnd).do(YamlScheduler.__StopWindow, tag=jobTag)
                        
                        if dayname == 'sunday':
                            schedule.every().sunday.at(windowStart).do(YamlScheduler.__StartWindow, job=jobToSchedule, interval=intervalValue, intervalUnit=intervalName, tag=jobTag)
                            schedule.every().sunday.at(windowEnd).do(YamlScheduler.__StopWindow, tag=jobTag)


        except Exception as e:
            print('Failed (' + e + ')')