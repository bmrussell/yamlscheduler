import datetime
from yamlscheduler import YamlScheduler

def test():
    try:
        now = datetime.datetime.now()
        dt_string = now.strftime("%d-%m-%Y %H:%M:%S")
        print(dt_string + ' job called')        
    except Exception as e:
        print('test() Failed (' + e + ')')

def main() -> None:
    try:
        print('===== START =====')
        sch = YamlScheduler()
        YamlScheduler.Initialise(test)
        YamlScheduler.Wait()
        print('===== END =====')
    except Exception as e:
        print('Failed (' + e + ')')
    finally:
        print('Done.')

if __name__ == '__main__':
    main()
