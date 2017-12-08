# Company: CHT-PT
# Author: Chien-Yu Chen
# Date: 2017/11/23
# Mail: gooa1c2e3@gmail.com
# Support: Python 2.7

from sys import exit as _exit
from argparse import ArgumentParser
import time
from datetime import datetime
from collections import namedtuple
import traceback

try:
    from kafka import KafkaConsumer
    from kafka import TopicPartition
    from kafka.common import CommitFailedError
    from kafka.common import KafkaTimeoutError
    from kafka.common import NoBrokersAvailable
except ImportError as e:
    print "Error: Can not find kafka-python module, exit"
    _exit(1)
    

__version__ = "0.0.1_py27"        
    
def _get_parser():
    """ To parse arguments from command line input """
    class Options:
        def __init__(self):
            self._parser = self.__init__parser()
            
        def __init__parser(self):
            _usage = """Input topic and datetime, seeker could find data on kafka.
                Datetime string format: YYYY-MM-DDThh:mm:ss, eg.1970-01-01T00:00:00
                """
            _parser = ArgumentParser(description=_usage)
            _parser.add_argument(
                '-s', '--start', 
                help="Input datetime, it is necessary argument.", 
                action="store",
                dest='start'
                )

            _parser.add_argument(
                '-t', '--topic', 
                help='Input topic, it is necessary argument.', 
                action="store",
                dest='topic'
                )
            
            _parser.add_argument(
                '-d', '--debug', 
                help=""" Show exception traceback infomation """, 
                action="store_true",
                dest='debug'
                )
                    
            _parser.add_argument(
                '-S', '--Seek', 
                help="""Change the maximum number of data to seek & poll, 
                3 was set by default. 
                If -1 was set, unlimited mode will be turn on,
                seeker will try to poll all data from given offset by batches, 
                the maximum batch szie is 4000.""", 
                action="store",
                type = int,
                dest='seek'
                )
                
            _parser.add_argument(
                '-f', '--filepath', 
                help='Save polled data to the given file.', 
                action="store",
                dest='filepath'
                )
                
            _parser.add_argument(
                '-F', '--offset', 
                help="""Instead of the offset by input datetime, 
                the offset of this argument would be used to poll data.""", 
                action="store",
                type = int,
                dest='offset'
                )
                
            _parser.add_argument(
                '-p', '--partition', 
                help='Input partition, 0 was set by default.', 
                action="store",
                type = int,
                dest='partition'
                )
            
            _parser.add_argument(
                '-b', '--brokers', 
                help="""Input broker list, it is necessary argument.
                For example: 192.168.1.1:9091,192.168.1.5:9090 """, 
                action="store",
                dest='brokers'
                )
                
            _parser.add_argument(
                '-o', '--output_basic_info', 
                help=""" Print basic information. """, 
                action="store_true",
                dest='output'
                )
            
            _parser.add_argument(
                '-v', '--version', 
                help='show version', 
                action="version", 
                version='version= {}'.format(__version__)
                )
            return _parser
                            
    option = Options()
    return option._parser


class Seeker():
    """ Datetime string format: '%Y-%m-%dT%H:%M:%S' 
        eg. 2017-12-01T12:31:54 for 2017/12/01 12:31:54
    """
    def __init__(
        self,
        start_datetime, 
        topic,
        brokers=None, 
        partition=None,
        seek_num=3,
        path=None,
        force=None,
    ):
        self._partition = 0
        #self._brokers = ["192.168.1.189:9092"]
        
        self._beginning_data = None
        self._last_data = None
 
        
        self._CR_namedtp = namedtuple(
                'ConsumerRecord', 
                ['topic', 'partition', 'offset', 'timestamp','value']
            )
        
        if start_datetime:
            try:
                self._start_datetime = self._to_datetime_obj(start_datetime)
                self._start_timestamp = self.datetime2timestamp(start_datetime)
            except ValueError as e:
                print "{} is not match format YYYY-MM-DDThh:mm:ss".format(start_datetime)
                _exit(1)
        else:
            print "Must input the argument: -s start_datetime"
            _exit(1)
            
        if topic:
            self._topic = topic
        else:
            print "Must input the argument: -t topic"
            _exit(1)
        
        if partition:
            self._partition = self._parse_partition_string(partition)
        
        if brokers:
            self._brokers = self._parse_broker_string(brokers)
        else:
            print "Must input the argument: -b brokers"
            _exit(1)
        
        if seek_num:
            self._seek_num = seek_num
        else:
            self._seek_num = 3
        
        if path:
            self.file_path = path
        else:
            self.file_path = None
            
        if force:
            self._force_offset = force
        else:
            self._force_offset = None
        
        self._data = None
        self._toparty = TopicPartition(self.topic, self._partition)
        
    def seek_and_poll(self):
        """ Wrap KafkaConsumer.seek & poll function"""
        try:
            if self._force_offset:
                print "Using -f offset: {}".format(self._force_offset)
                _tmp_offset = self._force_offset
                self._start_offset = None
            else:            
                _tmp = self.get_offset(self._start_timestamp)
                self._start_offset = _tmp[self._toparty].offset
                print "Using datetime to offset: {}".format(self._start_offset)
                _tmp_offset = self._start_offset
               
            self._consumer.seek(self._toparty, _tmp_offset)
            if self._seek_num > -1:
                self._data = self._consumer.poll(timeout_ms=3000, max_records=self._seek_num)
                try:
                    _batch = self._data.values()[0]
                    _batch_size = len(_batch)
                except IndexError as e:
                    _batch = None
                    _batch_size = 0
                print "Polled batch size: {}".format(_batch_size)
                if _batch:
                    if self.file_path:
                        if _batch_size > 0:
                            self._dump_to_file(mode='w', batch=_batch)
                        else:
                            print "No data to dump"
                    else:
                        self._print_record(_batch)
                else:
                    print "offset: {} Data: None".format(_tmp_offset)
                    
            elif self._seek_num==-1:
                print "seek num was set in -1, turn on unlimited mode, task start:"
                self._no_data_can_be_polled = False
                while not self._no_data_can_be_polled:
                    self._data = self._consumer.poll(timeout_ms=3000, max_records=4000)
                    try:
                        _batch = self._data.values()[0]
                        _batch_size = len(_batch)
                    except IndexError as e:
                        break
                    print "Polled batch size: {}".format(_batch_size)
                    if _batch_size==0:
                        self._no_data_can_be_polled = True
                        print "The task is finished"
                        break
                    else:
                        if self.file_path:
                            self._dump_to_file(mode='a', batch=_batch)
                        else:
                            self._print_record(_batch)
                    time.sleep(0.5)
            else:
                print "Illegal seek number was found: {}, exit".format(self._seek_num) 
                _exit(1)
        except KafkaTimeoutError as e:
            print str(e)
            _exit(1)
        except NoBrokersAvailable as e:
            print str(e)
            _exit(1)
    
    
    def connect_borkers(self):
        try:
            print "Connecting to kafka brokers: {}...".format(self._brokers),
            self._consumer = KafkaConsumer(bootstrap_servers=self._brokers)
            self.assign_topic()
        except NoBrokersAvailable as e:
            print str(e)
            print "Failed, exit"
            _exit(1)
        print "Succeed"
        
    def reconnect_brokers(self):
        self.close()
        self.connect_borkers()
        
    def assign_topic(self):
        try:
            self._consumer.assign([self._toparty])
            self.seek_beginning_offset()
            self.seek_last_offset()
        except ValueError:
            print "Connection maybe closed, can not assign topic"
    
    def _print_record(self, batch):
        for _record in batch:
            print "Offset:", _record.offset, ", Data:", _record.value
    
    def _dump_to_file(self, mode, batch):
        print "Dump data to {}...".format(self.file_path),
        try:
            with open(self.file_path, mode) as f:
                for _record in batch:
                    f.write(_record.value + "\n")
        except Exception as e:
            print "Failed"
            print str(e)
        print "Done"
    
    def seek_last_offset(self):
        self._consumer.seek_to_end(self._toparty)
        self._last_data = self._consumer.poll(timeout_ms=3000, max_records=1)
    
    def seek_beginning_offset(self):
        self._consumer.seek_to_beginning(self._toparty)
        self._beginning_data = self._consumer.poll(timeout_ms=3000, max_records=1)
        
    def _to_datetime_obj(self, datetime_string):
        return datetime.strptime(datetime_string, "%Y-%m-%dT%H:%M:%S")
    
    @property
    def file_path(self):
        return self.file_path
    
    @property
    def fource(self):
        return self._force_offset
    
    @property    
    def start_datetime(self):
        """ The start datetime in seeking data protcol"""
        return self._start_datetime
    
    @start_datetime.setter    
    def set_start_datetime(self, datetime):
        try:
            self._start_datetime = self._to_datetime_obj(datetime)
            self._start_timestamp = self.datetime2timestamp(datetime)
        except ValueError as e:
            print "{} is not match format YYYY-MM-DDThh:mm:ss".format(datetime)
    
    @property
    def seek_num(self):
        return self._seek_num
    
    @property
    def start_timestamp(self):
        return self._start_timestamp
        
    @property
    def start_offset(self):
        return self._start_offset
    
    @property
    def topic(self):
        return self._topic
        
    @property
    def beginning_data(self):
        if self._beginning_data is None:
            self.seek_beginning_offset()
        if self._beginning_data == {}:
            return self._CR_namedtp(
                topic=self._topic,
                partition=self._partition,
                timestamp=None,
                value=None,
                offset=None
            )
        return self._beginning_data[self._toparty][0]
        
    @property
    def last_data(self):
        if self._last_data is None:
            self.seek_last_offset()
        if self._last_data == {}:
            return self._CR_namedtp(
                topic=self._topic,
                partition=self._partition,
                timestamp=None,
                value=None,
                offset=None
            )
        return self._last_data[self._toparty][0]
        
    @topic.setter
    def set_topic(self, topic):
        self._topic = topic
        self._toparty = TopicPartition(self.topic, self._partition)
        self.assign_topic()
        
    @property
    def brokers(self):
        return self._brokers
    
    @brokers.setter
    def set_brokers(self, brokers):
        self._brokers = self_parse_broker_string(brokers)
        self.reconnect_brokers()
        
    def _parse_broker_string(self, brokers):
        if isinstance(brokers, list):
            return borkers
        elif isinstance(brokers, str):
            return [broker.strip() for broker in brokers.split(',')]
            
    @property
    def partition(self):
        return self._partition

    @partition.setter
    def set_partition(self, partition):
        self._partition = self._parse_partition_string(partition)
        self._toparty = TopicPartition(self.topic, self._partition)
        self.assign_topic()
                
    def _parse_partition_string(self, partition):
        if isinstance(partition, int):
            return partition
        elif isinstance(partition, str):
            try:
                return int(partition)
            except ValueError as e:
                print "Invalid partition string was found: {}".format(partition)
        
    def datetime2timestamp(self, datetime_string):
        time_tuple = time.strptime(datetime_string, "%Y-%m-%dT%H:%M:%S")
        return time.mktime(time_tuple)
        
    def get_offset(self, _timestamp):
        try:
            print "To get offset by timpstamp: {}".format(_timestamp)
            return self._consumer.offsets_for_times({self._toparty:_timestamp})
        except ValueError:
            print "Connection maybe closed, try to reconnect"
            self.connect_borkers()
            return self._consumer.offsets_for_times({self._toparty:_timestamp})

    def close(self):
        print "Close connection"
        self._consumer.close()
            

if __name__=="__main__":
    parser = _get_parser()
    args = parser.parse_args()
    
    try:
        seeker = Seeker(
            start_datetime=args.start, 
            topic=args.topic,
            brokers=args.brokers, 
            partition=args.partition,
            seek_num=args.seek,
            path=args.filepath,
            force=args.offset
        )
        
        seeker.connect_borkers()
        seeker.seek_and_poll()
        seeker.close()
        
        if args.output:
            print "\n","===== Input information ====="
            print "Input offset:", seeker.fource
            print "Input datetime:", seeker.start_datetime
            print "Input timpstamp:", seeker.start_timestamp
            print "Datetime to offset:", seeker.start_offset
            print "File path:", seeker.file_path
            print "seek number:", seeker.seek_num
            
            print "\n","===== Topic information ====="
            print "Brokers:", seeker.brokers
            print "Partition:", seeker.partition
            print "Topic:", seeker.topic
            print "Fisrt offset on kafka:", seeker.beginning_data.offset
            print "Last offset on kafka:", seeker.last_data.offset
        
    except Exception as e:
        print str(e)
        if args.debug:
            traceback.print_exc()
    _exit(0)