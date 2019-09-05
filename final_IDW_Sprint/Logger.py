import re
from datetime import *

class Logger:
    def __init__(self, path):
        self.path = path    
        self.log_list = ''

    def add_log(self, log_type, log_msg):
        sys_ts = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
        self.log_list += '\n[' + sys_ts + '][' + log_type + ']' + ' '*(5-len(log_type))+ ':' + log_msg

    def add_log_details(self, log_msg):
        self.log_list += '\n'+ re.sub(r'^', ' '*30, log_msg, flags=re.M)

    def append_sublog(self, log):
        self.log_list += log

    def get_log(self):
        return self.log_list.strip()

    def print_log(self):
        log_file = self.path + '\\tsql_convert_' + datetime.now().strftime('%m-%d-%Y_%H-%M-%S') + '.log'
        with open(log_file, 'w') as f:
            f.write(self.get_log())