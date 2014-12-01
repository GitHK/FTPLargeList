# -*- coding: utf-8 -*-
from collections import deque
from time import strftime
import ftplib
from retrying import retry
from datetime import datetime
import logging


class FTPHelper:
    __host = None
    __port = 21
    __user = None
    __password = None
    __timeout = 5
    
    _ftp = None
    __ftp_working_directory = '/'
    
    def __init__(self, host, user, password, port=21, timeout=5):
        self.__host = host
        self.__port = port
        self.__user = user
        self.__password = password
        self.__timeout = timeout
    
    @retry(stop_max_attempt_number=5)
    def connect(self):
        if self._ftp == None:
            ftp = ftplib.FTP()
            ftp.connect(host=self.__host,
                        port=self.__port,
                        timeout=float(self.__timeout))
                        if self.__user.lower() == 'anonymous':
                            ftp.login()
                        else:
                            ftp.login(user=self.__user,
                                      passwd=self.__password)
                                self._ftp = ftp
                                      return self._ftp

def reconnect(self):
    self._ftp.close()
        self._ftp = None
        connection = self.connect()
        self.set_directory(self.__ftp_working_directory)
        return connection
    
    @retry(stop_max_attempt_number=5)
    def set_directory(self, directory_path):
        self.__ftp_working_directory = directory_path
        self._ftp.cwd(directory_path)
    
    @retry(stop_max_attempt_number=5)
    def list_directory(self):
        data = []
        self._ftp.dir(data.append)
        for line in data:
            print line

@retry(stop_max_attempt_number=2)
    def file_mdtm(self, file):
        try:
            ftp_time_string = self._ftp.sendcmd('MDTM ' + file)
            return datetime.strptime(ftp_time_string[4:], "%Y%m%d%H%M%S")
        except ftplib.error_perm:
            return None


def _nlst_deque(self, *args):
    cmd = 'NLST'
        for arg in args:
            cmd = cmd + (' ' + arg)
    files = deque()
        self._ftp.retrlines(cmd, files.append)
        return files

def current_directory_as_list(self):
    files = deque()
        try:
            files = self._nlst_deque()
    except ftplib.error_perm, resp:
        if not str(resp) == "550 No files found":
            raise
        return files

@retry(stop_max_attempt_number=5)
    def pwd(self):
        return self._ftp.pwd()
    
    @retry(stop_max_attempt_number=5)
    def remote_file_size(self, file):
        return self._ftp.size(file)
    
    
    def download_file(self, remote_file, local_file, max_attempts=5):
        
        def check_size(remote, local):
            if local > remote:
                raise FTPDownloadException(
                                           'Current saved file is larger then the remote one %s > %s' % (
                                                                                                         file.tell(), remote))
        
        with open(local_file, 'ab+') as file:
            
            link = self._ftp
            remote_file_size = self.remote_file_size(remote_file)
            
            while remote_file_size > file.tell():
                try:
                    check_size(remote_file_size, file.tell())
                    link.sendcmd("TYPE I")
                    link.sendcmd("REST " + str(file.tell()))
                    if file.tell() == 0:
                        link.retrbinary('RETR ' + remote_file, file.write)
                    else:
                        link.retrbinary('RETR ' + remote_file, file.write, file.tell())
                except Exception as myerror:
                    if max_attempts != 0:
                        logging.warning("%s while > except, something going wrong: %s\n \tfile lenght is: %i > %i\n" %
                                        (strftime("%d-%m-%Y %H.%M"), myerror, remote_file_size, file.tell()))
                                        link = self.reconnect()
                                        max_attempts -= 1
                    else:
                        if max_attempts == 0:
                            raise FTPDownloadException('No success downloading file %s' % file.name)
                        break
                                                                                                         check_size(remote_file_size, file.tell())
                        file.close()


class FTPDownloadException(Exception):
    def __init__(self, value):
        self.parameter = value
    
    def __str__(self):
        return repr(self.parameter)
