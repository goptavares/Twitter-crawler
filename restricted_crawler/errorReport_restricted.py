import datetime
import traceback
import sys
import os


def getErrorReport():
    errorReport = ErrorReport()
    return errorReport


class ErrorReport():        

    def __init__(self):
        return

    def startLog(self):
        timestamp = str(datetime.datetime.now())
        fileName = 'Log_'+timestamp+'.txt.'
        self.logFile = open(fileName,'w')

    def endLog(self):
        self.logFile.close()

    def writeError(self):
        traceback.print_exc(file=self.logFile)
        self.logFile.write('\n')
        self.logFile.flush()
        os.fsync(self.logFile)

    def writeMessage(self, message=''):
        self.logFile.write(message)
        self.logFile.write('\n\n')
        self.logFile.flush()
        os.fsync(self.logFile)
