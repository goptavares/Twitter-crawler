import psycopg2
from datetime import datetime

DATETIME_STRING_FORMAT = '%a %b %d %H:%M:%S +0000 %Y'
QUIET=0
STDOUTPUT=1
VERBOSE=2
DEBUG=3

profiletable = 'profiles'
socialgraphtable = 'socialgraph'
tweettable = 'tweets'


class ConnectionClosed(Exception):
    def __init__(self):
        self.message = 'ConnectionClosed Exception!'
        return


class CursorClosed(Exception):
    def __init__(self):
        self.message = 'CursorClosed Exception!'
        return


def getDatabaseAccess():
    dbAccess = DatabaseAccess()
    return dbAccess


class DatabaseAccess():        

    def __init__(self):
        self.dbconn = None
        self.dbcursor = None


    def makeConnection(self, **kwargs):
        host = kwargs.get('host')
        database = kwargs.get('db')
        user = kwargs.get('user')
        password = kwargs.get('passwd')
        self.dbconn = psycopg2.connect(database=database,user=user,password=password)
        self.getCursor()


    def getCursor(self):
        self.dbcursor = self.dbconn.cursor()


    def closeConnection(self):
        self.dbconn.close()
        

    def createTables(self):

        # Create table 'profiles' if it doesn't already exist
        tableCheck = self.dbcursor.execute('SELECT count(table_name)::int FROM information_schema.tables WHERE table_name = \'' + profiletable + '\'')
        tableCheck = self.dbcursor.fetchone() 
        count = tableCheck[0]
        if count == 0:
            try:
                self.dbcursor.execute('CREATE TABLE ' \
                                     + profiletable + '(' \
                                     + 'uid BIGINT PRIMARY KEY,' \
                                     + 'name VARCHAR(40),' \
                                     + 'screen_name VARCHAR(40),' \
                                     + 'location VARCHAR(40),' \
                                     + 'protected BOOL,' \
                                     + 'utc_offset INTEGER,' \
                                     + 'time_zone VARCHAR(40),' \
                                     + 'statuses_count INTEGER,' \
                                     + 'followers_count INTEGER,' \
                                     + 'friends_count INTEGER,' \
                                     + 'geo_enabled BOOL,' \
                                     + 'lang VARCHAR(2),' \
                                     + 'created_at TIMESTAMP)')
            except Exception, e:
                print 'DatabaseAccess exception: ', e
                print 'Connection status: ', self.dbconn.closed 
                print 'Cursor status: ', self.dbcursor.closed
                if self.dbconn.closed:
                    raise ConnectionClosed()
                elif self.dbcursor.closed:
                    raise CursorClosed()
                else:
                    raise e

        # Create table 'tweets' if it doesn't already exist
        tableCheck = self.dbcursor.execute('SELECT count(table_name)::int FROM information_schema.tables WHERE table_name = \'' + tweettable + '\'')
        tableCheck = self.dbcursor.fetchone() 
        count = tableCheck[0]
        if count == 0:
            try:
                self.dbcursor.execute('CREATE TABLE ' + tweettable + '(' \
                                     + 'tid BIGINT PRIMARY KEY,' \
                                     + 'uid BIGINT NOT NULL,' \
                                     + 'text VARCHAR(160),' \
                                     + 'created_at TIMESTAMP,' \
                                     + 'truncated BOOL,' \
                                     + 'retweeted BOOL)')
            except Exception, e:
                print 'DatabaseAccess exception: ', e
                print 'Connection status: ', self.dbconn.closed 
                print 'Cursor status: ', self.dbcursor.closed 
                if self.dbconn.closed:
                    raise ConnectionClosed()
                elif self.dbcursor.closed:
                    raise CursorClosed()
                else:
                    raise e

        # Create table 'socialgraph' if it doesn't already exist
        checkTable = self.dbcursor.execute('SELECT count(table_name)::int FROM information_schema.tables WHERE table_name = \'' + socialgraphtable + '\'')
        checkTable = self.dbcursor.fetchone() 
        count = checkTable[0]
        if count == 0:
            try:
                self.dbcursor.execute('CREATE TABLE ' + socialgraphtable + '(' \
                                     + 'parent BIGINT NOT NULL,' \
                                     + 'child BIGINT NOT NULL,' \
                                     + 'UNIQUE (parent, child))')
            except Exception, e:
                print 'DatabaseAccess exception: ', e
                print 'Connection status: ', self.dbconn.closed
                print 'Cursor status: ', self.dbcursor.closed 
                if self.dbconn.closed:
                    raise ConnectionClosed()
                elif self.dbcursor.closed:
                    raise CursorClosed()
                else:
                    raise e

        # Commit changes to the database
        try:
            self.dbconn.commit()
        except Exception, e:
            print 'DatabaseAccess exception: ', e
            print 'Connection status: ', self.dbconn.closed 
            print 'Cursor status: ', self.dbcursor.closed
            if self.dbconn.closed:
                raise ConnectionClosed()
            elif self.dbcursor.closed:
                self.dbconn.rollback()
                raise CursorClosed()
            else:
                self.dbconn.rollback()
                raise e


    def tableInsert(self, tablename=None, entrydict=None, **kwargs):
        verbosity = kwargs.get('verbosity')
        fields = ""
        valuetemplate = ""
        values = []

        for k,v in entrydict.iteritems():
            fields += str(k) + ","
            valuetemplate += " %s,"
            values.append(v)
        fields = fields[:-1]
        valuetemplate = valuetemplate[:-1]
        values = tuple(values)

        query = "INSERT INTO "
        query += tablename + " (" + fields + ") VALUES(" + valuetemplate + ")"

        if verbosity >= DEBUG:
            print "[debug] query: ", query
            print "[debug] values: ", values
        try:
            self.dbcursor.execute(query,values)
        except Exception, e:
            print 'DatabaseAccess exception: failed to insert into table ', tablename, ' with message: ', e
            print 'Connection status: ', self.dbconn.closed
            print 'Cursor status: ', self.dbcursor.closed 
            if self.dbconn.closed:
                raise ConnectionClosed()
            elif self.dbcursor.closed:
                raise CursorClosed()
            else:
                raise e
        finally:
            try:
                self.dbconn.commit()
            except Exception, e:
                print 'DatabaseAccess exception: failed to commit changes to table ', tablename, ' with message:', e
                print 'Connection status: ', self.dbconn.closed 
                print 'Cursor status: ', self.dbcursor.closed
                if self.dbconn.closed:
                    raise ConnectionClosed()
                elif self.dbcursor.closed:
                    self.dbconn.rollback()
                    raise CursorClosed()
                else:
                    self.dbconn.rollback()
                    raise e
            

    def profileToSQL(self, profile, fields, **kwargs):
        dprofile = profile.AsDict()
        dprofile['uid'] = dprofile.pop('id')
        entrydict = dict([ (key,dprofile[key]) for key in fields if key in dprofile ])
        try:
            self.tableInsert(tablename=profiletable, entrydict=entrydict, **kwargs)
        except Exception, e:
            raise


    def tweetToSQL(self, status, **kwargs):
        created_at = datetime.strptime(status.created_at, DATETIME_STRING_FORMAT)
        entrydict = dict(uid=status.user.id,
                         tid=status.id,
                         text=status.text,
                         created_at=created_at,
                         truncated=status.truncated,
                         retweeted=status.retweeted)
        try:
            self.tableInsert(tablename=tweettable, entrydict=entrydict, **kwargs)
        except Exception, e:
            raise


    def followToSQL(self, follower, followed, followerfield='parent', followedfield='child', **kwargs):
        entrydict = dict( [(followerfield,follower), (followedfield,followed)])
        try:
            self.tableInsert(tablename=socialgraphtable, entrydict=entrydict)
        except Exception, e:
            raise



