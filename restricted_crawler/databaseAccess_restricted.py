import psycopg2
from datetime import datetime, date

DATETIME_STRING_FORMAT = '%a %b %d %H:%M:%S +0000 %Y'
QUIET=0
STDOUTPUT=1
VERBOSE=2
DEBUG=3

profiletable = 'profiles'
followertable = 'followers'
friendtable = 'friends'
tweettable = 'tweets'
retweettable = 'retweets'
favoritetable = 'favorites'


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
                                     + 'favourites_count INTEGER,' \
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

        # Create table 'followers' if it doesn't already exist
        checkTable = self.dbcursor.execute('SELECT count(table_name)::int FROM information_schema.tables WHERE table_name = \'' + followertable + '\'')
        checkTable = self.dbcursor.fetchone() 
        count = checkTable[0]
        if count == 0:
            try:
                self.dbcursor.execute('CREATE TABLE ' + followertable + '(' \
                                     + 'uid BIGINT NOT NULL,' \
                                     + 'follower_id BIGINT NOT NULL,' \
                                     + 'UNIQUE (uid, follower_id))')
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

        # Create table 'friends' if it doesn't already exist
        checkTable = self.dbcursor.execute('SELECT count(table_name)::int FROM information_schema.tables WHERE table_name = \'' + friendtable + '\'')
        checkTable = self.dbcursor.fetchone() 
        count = checkTable[0]
        if count == 0:
            try:
                self.dbcursor.execute('CREATE TABLE ' + friendtable + '(' \
                                     + 'uid BIGINT NOT NULL,' \
                                     + 'friend_id BIGINT NOT NULL,' \
                                     + 'UNIQUE (uid, friend_id))')
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
                                     + 'uid BIGINT NOT NULL,' \
                                     + 'tid BIGINT PRIMARY KEY,' \
                                     + 'text VARCHAR(160),' \
                                     + 'created_at TIMESTAMP,' \
                                     + 'is_reply BOOL,' \
                                     + 'in_reply_to_user_id BIGINT,' \
                                     + 'in_reply_to_status_id BIGINT,' \
                                     + 'is_mention BOOL,' \
                                     + 'mentions VARCHAR(40)[],' \
                                     + 'mention_ids BIGINT[],' \
                                     + 'is_hashtag BOOL,' \
                                     + 'hashtags VARCHAR(100)[],' \
                                     + 'is_media BOOL,' \
                                     + 'is_url BOOL,' \
                                     + 'retweet_count INTEGER,' \
                                     + 'retweeter_ids BIGINT[])')

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

        # Create table 'retweets' if it doesn't already exist
        tableCheck = self.dbcursor.execute('SELECT count(table_name)::int FROM information_schema.tables WHERE table_name = \'' + retweettable + '\'')
        tableCheck = self.dbcursor.fetchone() 
        count = tableCheck[0]
        if count == 0:
            try:
                self.dbcursor.execute('CREATE TABLE ' + retweettable + '(' \
                                     + 'uid BIGINT NOT NULL,' \
                                     + 'retweeted_user VARCHAR(30),' \
                                     + 'tid BIGINT NOT NULL,' \
                                     + 'text VARCHAR(160),' \
                                     + 'created_at TIMESTAMP,' \
                                     + 'UNIQUE (uid, tid))')
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

        # Create table 'favorites' if it doesn't already exist
        tableCheck = self.dbcursor.execute('SELECT count(table_name)::int FROM information_schema.tables WHERE table_name = \'' + favoritetable + '\'')
        tableCheck = self.dbcursor.fetchone() 
        count = tableCheck[0]
        if count == 0:
            try:
                self.dbcursor.execute('CREATE TABLE ' + favoritetable + '(' \
                                     + 'uid BIGINT NOT NULL,' \
                                     + 'tid BIGINT NOT NULL,' \
                                     + 'favorited_user_id BIGINT NOT NULL,' \
                                     + 'text VARCHAR(160),' \
                                     + 'created_at TIMESTAMP,' \
                                     + 'UNIQUE (uid, tid))')
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


    def followerToSQL(self, usrid, follower_id, **kwargs):
        entrydict = dict(uid=usrid,
                         follower_id=follower_id)
        try:
            self.tableInsert(tablename=followertable, entrydict=entrydict, **kwargs)
        except Exception, e:
            raise


    def friendToSQL(self, usrid, friend_id, **kwargs):
        entrydict = dict(uid=usrid,
                         friend_id=friend_id)
        try:
            self.tableInsert(tablename=friendtable, entrydict=entrydict, **kwargs)
        except Exception, e:
            raise


    def tweetToSQL(self, status, retweet_count, retweeter_ids, is_reply, is_mention, mentions, mention_ids, is_hashtag, hashtags, is_media, is_url, **kwargs):
        entrydict = dict(uid=status.user.id,
                         tid=status.id,
                         text=status.text,
                         created_at=status.created_at,
                         is_reply=is_reply,
                         in_reply_to_user_id=status.in_reply_to_user_id,
                         in_reply_to_status_id=status.in_reply_to_status_id,
                         is_mention=is_mention,
                         mentions=mentions,
                         mention_ids=mention_ids,
                         is_hashtag=is_hashtag,
                         hashtags=hashtags,
                         is_media=is_media,
                         is_url=is_url,
                         retweet_count=retweet_count,
                         retweeter_ids=retweeter_ids)
        try:
            self.tableInsert(tablename=tweettable, entrydict=entrydict, **kwargs)
        except Exception, e:
            raise


    def retweetToSQL(self, status, retweeted_user, **kwargs):
        created_at = datetime.strptime(status.created_at, DATETIME_STRING_FORMAT)
        entrydict = dict(uid=status.user.id,
                         retweeted_user=retweeted_user,
                         tid=status.id,
                         text=status.text,
                         created_at=created_at)
        try:
            self.tableInsert(tablename=retweettable, entrydict=entrydict, **kwargs)
        except Exception, e:
            raise


    def favoriteToSQL(self, usrid, status, **kwargs):
        created_at = datetime.strptime(status.created_at, DATETIME_STRING_FORMAT)
        entrydict = dict(uid=usrid,
                         tid=status.id,
                         favorited_user_id=status.user.id,
                         text=status.text,
                         created_at=created_at)
        try:
            self.tableInsert(tablename=favoritetable, entrydict=entrydict, **kwargs)
        except Exception, e:
            raise


