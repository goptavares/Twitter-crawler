import twitter
import databaseAccess_unrestricted
import rateLimiter_unrestricted
import errorReport_unrestricted

QUIET=0
STDOUTPUT=1
VERBOSE=2
DEBUG=3


def main(seeduser='gabioptavares',recentusers=1000,**kwargs):

    print 'Main arguments: ', kwargs

    verbosity = kwargs.get('verbosity')

    # Create log file
    log = errorReport_unrestricted.getErrorReport()
    log.startLog()

    # Connect to database and create tables
    dbAccess = databaseAccess_unrestricted.getDatabaseAccess()
    dbargs = dict([ (k,kwargs[k]) for k in ['host','db','user','passwd'] ])
    dbAccess.makeConnection(**dbargs)
    dbAccess.createTables()

    rlapi = rateLimiter_unrestricted.getRateLimiter(**kwargs)

    profilefields = ['uid',
                     'name',
                     'screen_name',
                     'location',
                     'protected',
                     'utc_offset',
                     'time_zone',
                     'statuses_count',
                     'followers_count',
                     'friends_count',
                     'geo_enabled',
                     'lang',
                     'created_at']

    # Initialize the queue
    profile = rlapi.freqLimitGetUser(seeduser)
    startid = profile.id
    samelanguage = profile.lang
    queue = set([])
    queue.add(startid)
    recent = []

    while len(queue) > 0 :
        queue = queue - set(recent)
        if len(recent) > recentusers:
            recent = recent[-recentusers:]
        newuserids = [ queue.pop() for _ in range(min(100,len(queue))) ]
        if verbosity >= DEBUG:
            print '[DEBUG] newuserids:', newuserids

        # Get all new user profiles in a single call
        try:
            profiles = rlapi.freqLimitUsersLookup(newuserids)
            for profile in profiles:
                if verbosity >= VERBOSE:
                    print 'Processing profile ', str(profile.screen_name), ', ', str(profile.id)
                    log.writeMessage('Processing profile ' + str(profile.screen_name) + ', ' + str(profile.id))
                if profile.lang == samelanguage and profile.followers_count < 100000:
                    try:
                    # Try to insert into profiles table
                        dbAccess.profileToSQL(profile, profilefields, **kwargs)
                    except databaseAccess_unrestricted.ConnectionClosed, e:
                        log.writeError()
                        print e.message
                        log.writeMessage(e.message)
                        dbAccess.makeConnection(**dbargs)
                        print 'Connection reestablished.'
                        log.writeMessage('Connection reestablished.')
                    except databaseAccess_unrestricted.CursorClosed, e:
                        log.writeError()
                        print e.message
                        log.writeMessage(e.message)
                        dbAccess.getCursor()
                        print 'Cursor updated.'
                        log.writeMessage('Cursor updated.')
                    except Exception, e:
                        log.writeError()
                        print 'Database exception: ', e
                        log.writeMessage('Database exception: ' + e.message)
                        # Note that insert failed (probably because entry already exists)
                        newuserids.remove(profile.id)
                else:
                    # Note that language is not the same as the seed user or user has too many followers
                    newuserids.remove(profile.id)
                    print 'User ' + str(profile.id) + ' ignored due to language or large number of followers.'
                    log.writeMessage('User ' + str(profile.id) + ' ignored due to language or large number of followers.')
        except Exception, e:
             log.writeError()
             print 'Twitter API exception: ', e
             log.writeMessage('Twitter API exception: ' + e.message)
        
        # Now get followers, friends and tweets for each user
        if verbosity >= DEBUG:
            print '[DEBUG] newuserids:', newuserids
        for usrid in newuserids:
            log.writeMessage('Fully processing id: ' + str(usrid))
            if verbosity >= VERBOSE:
                print 'Fully processing id: ', usrid

            cursor = -1
            while cursor != 0:
                res = []
                try:
                    res = rlapi.freqLimitGetFollowerIDs(usrid, cursor)
                    folids = res[u'ids']
                    cursor = res[u'next_cursor']

                    # Write followers to the database
                    for folid in folids:
                        try:
                            dbAccess.followToSQL(folid, usrid)
                            queue.add(folid)
                        except databaseAccess_unrestricted.ConnectionClosed, e:
                            log.writeError()
                            print e.message
                            log.writeMessage(e.message)
                            dbAccess.makeConnection(**dbargs)
                            print 'Connection reestablished.'
                            log.writeMessage('Connection reestablished.')
                        except databaseAccess_unrestricted.CursorClosed, e:
                            log.writeError()
                            print e.message
                            log.writeMessage(e.message)
                            dbAccess.getCursor()
                            print 'Cursor updated.'
                            log.writeMessage('Cursor updated.')
                        except Exception, e:
                            log.writeError()
                            print 'Database exception: ', e
                            log.writeMessage('Database exception: ' + e.message)
                except Exception, e:
                    log.writeError()
                    print 'Twitter API exception: ', e
                    print 'id: ', usrid
                    log.writeMessage('Twitter API exception: ' + e.message + '\nid: ' + str(usrid))
                    cursor = 0
                
            cursor = -1
            while cursor != 0:
                res = []
                try:
                    res = rlapi.freqLimitGetFriendIDs(usrid,cursor)
                    friendids = res['ids']
                    cursor = res['next_cursor']

                    # Write friends to the database     
                    for friendid in friendids:
                        try:
                            dbAccess.followToSQL(usrid, friendid)
                            queue.add(friendid)
                        except databaseAccess_unrestricted.ConnectionClosed, e:
                            log.writeError()
                            print e.message
                            log.writeMessage(e.message)
                            dbAccess.makeConnection(**dbargs)
                            print 'Connection reestablished.'
                            log.writeMessage('Connection reestablished.')
                        except databaseAccess_unrestricted.CursorClosed, e:
                            log.writeError()
                            print e.message
                            log.writeMessage(e.message)
                            dbAccess.getCursor()
                            print 'Cursor updated.'
                            log.writeMessage('Cursor updated.')
                        except Exception, e:
                            log.writeError()
                            print 'Database exception: ', e
                            log.writeMessage('Database exception: ' + e.message)
                except Exception, e:
                    log.writeError()
                    print 'Twitter API exception: ', e
                    print 'id: ', usrid
                    log.writeMessage('Twitter API exception: ' + e.message + '\nid: ' + str(usrid))
                    cursor = 0
                
            # Get timeline
            log.writeMessage('Getting user timeline')
            if verbosity >= VERBOSE:
                print 'Getting user timeline'
            timeline = []
            try:
                timeline = rlapi.freqLimitGetUserTimeline(user_id=usrid, count=200)

                # Write tweets to the database
                for status in timeline:
                    try:
                        dbAccess.tweetToSQL(status, **kwargs)
                    except databaseAccess_unrestricted.ConnectionClosed, e:
                        log.writeError()
                        print e.message
                        log.writeMessage(e.message)
                        dbAccess.makeConnection(**dbargs)
                        print 'Connection reestablished.'
                        log.writeMessage('Connection reestablished.')
                    except databaseAccess_unrestricted.CursorClosed, e:
                        log.writeError()
                        print e.message
                        log.writeMessage(e.message)
                        dbAccess.getCursor()
                        print 'Cursor updated.'
                        log.writeMessage('Cursor updated.')
                    except Exception, e:
                        log.writeError()
                        print 'Database exception: ', e
                        log.writeMessage('Database exception: ' + e.message)
                        log.writeMessage('Tweet: ' + status.text)
               except Exception, e:
                    log.writeError()
                    print 'Twitter API exception: ', e
                    print 'id: ', usrid
                    log.writeMessage('Twitter API exception: ' + e.message + '\nid: ' + str(usrid))
               
            # Finally, mark the user as recent
            recent.append(usrid)
            log.writeMessage('Appended user to the recent list.')
            if verbosity >= VERBOSE:
                print 'Appended user to the recent list. Now is: ', recent

    # Close database connection
    dbAccess.closeConnection()

    # End log file
    log.endLog()



if __name__ =='__main__':

    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option('-v',"--verbosity",
                    type=int,
                    default=VERBOSE,
                    help="Set the verbosity")
    parser.add_option("--host",
                    type=str,
                    default='localhost',
                    help="Host machine for database access")
    parser.add_option("--db",
                    type=str,
                    default='XXXXX',
                    help="Database name")
    parser.add_option("--user",
                    type=str,
                    default='XXXXX',
                    help="User name for database access")
    parser.add_option("--passwd",
                    type=str,
                    default='XXXXX',
                    help="Password for database access")
    

    (options, args) = parser.parse_args()
    kwargs = dict([[k,v] for k,v in options.__dict__.iteritems() if not v is None])
    main(*args,**kwargs)
