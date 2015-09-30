import twitter
import rateLimiter_restricted
import errorReport_restricted
import databaseAccess_restricted
from pyparsing import Word, alphas, alphanums, CaselessLiteral, empty, printables, Keyword, CaselessKeyword

QUIET=0
STDOUTPUT=1
VERBOSE=2
DEBUG=3


def main(**kwargs):

    print 'Main arguments: ', kwargs

    verbosity = kwargs.get('verbosity')

    # Create log file
    log = errorReport_restricted.getErrorReport()
    log.startLog()

    # Grammar for parsing retweet
    retweeted = Word( alphanums + "_" + "-" )
    grammar = Keyword("RT")  + "@" +  retweeted.setResultsName("name") + ":"

    # Connect to database and create tables
    dbAccess = databaseAccess_restricted.getDatabaseAccess()
    dbargs = dict([ (k,kwargs[k]) for k in ['host','db','user','passwd'] ])
    dbAccess.makeConnection(**dbargs)
    dbAccess.createTables()

    rlapi = rateLimiter_restricted.getRateLimiter(**kwargs)

    # List of fields in the profile table
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
                     'favourites_count',
                     'geo_enabled',
                     'lang',
                     'created_at']

    # List of users we want to track
    users = ['gabioptavares']

    for user in users:

        log.writeMessage('Started user: ' + user)

        # Get user profile
        try:
            profile = rlapi.freqLimitGetUser(user)
            usrid = profile.id
        except Exception, e:
             log.writeError()
             print 'Twitter API exception: ', e
             log.writeMessage('Twitter API exception: ' + e.message)
             continue

        # Insert profile into profile table
        try:
            dbAccess.profileToSQL(profile, profilefields, **kwargs)
        except databaseAccess_restricted.ConnectionClosed, e:
            log.writeError()
            print e.message
            log.writeMessage(e.message)
            dbAccess.makeConnection(**dbargs)
            print 'Connection reestablished.'
            log.writeMessage('Connection reestablished.')
        except databaseAccess_restricted.CursorClosed, e:
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

        # Get user's followers
        cursor = -1
        while cursor != 0:
            res = []
            try:
                res = rlapi.freqLimitGetFollowerIDs(usrid, cursor)
                folids = res[u'ids']
                cursor = res[u'next_cursor']

                # Insert followers into follower table
                for folid in folids:
                    try:
                        dbAccess.followerToSQL(usrid, folid)
                    except databaseAccess_restricted.ConnectionClosed, e:
                        log.writeError()
                        print e.message
                        log.writeMessage(e.message)
                        dbAccess.makeConnection(**dbargs)
                        print 'Connection reestablished.'
                        log.writeMessage('Connection reestablished.')
                    except databaseAccess_restricted.CursorClosed, e:
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

        # Get user's friends
        cursor = -1
        while cursor != 0:
            res = []
            try:
                res = rlapi.freqLimitGetFriendIDs(usrid, cursor)
                friendids = res[u'ids']
                cursor = res[u'next_cursor']

                # Insert friends into friend table
                for friendid in friendids:
                    try:
                        dbAccess.friendToSQL(usrid, friendid)
                    except databaseAccess_restricted.ConnectionClosed, e:
                        log.writeError()
                        print e.message
                        log.writeMessage(e.message)
                        dbAccess.makeConnection(**dbargs)
                        print 'Connection reestablished.'
                        log.writeMessage('Connection reestablished.')
                    except databaseAccess_restricted.CursorClosed, e:
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

        # Get user's timeline
        timeline = []

        tl = rlapi.freqLimitGetUserTimeline(user_id=usrid, count=200, include_rts=True, include_entities=True)
        while (tl != None and tl != [] and len(timeline) <= 600):
            log.writeMessage('TIMELINE LENGTH: '+str(len(timeline)))
            timeline = timeline + tl
            maxId = (tl[-1]).id -1
            try:
                tl = rlapi.freqLimitGetUserTimeline(user_id=usrid, count=200, max_id=maxId, include_rts=True, include_entities=True)
            except Exception, e:
                log.writeError()
                print 'Twitter API exception: ', e
                print 'id: ', usrid
                log.writeMessage('Twitter API exception: ' + e.message + '\nid: ' + str(usrid))

        for status in timeline:

            # Check if it is a retweet
            isRT = False
            res = []
            try:
                res = grammar.parseString(status.text)
            except Exception, e:
                pass
            finally:
                if len(res) > 0:
                    isRT = True
                    retweeted_user = res.name

            # If it is a retweet, insert into retweet table
            if isRT:
                try:
                    dbAccess.retweetToSQL(status, retweeted_user, **kwargs)
                except databaseAccess_restricted.ConnectionClosed, e:
                    log.writeError()
                    print e.message
                    log.writeMessage(e.message)
                    dbAccess.makeConnection(**dbargs)
                    print 'Connection reestablished.'
                    log.writeMessage('Connection reestablished.')
                except databaseAccess_restricted.CursorClosed, e:
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

            # Else, get retweets, check for entities and insert into tweet table
            else:
                # Get retweets
                try:
                    retweets = rlapi.freqLimitGetRetweets(status.id)
                except Exception, e:
                    log.writeError()
                    print 'Twitter API exception: ', e
                    print 'id: ', usrid
                    log.writeMessage('Twitter API exception: ' + e.message + '\nid: ' + str(usrid))
                retweeter_ids = []
                for retweet in retweets:
                    retweeter_ids.append(retweet.user.id)
                retweet_count = len(retweeter_ids)

                # Check for reply and entities (mentions, hashtags, media and urls)
                is_reply = False
                is_mention = False
                mentions = []
                mention_ids = []
                is_hashtag = False
                hashtags = []
                is_media = False
                is_url = False
                if status.in_reply_to_status_id != None:
                    is_reply = True
                if len(status.user_mentions) > 0:
                    is_mention = True    
                    for mention in status.user_mentions:
                        mentions.append(mention.screen_name)
                        mention_ids.append(mention.id)
                if len(status.hashtags) > 0:
                    is_hastag = True
                    for hashtag in status.hashtags:
                        hashtags.append(hashtag.text)
                if status.media != None:
                    is_media = True
                if len(status.urls) > 0:
                    is_url = True

                # Insert into tweet table
                try:
                    dbAccess.tweetToSQL(status, retweet_count, retweeter_ids, is_reply, is_mention, mentions, mention_ids, is_hashtag, hashtags, is_media, is_url, **kwargs)
                except databaseAccess_restricted.ConnectionClosed, e:
                    log.writeError()
                    print e.message
                    log.writeMessage(e.message)
                    dbAccess.makeConnection(**dbargs)
                    print 'Connection reestablished.'
                    log.writeMessage('Connection reestablished.')
                except databaseAccess_restricted.CursorClosed, e:
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

        # Get user's favorites

        try:
            favorites = rlapi.freqLimitGetFavorites(usrid)
        except Exception, e:
            log.writeError()
            print 'Twitter API exception: ', e
            print 'id: ', usrid
            log.writeMessage('Twitter API exception: ' + e.message + '\nid: ' + str(usrid))

        # Insert favorites into favorite table
        for favorite in favorites:
            try:
                dbAccess.favoriteToSQL(usrid, favorite, **kwargs)
            except databaseAccess_restricted.ConnectionClosed, e:
                log.writeError()
                print e.message
                log.writeMessage(e.message)
                dbAccess.makeConnection(**dbargs)
                print 'Connection reestablished.'
                log.writeMessage('Connection reestablished.')
            except databaseAccess_restricted.CursorClosed, e:
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

        log.writeMessage('Finished user: ' + user)


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
