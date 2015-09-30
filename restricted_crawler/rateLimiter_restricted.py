import time
import twitter

QUIET=0
STDOUTPUT=1
VERBOSE=2
DEBUG=3


class SuppressedCallException(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self ):
        return repr(self.value)

class RateLimiter(object):

    readmethods = [ 'FilterPublicTimeline',
                    'GetUser',
                    'GetDirectMessages',
                    'GetFavorites',
                    'GetFeatured',
                    'GetFollowerIDs',
                    'GetFollowers',
                    'GetFriendIDs',
                    'GetFriends',
                    'GetFriendsTimeline',
                    'GetLists',
                    'GetMentions',
                    'GetPublicTimeline',
                    'GetReplies',
                    'GetRetweets',
                    'GetSearch',
                    'GetStatus',
                    'GetSubscriptions',
                    'GetTrendsCurrent',
                    'GetTrendsDaily',
                    'GetTrendsWeekly',
                    'GetUser',
                    'GetUserByEmail',
                    'GetUserRetweets',
                    'GetUserTimeline',
                    'MaximumHitFrequency',
                    'UsersLookup'
                    ]

    writemethods = []

    # Initializing rate limiting variable 
    def __init__(self, api, verbosity):
        self.max_calls = 0  # number of allowed calls in one hour : the quota
        self.calls_left = 0  # what is left in the quota
        self.t_end = 0 # end time of an one-hour period
        self.api = api
        for methodname in self.readmethods+self.writemethods:
            self.WrapMethod(methodname)
            self.WaitToMethod(methodname)
            self.FreqLimitMethod(methodname)
        self.GetTwitterRateLimit(api=self.api)
        self.timefrom = None
        self.verbosity = verbosity


    def GetTwitterRateLimit(self, api=None):
        # Refresh the number of calls left, max calls and refresh time.
        if api is None:
            api = self.api
        rl = api.GetRateLimitStatus()
        self.calls_left = rl['remaining_hits']
        self.max_calls = rl['hourly_limit']
        self.t_end = rl['reset_time_in_seconds']


    def freqLimitMethod(self, methodname):
        # Waits the appropriate length before calling.
        # Twitter api may throttle us if we try calling repeatedly but within our limit.
        method = getattr(self.api,methodname)
        def freqlimitmethod(*args,**kwargs ):
            if self.verbosity >= DEBUG:
                print "[DEBUG] In FreqLimit"+methodname
            if self.timefrom == None:
                self.timefrom = time.time()
            mhf = self.MaximumHitFrequency()
            if mhf == None:
                mhf = 30
            waittill = self.timefrom + mhf
            now = time.time()
            if now > waittill:
                pass
            else:
                if self.verbosity >= STDOUTPUT:
                    print "Sleeping for "+ str(waittill-now)+ " seconds"
                time.sleep(waittill-now)
                if self.verbosity >= STDOUTPUT:
                    print "Waking up"
            res = method(*args,**kwargs)
            self.timefrom = time.time()
            return res
        setattr(self, 'freqLimit'+methodname, freqlimitmethod)


def getRateLimiter(consumer_key = 'XXXXX',
                   consumer_secret = 'XXXXX',
                   access_token_key = 'XXXXX',
                   access_token_secret = 'XXXXX',
                   **kwargs):

    api = twitter.Api(consumer_key=consumer_key,
                      consumer_secret=consumer_secret,
                      access_token_key=access_token_key,
                      access_token_secret=access_token_secret,
                      cache=None)

    rlapi = RateLimiter(api,verbosity=kwargs.get('verbosity'))
    return rlapi

