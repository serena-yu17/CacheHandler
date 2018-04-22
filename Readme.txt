This C# library offers convenient management of memory cache. 

class CacheHandler:

void buildCache(string key, object data, int expirySec = 3600, bool throttle = true)

key: a unique memory entry string
data: the data to be stored
expirySec: expiry time in seconds
throttle: if not throttle, the update of cache will be forced before fetching new data beyond expiry time

readCache(string key, Func<object> getData, int intervalSec = 3600, int expirySec = 7200)

Read data from the cache. If the data has expired, it will wait until fresh data is obtained.
key: a unique key as the cache entry
getData: a delegate to obtain the data in case of expiry
intervalSec: interval before next update in seconds
expirySec: sliding expiry time before the cache will be removed

object readCacheBackground(string key, Func<object> getData, int intervalSec = 3600, int expirySec = 7200)

Read data from the cache. If the data has expired, it will not wait, but first supply the old data, and update the data at the background without letting the user to wait.
key: a unique key as the cache entry
data: a delegate to obtain the data in case of expiry
intervalSec: interval before next update in seconds
expirySec: sliding expiry time before the cache will be wiped out

