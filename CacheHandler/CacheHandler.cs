using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.Caching;
using System.Threading;
using System.Threading.Tasks;

namespace Livingstone.Library
{
    //timestamped data used in the memorycache, Use the timestamp to control expiracy
    class MemoryCacheTimedItem
    {
        public DateTime expiry { get; set; }
        public object data { get; set; }

        //default: ts = now
        public MemoryCacheTimedItem(object data, int expireSec)
        {
            this.data = data;
            expiry = DateTime.Now.AddSeconds(expireSec);
        }
    }

    public static class CacheHandler
    {
        static ConcurrentDictionary<string, Task> keyTasks = new ConcurrentDictionary<string, Task>();
        static ConcurrentDictionary<string, CancellationTokenSource> keyCancelToken = new ConcurrentDictionary<string, CancellationTokenSource>();
        static ConcurrentDictionary<string, ConcurrentBag<Exception>> errors = new ConcurrentDictionary<string, ConcurrentBag<Exception>>();

        private static void recordError(string key, Exception e)
        {
            if (e == null)
                return;
            if (!errors.ContainsKey(key))
                errors[key] = new ConcurrentBag<Exception>();
            errors[key].Add(e);
        }

        public static void resetMemCache(ConcurrentDictionary<string, Func<object>> memKeyEntries)
        {
            if (MemoryCache.Default != null)
                foreach (var keyFuncSet in memKeyEntries)
                    if (MemoryCache.Default.Contains(keyFuncSet.Key) && MemoryCache.Default[keyFuncSet.Key] != null)
                    {
                        if (keyTasks.ContainsKey(keyFuncSet.Key) && !keyTasks[keyFuncSet.Key].IsCompleted)
                            keyCancelToken[keyFuncSet.Key].Cancel();

                        CancellationTokenSource ts = new CancellationTokenSource();
                        CancellationToken ct = ts.Token;
                        keyCancelToken[keyFuncSet.Key] = ts;
                        keyTasks[keyFuncSet.Key] = Task.Factory.StartNew(() =>
                        {
                            try
                            {
                                object data = keyFuncSet.Value();
                                if (!ct.IsCancellationRequested &&
                                MemoryCache.Default.Contains(keyFuncSet.Key) && MemoryCache.Default[keyFuncSet.Key] != null)
                                    (MemoryCache.Default[keyFuncSet.Key] as MemoryCacheTimedItem).data = data;
                            }
                            catch (Exception e)
                            {
                                recordError(keyFuncSet.Key, e);
                            }
                        });
                    }
                    else
                    {
                        //clean up expired keys
                        keyTasks.TryRemove(keyFuncSet.Key, out var dispose1);
                        if (dispose1 != null)
                            dispose1.Dispose();
                        keyCancelToken.TryRemove(keyFuncSet.Key, out var dispose2);
                        if (dispose2 != null)
                            dispose2.Dispose();
                        if (MemoryCache.Default.Contains(keyFuncSet.Key))
                            MemoryCache.Default.Remove(keyFuncSet.Key);
                        if (errors.ContainsKey(keyFuncSet.Key))
                            errors.TryRemove(keyFuncSet.Key, out var dispose);
                    }
        }

        //key: a unique key as the cache entry
        //data: the data to be stored
        //expirySec: expiry time in seconds
        //throttle: if not throttle, the update of cache will be forced before fetching new data beyond expiry time
        public static void buildCache(string key, object data, int expirySec = 3600, bool throttle = true)
        {
            if (MemoryCache.Default != null)
            {
                if (expirySec == 0)
                    expirySec = 432000;     //a week
                //noForce: do not force updates before cache expires
                if (!throttle || !MemoryCache.Default.Contains(key) || MemoryCache.Default[key] == null ||
                     (MemoryCache.Default[key] as MemoryCacheTimedItem).expiry > DateTime.Now
                    )
                {
                    MemoryCacheTimedItem newEntry = new MemoryCacheTimedItem(data, expirySec);
                    MemoryCache.Default.Set(key, newEntry,
                        new CacheItemPolicy() { SlidingExpiration = TimeSpan.FromSeconds(expirySec) }
                        );
                }
            }
        }

        //key: a unique key as the cache entry
        //data: a delegate to obtain the data in case of expiry
        //intervalSec: interval before next update in seconds
        //expirySec: sliding expiry time before the cache will be wiped out
        public static object readCache(string key, Func<object> getData, int intervalSec = 3600, int expirySec = 7200)
        {
            if (MemoryCache.Default == null)
                return null;
            //data within effective time
            if (MemoryCache.Default != null && MemoryCache.Default.Contains(key))
            {
                var dataInCache = MemoryCache.Default[key] as MemoryCacheTimedItem;
                if (dataInCache != null && dataInCache.expiry <= DateTime.Now)
                    return dataInCache.data;
            }
            //else: expired or not existing, build cache for later use
            object data = null;
            if (keyTasks.ContainsKey(key) && !keyTasks[key].IsCompleted)
            {
                keyCancelToken[key].Cancel();
                errors.TryRemove(key, out var dispose);
            }

            CancellationTokenSource ts = new CancellationTokenSource();
            CancellationToken ct = ts.Token;
            keyCancelToken[key] = ts;
            keyTasks[key] = Task.Run(() =>
            {
                try
                {
                    data = getData();
                    if (!ct.IsCancellationRequested)
                        buildCache(key, data, expirySec, false);
                }
                catch (Exception e)
                {
                    recordError(key, e);
                }
            });
            keyTasks[key].Wait();

            //release memory
            if (keyTasks[key] != null && keyTasks[key].IsCompleted)
            {
                keyTasks.TryRemove(key, out var tempTsk);
                tempTsk.Dispose();
                keyCancelToken.TryRemove(key, out var tempCt);
                if (tempCt != null)
                    tempCt.Dispose();
            }

            if (errors.ContainsKey(key))
            {
                var aggEx = new AggregateException(errors[key]);
                errors.TryRemove(key, out var dispose);
                throw aggEx.Flatten();
            }
            return data;
        }

        //key: a unique key as the cache entry
        //data: a delegate to obtain the data in case of expiry
        //intervalSec: interval before next update in seconds
        //expirySec: sliding expiry time before the cache will be wiped out
        public static object readCacheBackground(string key, Func<object> getData, int intervalSec = 3600, int expirySec = 7200)
        {
            if (MemoryCache.Default == null)
                return null;
            if (expirySec == 0)
                expirySec = 432000;     //a week
            //background update for next use if data expires
            if (
                ((!MemoryCache.Default.Contains(key) || MemoryCache.Default[key] == null)
                    || (MemoryCache.Default[key] as MemoryCacheTimedItem).expiry > DateTime.Now)
                && (!keyTasks.ContainsKey(key) || keyTasks[key].IsCompleted)
                )
            {
                CancellationTokenSource ts = new CancellationTokenSource();
                CancellationToken ct = ts.Token;
                keyCancelToken[key] = ts;
                keyTasks[key] = Task.Factory.StartNew(() =>
                {
                    try
                    {
                        object newData = getData();
                        if (!ct.IsCancellationRequested)
                            buildCache(key, newData, intervalSec, false);
                    }
                    catch (Exception e)
                    {
                        recordError(key, e);
                    }
                });
            }
            //if data does not exist, building cache becomes the only option
            if (!MemoryCache.Default.Contains(key) || MemoryCache.Default[key] == null)
                keyTasks[key].Wait();

            //release memory
            if (keyTasks[key] != null && keyTasks[key].IsCompleted)
            {
                keyTasks.TryRemove(key, out var tempTsk);
                tempTsk.Dispose();
                keyCancelToken.TryRemove(key, out var tempCt);
                if (tempCt != null)
                    tempCt.Dispose();
            }

            if (errors.ContainsKey(key))
            {
                var aggEx = new AggregateException(errors[key]);
                errors.TryRemove(key, out var dispose);
                throw aggEx.Flatten();
            }
            return (MemoryCache.Default[key] as MemoryCacheTimedItem).data;
        }
    }
}