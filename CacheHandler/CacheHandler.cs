using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.Caching;
using System.Threading;
using System.Threading.Tasks;

namespace Livingstone.Library
{
    public static class CacheHandler
    {
        static MemoryCache memoryCache = new MemoryCache("CacheHandler");

        static ConcurrentDictionary<string, Task<object>> keyTasks = new ConcurrentDictionary<string, Task<object>>();
        static ConcurrentDictionary<string, CancellationTokenSource> keyCancelToken = new ConcurrentDictionary<string, CancellationTokenSource>();
        static ConcurrentDictionary<string, ConcurrentBag<Exception>> errors = new ConcurrentDictionary<string, ConcurrentBag<Exception>>();
        static Dictionary<string, object> locks = new Dictionary<string, object>();

        //timestamped data used in the memorycache, Use the timestamp to control expiracy
        class MemoryCacheTimedItem
        {
            public DateTime validTime { get; set; }
            public object data { get; set; }

            //default: ts = now
            public MemoryCacheTimedItem(object data, int expireSec)
            {
                this.data = data;
                validTime = DateTime.UtcNow.AddSeconds(expireSec);
            }
        }

        private static void recordError(string key, Exception e)
        {
            if (e == null)
                return;
            if (!errors.ContainsKey(key))
                errors[key] = new ConcurrentBag<Exception>();
            errors[key].Add(e);
        }

        public static void removeKey(string key)
        {
            if (keyCancelToken.TryRemove(key, out var ct))
            {
                ct.Cancel();
                ct.Dispose();
            }
            if (keyTasks.TryRemove(key, out var tsk))
                tsk.Dispose();
            errors.TryRemove(key, out var err);
        }

        public static void resetMemCache(IDictionary<string, Func<object>> memKeyEntries)
        {
            Parallel.ForEach(memKeyEntries, (keyFuncSet) =>
           {
               if (!locks.ContainsKey(keyFuncSet.Key))
                   lock (locks)
                       if (!locks.ContainsKey(keyFuncSet.Key))
                           locks[keyFuncSet.Key] = new object();
               if (memoryCache.Contains(keyFuncSet.Key) && memoryCache[keyFuncSet.Key] != null)
                   lock (locks[keyFuncSet.Key])
                   {
                       if (keyTasks.ContainsKey(keyFuncSet.Key) && !keyTasks[keyFuncSet.Key].IsCompleted)
                           keyCancelToken[keyFuncSet.Key].Cancel();
                       if (keyCancelToken[keyFuncSet.Key] != null)
                           keyCancelToken[keyFuncSet.Key].Dispose();
                       CancellationTokenSource ts = new CancellationTokenSource();
                       CancellationToken ct = ts.Token;
                       keyCancelToken[keyFuncSet.Key] = ts;
                       var localkeyFuncSet = keyFuncSet;
                       keyTasks[keyFuncSet.Key] = Task.Factory.StartNew(() =>
                       {
                           object data = null;
                           try
                           {
                               data = localkeyFuncSet.Value();
                               if (!ct.IsCancellationRequested &&
                               memoryCache.Contains(localkeyFuncSet.Key) && memoryCache[localkeyFuncSet.Key] != null)
                                   (memoryCache[localkeyFuncSet.Key] as MemoryCacheTimedItem).data = data;
                               else return null;
                           }
                           catch (Exception e)
                           {
                               recordError(localkeyFuncSet.Key, e);
                           }
                           return data;
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
                   if (memoryCache.Contains(keyFuncSet.Key))
                       memoryCache.Remove(keyFuncSet.Key);
                   if (errors.ContainsKey(keyFuncSet.Key))
                       errors.TryRemove(keyFuncSet.Key, out var dispose);
                   lock (locks)
                       locks.Remove(keyFuncSet.Key);
               }
           });
        }

        public static async Task resetMemCacheAsync(IDictionary<string, Func<object>> memKeyEntries)
        {
            ConcurrentBag<Task> tskList = new ConcurrentBag<Task>();
            Parallel.ForEach(memKeyEntries, (keyFuncSet) =>
            {
                if (!locks.ContainsKey(keyFuncSet.Key))
                    lock (locks)
                        if (!locks.ContainsKey(keyFuncSet.Key))
                            locks[keyFuncSet.Key] = new object();
                lock (locks[keyFuncSet.Key])
                    if (memoryCache.Contains(keyFuncSet.Key) && memoryCache[keyFuncSet.Key] != null)
                    {
                        if (keyTasks.ContainsKey(keyFuncSet.Key) && !keyTasks[keyFuncSet.Key].IsCompleted)
                            keyCancelToken[keyFuncSet.Key].Cancel();
                        if (keyCancelToken[keyFuncSet.Key] != null)
                            keyCancelToken[keyFuncSet.Key].Dispose();
                        CancellationTokenSource ts = new CancellationTokenSource();
                        CancellationToken ct = ts.Token;
                        keyCancelToken[keyFuncSet.Key] = ts;
                        var localkeyFuncSet = keyFuncSet;
                        var newTsk = Task.Run(() =>
                        {
                            object data = null;
                            try
                            {
                                data = localkeyFuncSet.Value();
                                if (!ct.IsCancellationRequested &&
                                memoryCache.Contains(localkeyFuncSet.Key) && memoryCache[localkeyFuncSet.Key] != null)
                                    (memoryCache[localkeyFuncSet.Key] as MemoryCacheTimedItem).data = data;
                                else return null;
                            }
                            catch (Exception e)
                            {
                                recordError(localkeyFuncSet.Key, e);
                            }
                            return data;
                        });
                        keyTasks[keyFuncSet.Key] = newTsk;
                        tskList.Add(newTsk);
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
                        if (memoryCache.Contains(keyFuncSet.Key))
                            memoryCache.Remove(keyFuncSet.Key);
                        if (errors.ContainsKey(keyFuncSet.Key))
                            errors.TryRemove(keyFuncSet.Key, out var dispose);
                    }
            });
            await Task.WhenAll(tskList).ConfigureAwait(false);
        }

        //key: a unique key as the cache entry
        //data: the data to be stored
        //expirySec: expiry time in seconds
        //throttle: if not throttle, the update of cache will be forced before fetching new data beyond expiry time
        public static void buildCache(string key, object data, int intervalSec = 3600, int expirySec = 7200, bool throttle = true)
        {
            if (expirySec == 0)
                expirySec = 432000;     //a week
                                        //noForce: do not force updates before cache expires
            if (!throttle || !memoryCache.Contains(key) || memoryCache[key] == null ||
                 (memoryCache[key] as MemoryCacheTimedItem).validTime < DateTime.UtcNow
                )
            {
                MemoryCacheTimedItem newEntry = new MemoryCacheTimedItem(data, intervalSec);
                memoryCache.Set(key, newEntry,
                    new CacheItemPolicy() { SlidingExpiration = TimeSpan.FromSeconds(expirySec) }
                    );
            }
        }

        public static void buildCache(string key, Func<object> getData, int intervalSec = 3600, int expirySec = 7200, bool throttle = true)
        {
            if (!locks.ContainsKey(key))
                lock (locks)
                    if (!locks.ContainsKey(key))
                        locks[key] = new object();

            if (expirySec == 0)
                expirySec = 432000;     //a week
                                        //noForce: do not force updates before cache expires
            if (!throttle || !memoryCache.Contains(key) || memoryCache[key] == null ||
                 (memoryCache[key] as MemoryCacheTimedItem).validTime < DateTime.UtcNow
                )
            {
                lock (locks[key])
                    if (!keyTasks.ContainsKey(key) || keyTasks[key].IsCompleted)
                    {
                        CancellationTokenSource ts = new CancellationTokenSource();
                        CancellationToken ct = ts.Token;
                        keyCancelToken[key] = ts;
                        keyTasks[key] = Task.Run(() =>
                        {
                            object newData = null;
                            try
                            {
                                newData = getData();
                                if (!ct.IsCancellationRequested)
                                    buildCache(key, newData, intervalSec, expirySec, false);
                                else return null;
                            }
                            catch (Exception e)
                            {
                                recordError(key, e);
                            }
                            return newData;
                        });
                    }
            }
        }

        //key: a unique key as the cache entry
        //getData: a delegate to obtain the data in case of expiry
        //intervalSec: interval before next update in seconds
        //expirySec: sliding expiry time before the cache will be wiped out
        public static object readCache(string key, Func<object> getData, int intervalSec = 3600, int expirySec = 7200)
        {
            if (!locks.ContainsKey(key))
                lock (locks)
                    if (!locks.ContainsKey(key))
                        locks[key] = new object();

            //data within effective time
            if (memoryCache.Contains(key))
            {
                var dataInCache = memoryCache[key] as MemoryCacheTimedItem;
                if (dataInCache != null && dataInCache.validTime > DateTime.UtcNow)
                    return dataInCache.data;
            }

            lock (locks[key])
            {
                //else: expired or not existing, build cache for later use                
                if (keyTasks.ContainsKey(key) && !keyTasks[key].IsCompleted)
                {
                    keyCancelToken[key].Cancel();
                    errors.TryRemove(key, out var toDispose);
                }

                CancellationTokenSource ts = new CancellationTokenSource();
                CancellationToken ct = ts.Token;
                keyCancelToken[key] = ts;
                keyTasks[key] = Task.Run(() =>
                {
                    object data = null;
                    try
                    {
                        data = getData();
                        if (!ct.IsCancellationRequested)
                            buildCache(key, data, intervalSec, expirySec, false);
                        else return null;
                    }
                    catch (Exception e)
                    {
                        recordError(key, e);
                    }
                    return data;
                });
            }

            keyTasks[key].Wait();
            object res = null;

            lock (locks[key])
                //release memory
                if (keyTasks[key] != null && keyTasks[key].IsCompleted)
                {
                    keyTasks.TryRemove(key, out var tempTsk);
                    res = tempTsk.Result;
                    tempTsk.Dispose();
                    keyCancelToken.TryRemove(key, out var tempCt);
                    if (tempCt != null)
                        tempCt.Dispose();
                }

            if (errors.TryRemove(key, out var ex))
            {
                var aggEx = new AggregateException(ex);
                throw aggEx.Flatten();
            }
            if (res == null)
                res = (memoryCache[key] as MemoryCacheTimedItem).data;
            return res;
        }

        //key: a unique key as the cache entry
        //data: a delegate to obtain the data in case of expiry
        //intervalSec: interval before next update in seconds
        //expirySec: sliding expiry time before the cache will be wiped out
        public static object readCacheBackground(string key, Func<object> getData, int intervalSec = 3600, int expirySec = 7200)
        {
            if (!locks.ContainsKey(key))
                lock (locks)
                    if (!locks.ContainsKey(key))
                        locks[key] = new object();

            if (expirySec == 0)
                expirySec = 432000;     //a week
                                        //background update for next use if data expires
            if (
                ((!memoryCache.Contains(key) || memoryCache[key] == null)
                    || (memoryCache[key] as MemoryCacheTimedItem).validTime < DateTime.UtcNow))
            {
                lock (locks[key])
                    if (!keyTasks.ContainsKey(key) || keyTasks[key].IsCompleted)
                    {
                        CancellationTokenSource ts = new CancellationTokenSource();
                        CancellationToken ct = ts.Token;
                        keyCancelToken[key] = ts;
                        keyTasks[key] = Task.Run(() =>
                        {
                            object newData = null;
                            try
                            {
                                newData = getData();
                                if (!ct.IsCancellationRequested)
                                    buildCache(key, newData, intervalSec, expirySec, false);
                                else return null;
                            }
                            catch (Exception e)
                            {
                                recordError(key, e);
                            }
                            return newData;
                        });
                    }
            }

            //if data does not exist, building cache becomes the only choice
            if ((!memoryCache.Contains(key) || memoryCache[key] == null) && keyTasks.ContainsKey(key))
                keyTasks[key].Wait();
            object res = null;
            lock (locks[key])
                if (keyTasks.ContainsKey(key) && keyTasks[key] != null && keyTasks[key].IsCompleted)
                {
                    keyTasks.TryRemove(key, out var tempTsk);
                    res = tempTsk.Result;
                    tempTsk.Dispose();
                    keyCancelToken.TryRemove(key, out var tempCt);
                    if (tempCt != null)
                        tempCt.Dispose();
                }

            if (errors.TryRemove(key, out var ex))
            {
                var aggEx = new AggregateException(ex);
                throw aggEx.Flatten();
            }
            if (res == null)
                res = (memoryCache[key] as MemoryCacheTimedItem).data;
            return res;
        }
    }
}