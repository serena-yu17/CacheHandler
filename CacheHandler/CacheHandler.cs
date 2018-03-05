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
        public DateTime ts { get; set; }
        public object data { get; set; }

        //default: ts = now
        public MemoryCacheTimedItem(object data)
        {
            ts = DateTime.Now;
            this.data = data;
        }
    }


    public static class CacheHandler
    {
        static ConcurrentDictionary<string, Task> keyTasks = new ConcurrentDictionary<string, Task>();
        static ConcurrentDictionary<string, CancellationTokenSource> keyTs = new ConcurrentDictionary<string, CancellationTokenSource>();

        public static void resetMemCache(ConcurrentDictionary<string, Func<object>> memKeys)
        {
            if (MemoryCache.Default != null)
                foreach (var memKey in memKeys)
                    if (MemoryCache.Default.Contains(memKey.Key))
                    {
                        if (keyTasks.ContainsKey(memKey.Key) && !keyTasks[memKey.Key].IsCompleted)
                            keyTs[memKey.Key].Cancel();
                        MemoryCache.Default.Remove(memKey.Key);

                        CancellationTokenSource ts = new CancellationTokenSource();
                        CancellationToken ct = ts.Token;
                        keyTs[memKey.Key] = ts;
                        keyTasks[memKey.Key] = Task.Factory.StartNew(() =>
                        {
                            object data = memKey.Value();
                            if (!ct.IsCancellationRequested)
                                buildCache(memKey.Key, data, 240);
                        }, TaskCreationOptions.LongRunning);
                    }
            memKeys.Clear();
        }

        public static void buildCache(string key, object data, int expiry = 3600, bool noForce = true)
        {
            if (MemoryCache.Default != null)
            {
                //noForce: do not force updates before cache expires
                if (!noForce || !MemoryCache.Default.Contains(key) || MemoryCache.Default[key] == null ||
                    !(expiry == 0 || (DateTime.Now - (MemoryCache.Default[key] as MemoryCacheTimedItem).ts).TotalSeconds <= expiry))
                {
                    MemoryCacheTimedItem newEntry = new MemoryCacheTimedItem(data);
                    DateTimeOffset offset = ObjectCache.InfiniteAbsoluteExpiration;
                    if (expiry > 0)
                        offset = DateTimeOffset.UtcNow.AddSeconds(expiry);
                    MemoryCache.Default.Set(key, newEntry,
                        new CacheItemPolicy() { AbsoluteExpiration = offset }
                        );
                }
            }
        }

        public static object readCache(string key, Func<object> getData, int intervalSec = 3600, int expiry = 7200)
        {
            //data within effective time
            if (MemoryCache.Default != null && MemoryCache.Default.Contains(key))
            {
                var dataInCache = MemoryCache.Default[key] as MemoryCacheTimedItem;
                if (dataInCache != null && (intervalSec == 0 || (DateTime.Now - dataInCache.ts).TotalSeconds <= intervalSec))
                    return dataInCache.data;
            }
            //if expired or not existing, build cache for later use
            object data = null;
            if (keyTasks.ContainsKey(key) && !keyTasks[key].IsCompleted)
                keyTs[key].Cancel();

            CancellationTokenSource ts = new CancellationTokenSource();
            CancellationToken ct = ts.Token;
            keyTs[key] = ts;
            keyTasks[key] = Task.Run(() =>
            {
                data = getData();
                if (!ct.IsCancellationRequested)
                    buildCache(key, data, expiry, false);
            });
            keyTasks[key].Wait();
            return data;
        }

        public static object readCacheBackground(string key, Func<object> getData, int intervalSec = 3600, int expiry = 7200)
        {
            //background update for next use if data expires
            if (((!MemoryCache.Default.Contains(key) || MemoryCache.Default[key] == null)
                || (intervalSec != 0 && (DateTime.Now - (MemoryCache.Default[key] as MemoryCacheTimedItem).ts).TotalSeconds > intervalSec))
                && (!keyTasks.ContainsKey(key) || keyTasks[key].IsCompleted))
            {
                CancellationTokenSource ts = new CancellationTokenSource();
                CancellationToken ct = ts.Token;
                keyTs[key] = ts;
                keyTasks[key] = Task.Factory.StartNew(() =>
                {
                    object newData = getData();
                    if (!ct.IsCancellationRequested)
                        buildCache(key, newData, intervalSec, false);
                }, TaskCreationOptions.LongRunning);
            }
            //if data not exists, building cache becomes the only option
            if (!MemoryCache.Default.Contains(key) || MemoryCache.Default[key] == null)
                keyTasks[key].Wait();
            return (MemoryCache.Default[key] as MemoryCacheTimedItem).data;
        }
    }
}