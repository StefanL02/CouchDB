using CouchDB.Driver;
using CouchDB.Driver.Extensions;
using CouchDB.Driver.Types;
using Newtonsoft.Json;
using StackExchange.Redis;

public class Movie : CouchDocument
{
    [JsonProperty("movie_title")]
    public string? movieTitle { get; set; }

    [JsonProperty("genre")]
    public string? genre { get; set; }

    [JsonProperty("release_date")]
    public string? releaseDate { get; set; }

    [JsonProperty("total_gross")]
    public double? totalGross { get; set; }
}

class Program
{
    static async Task Main()
    {
        try
        {
            //Connect to CouchDB
            var client = new CouchClient(
                "http://127.0.0.1:5984",
                s => s.UseBasicAuthentication("admin", "admin1")
            );

            ICouchDatabase<Movie> db = client.GetDatabase<Movie>("movies");

            // --- CACHE KEY & TTL ---
            const string cacheKey = "movies:all:v1"; 
            TimeSpan ttl = TimeSpan.FromMinutes(3);

            var allMovies = await GetOrSetCachedAsync(cacheKey, ttl, async () =>
            {
                // Fetch from CouchDB
                var docs = await db.ToListAsync();

                // Optional: skip design docs if any slipped in 
                var filtered = docs
                    .Where(m => string.IsNullOrEmpty(m.Id) || !m.Id.StartsWith("_design/"))
                    .ToList();

                return filtered;
            });

            Console.WriteLine("First 25 movies retrieved:\n");
            foreach (var m in allMovies.Take(25))
            {
                Console.WriteLine($"{m.Id} | {m.movieTitle} | {m.releaseDate}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }
    }

   
    private static async Task<List<Movie>> GetOrSetCachedAsync(string key, TimeSpan ttl, Func<Task<List<Movie>>> valueFactory)
    {
        var muxer = ConnectionMultiplexer.Connect(
             new ConfigurationOptions
             {
                 EndPoints = { { "redis-12456.c266.us-east-1-3.ec2.redns.redis-cloud.com", 12456 } },
                 User = "default",
                 Password = "7gVrHDSALtuH4wtNsxnf3uscW3RHIIF5"
             }
         );
        var db = muxer.GetDatabase();

        // Try cache
        var cached = await db.StringGetAsync(key);
        if (cached.HasValue)
        {
            try
            {
                return JsonConvert.DeserializeObject<List<Movie>>(cached)!;
            }
            catch
            {
                // If deserialization fails, fall through to refresh cache
            }
        }

        // Cache miss = compute + set
        var fresh = await valueFactory();
        var json = JsonConvert.SerializeObject(fresh);
        await db.StringSetAsync(key, json, expiry: ttl);
        return fresh;
    }
}