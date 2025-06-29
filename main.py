import asyncio
from elasticsearch import AsyncElasticsearch

ES_ADDRESS = "http://localhost:9201"
client = AsyncElasticsearch(ES_ADDRESS)

async def main():
    try:
        await client.info(
            error_trace=True,
            human=True,
            pretty=True
        )
    except Exception as e:
        print(f"Call to Elastic Info API failed. Are you sure Elasticsearch is running on {ES_ADDRESS}?")
        print(e)
        exit(1)

    pit_keep_alive = "1m"
    point_in_time = (await client.open_point_in_time(
        keep_alive=pit_keep_alive,
        allow_partial_search_results=False,
        index=".ds-traces*"
    ))["id"]

    max_returned_hits = 10_000
    search_after = None
    hits = 1
    i = 0
    total_hits = 0
    
    while True:
        resp = await client.search(
            pit={"id": point_in_time, "keep_alive": pit_keep_alive},
            allow_partial_search_results=False,
            query={
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {
                                    "gt": "2025-04-30T16:12:00.000+02:00",
                                    "lt": "2025-04-30T16:30:00.000+02:00"
                                }
                            }
                        },
                        {
                            "query_string": {
                                "query": "transaction.name: \"POST /v1/rule/disable\""
                            }
                        }
                    ]
                }
            },
            timeout="60s",
            search_after=search_after,
            size=max_returned_hits,
            sort={
                "@timestamp": {
                    "order": "asc",
                    "format": "strict_date_optional_time_nanos",
                    "numeric_type": "date_nanos"
                }
            }
        )
        
        i += 1
        print(f"Search {i} done")
        hits = len(resp["hits"]["hits"])
        
        if hits == 0:
            break

        total_hits += hits
        point_in_time = resp["pit_id"]
        search_after = resp['hits']['hits'][-1]['sort']
    
        print(f"Got {min(hits, max_returned_hits)} (out of {hits}) hits:")
        
        if hits < max_returned_hits:
            break

    print(f"{total_hits=}")
    await client.close_point_in_time(id=point_in_time)



if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
