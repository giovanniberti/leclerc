import asyncio
from elasticsearch import AsyncElasticsearch

client = AsyncElasticsearch("http://localhost:9201")

async def main():
    max_returned_hits = 200
    resp = await client.search(
        index=".ds-traces*",
        body={
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {
                                    "gt": "2025-04-17T09:40:39.809Z"
                                }
                            }
                        },
                        {
                            "query_string": {
                                "query": "trace.id: \"74c3dfca1ea4aa65b0c73780313a33cc\""
                            }
                        }
                    ]
                }
            },
            "size": max_returned_hits
        },
    )

    hits = resp['hits']['total']['value']
    print(f"Got {min(hits, max_returned_hits)} (out of {hits}) hits:")
    for hit in resp["hits"]["hits"]:
        data = hit["_source"]
        print(f"span id: {data['span']['id']} span duration: {data['span']['duration']['us']} us")



if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
