import asyncio
from elasticsearch import AsyncElasticsearch

client = AsyncElasticsearch("http://localhost:9201")

async def main():
    resp = await client.search(
        index=".ds-traces*",
        body={
            "query": {
                "query_string": {
                    "query": 'trace.id: "74c3dfca1ea4aa65b0c73780313a33cc"',
                }
            },
            "size": 20
        },
    )

    print("Got {} hits:".format(resp["hits"]["total"]["value"]))
    for hit in resp["hits"]["hits"]:
        data = hit["_source"]
        print(f"span id: {data['span']['id']} span duration: {data['span']['duration']['us']} us")



if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
