class DummyNodePostprocessor(BaseNodePostprocessor):
    def _call_api(self, texts: List[str], query) -> List[List]:
        import httpx

        headers = {"Content-Type": "application/json"}
        json_data = {"texts": texts, "query": query, "truncate": True}

        print(json.dumps(json_data))

        with httpx.Client() as client:
            response = client.post(
                f"https://rerank.thevotum.com/rerank",
                headers=headers,
                json=json_data,
            )
        res = response.json()
        sorted_res = sorted(res, key=lambda x: x["index"])
        return sorted_res

    def _postprocess_nodes(
        self, nodes: List[NodeWithScore], query_bundle: QueryBundle
    ) -> List[NodeWithScore]:
        result = self._call_api(
            texts=[o.text for o in nodes], query=query_bundle.query_str
        )

        print(result)
        for r, n in zip(result, nodes):
            n.score = r["score"]

        new_nodes = sorted(nodes, key=lambda x: -x.score if x.score else 0)[:5]
        print([nn.score for nn in new_nodes])

        return new_nodes

