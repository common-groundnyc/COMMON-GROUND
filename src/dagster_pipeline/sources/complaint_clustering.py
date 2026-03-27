"""Auto-categorize complaints and violations using BERTopic semantic clustering.

Discovers themes beyond official taxonomy in 311 complaints, HPD violations,
and OATH hearings. Example: "illegal conversion disguised as noise complaint".

Runs as a Dagster asset — topic labels stored in DuckLake for MCP tool use.
"""


def cluster_complaints(texts: list[str], min_topic_size: int = 10) -> tuple[list[int], list[str]]:
    """Cluster complaint texts into semantic topics.

    Args:
        texts: List of complaint/violation descriptions
        min_topic_size: Minimum cluster size

    Returns:
        (topic_ids, topic_labels) — parallel lists, -1 = outlier
    """
    # NOTE: BERTopic runs as a batch Dagster asset, NOT in the MCP server.
    # Using MiniLM here is fine — its 384-dim embeddings stay in the clustering pipeline.
    # Only topic LABELS get stored in DuckLake, not raw vectors.
    from bertopic import BERTopic

    topic_model = BERTopic(
        embedding_model="all-MiniLM-L6-v2",
        min_topic_size=min_topic_size,
        verbose=True,
    )
    topics, _ = topic_model.fit_transform(texts)

    # Extract readable topic labels
    topic_info = topic_model.get_topic_info()
    label_map = dict(zip(topic_info["Topic"], topic_info["Name"]))
    labels = [label_map.get(t, "Unknown") for t in topics]

    return topics, labels
